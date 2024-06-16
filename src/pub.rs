use crate::backend::DisconnectNotifier;
use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::message::*;
use crate::reconnect::{ReconnectConfig, ReconnectHandle};
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{async_rt, CaptureSocket, SocketOptions};
use crate::{codec::*, TryIntoEndpoint};
use crate::{
    MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketSend, SocketType, ZmqError,
};

use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::{select, FutureExt, StreamExt};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) struct Subscriber {
    pub(crate) subscriptions: Vec<Vec<u8>>,
    pub(crate) send_queue: Pin<Box<ZmqFramedWrite>>,
    _subscription_coro_stop: oneshot::Sender<()>,
}

pub(crate) struct PubSocketBackend {
    subscribers: scc::HashMap<PeerIdentity, Subscriber>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: SocketOptions,
    /// Notifiers for reconnection tasks - keyed by `peer_id`
    disconnect_notifiers: Mutex<HashMap<PeerIdentity, DisconnectNotifier>>,
}

impl PubSocketBackend {
    /// Register a notifier to be called when a peer disconnects.
    ///
    /// Used by reconnection tasks to be notified when they should attempt reconnection.
    pub(crate) fn register_disconnect_notifier(
        &self,
        peer_id: PeerIdentity,
        notifier: DisconnectNotifier,
    ) {
        self.disconnect_notifiers.lock().insert(peer_id, notifier);
    }

    /// Unregister a disconnect notifier for a peer.
    #[allow(dead_code)]
    pub(crate) fn unregister_disconnect_notifier(&self, peer_id: &PeerIdentity) {
        self.disconnect_notifiers.lock().remove(peer_id);
    }

    fn message_received(&self, peer_id: &PeerIdentity, message: Message) {
        let data = match message {
            Message::Message(m) => {
                if m.len() != 1 {
                    log::warn!("Received message with unexpected length: {}", m.len());
                    return;
                }
                m.into_vec().pop().unwrap_or_default()
            }
            _ => return,
        };

        if data.is_empty() {
            return;
        }

        match data.first() {
            Some(1) => {
                // Subscribe
                if let Some(mut entry) = self.subscribers.get_sync(peer_id) {
                    entry.subscriptions.push(Vec::from(&data[1..]));
                }
            }
            Some(0) => {
                // Unsubscribe
                let sub = Vec::from(&data[1..]);
                if let Some(mut entry) = self.subscribers.get_sync(peer_id) {
                    if let Some(index) = entry.subscriptions.iter().position(|s| s == &sub) {
                        entry.subscriptions.remove(index);
                    }
                }
            }
            _ => log::warn!(
                "Received message with unexpected first byte: {:?}",
                data.first()
            ),
        }
    }
}

impl SocketBackend for PubSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::PUB
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.subscribers.clear_sync();
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl MultiPeerBackend for PubSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (mut recv_queue, send_queue) = io.into_parts();
        // TODO provide handling for recv_queue
        let (sender, stop_receiver) = oneshot::channel();
        self.subscribers
            .upsert_async(
                peer_id.clone(),
                Subscriber {
                    subscriptions: vec![],
                    send_queue: Box::pin(send_queue),
                    _subscription_coro_stop: sender,
                },
            )
            .await;
        let backend = self;
        let peer_id = peer_id.clone();
        async_rt::task::spawn(async move {
            let mut stop_receiver = stop_receiver.fuse();
            loop {
                select! {
                     _ = stop_receiver => {
                         break;
                     },
                     message = recv_queue.next().fuse() => {
                        match message {
                            Some(Ok(m)) => backend.message_received(&peer_id, m),
                            Some(Err(e)) => {
                                log::debug!("Error receiving message: {:?}", e);
                                backend.peer_disconnected(&peer_id);
                                break;
                            }
                            None => {
                                backend.peer_disconnected(&peer_id);
                                break
                            }
                        }

                     }
                }
            }
        });
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        log::info!("Client disconnected {:?}", peer_id);
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }
        self.subscribers.remove_sync(peer_id);

        // Notify reconnection task if registered
        if let Some(mut notifier) = self.disconnect_notifiers.lock().remove(peer_id) {
            // Use try_send to avoid blocking - if channel is full, the reconnect task
            // will eventually notice the peer is gone
            let _ = notifier.try_send(peer_id.clone());
        }
    }
}

pub struct PubSocket {
    pub(crate) backend: Arc<PubSocketBackend>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
    /// Handles to background reconnection tasks
    reconnect_handles: Vec<ReconnectHandle>,
}

impl Drop for PubSocket {
    fn drop(&mut self) {
        // Shutdown all reconnection tasks
        for handle in self.reconnect_handles.drain(..) {
            handle.shutdown();
        }
        self.backend.shutdown();
    }
}

#[async_trait]
impl SocketSend for PubSocket {
    async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
        let first_frame = match message.get(0) {
            Some(frame) => frame,
            None => return Ok(()), // Empty message, nothing to publish
        };
        let mut dead_peers = Vec::new();
        let mut iter = self.backend.subscribers.begin_async().await;
        while let Some(mut subscriber) = iter {
            for sub_filter in &subscriber.subscriptions {
                if sub_filter.len() <= first_frame.len()
                    && sub_filter.as_slice() == &first_frame[0..sub_filter.len()]
                {
                    let res = subscriber
                        .send_queue
                        .as_mut()
                        .try_send(Message::Message(message.clone()));
                    match res {
                        Ok(()) => {}
                        Err(ZmqError::Codec(CodecError::Io(e))) => {
                            if e.kind() == ErrorKind::BrokenPipe {
                                dead_peers.push(subscriber.key().clone());
                            } else {
                                log::error!("Error receiving message: {:?}", e);
                            }
                        }
                        Err(ZmqError::BufferFull(_)) => {
                            // ignore silently. https://rfc.zeromq.org/spec/29/ says:
                            // For processing outgoing messages:
                            //   SHALL silently drop the message if the queue for a subscriber is full.
                            log::debug!("Queue for subscriber is full",);
                        }
                        Err(e) => {
                            log::error!("Error receiving message: {:?}", e);
                            return Err(e);
                        }
                    }
                    break;
                }
            }
            iter = subscriber.next_async().await;
        }
        for peer in dead_peers {
            self.backend.peer_disconnected(&peer);
        }
        Ok(())
    }
}

impl CaptureSocket for PubSocket {}

#[async_trait]
impl Socket for PubSocket {
    fn with_options(options: SocketOptions) -> Self {
        Self {
            backend: Arc::new(PubSocketBackend {
                subscribers: scc::HashMap::new(),
                socket_monitor: Mutex::new(None),
                socket_options: options,
                disconnect_notifiers: Mutex::new(HashMap::new()),
            }),
            binds: HashMap::new(),
            reconnect_handles: Vec::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle> {
        &mut self.binds
    }

    /// Connects to the given endpoint with automatic reconnection support.
    ///
    /// Unlike the default `Socket::connect`, this implementation spawns a
    /// background task that will automatically reconnect if the connection
    /// is lost.
    async fn connect(&mut self, endpoint: &str) -> ZmqResult<()> {
        let endpoint = TryIntoEndpoint::try_into(endpoint)?;

        // Initial connection
        let (socket, resolved_endpoint) = crate::util::connect_forever(endpoint.clone()).await?;
        let peer_id =
            crate::util::peer_connected(socket, self.backend.clone() as Arc<dyn MultiPeerBackend>)
                .await?;

        // Emit Connected event
        if let Some(monitor) = self.backend.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Connected(resolved_endpoint, peer_id.clone()));
        }

        // Create a closure that registers disconnect notifiers with the backend
        let backend_for_closure = self.backend.clone();
        let register_fn: crate::reconnect::RegisterDisconnectFn =
            Box::new(move |peer_id, notifier| {
                backend_for_closure.register_disconnect_notifier(peer_id, notifier);
            });

        // Spawn reconnection task
        let reconnect_handle = crate::reconnect::spawn_reconnect_task(
            endpoint,
            self.backend.clone() as Arc<dyn MultiPeerBackend>,
            peer_id,
            register_fn,
            ReconnectConfig::default(),
        );
        self.reconnect_handles.push(reconnect_handle);

        Ok(())
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::tests::{
        test_bind_to_any_port_helper, test_bind_to_unspecified_interface_helper,
    };
    use crate::ZmqResult;
    use std::net::IpAddr;

    #[async_rt::test]
    async fn test_bind_to_any_port() -> ZmqResult<()> {
        let s = PubSocket::new();
        test_bind_to_any_port_helper(s).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv4_interface() -> ZmqResult<()> {
        let any_ipv4: IpAddr = "0.0.0.0".parse().unwrap();
        let s = PubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv4, s, 4000).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv6_interface() -> ZmqResult<()> {
        let any_ipv6: IpAddr = "::".parse().unwrap();
        let s = PubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv6, s, 4010).await
    }
}
