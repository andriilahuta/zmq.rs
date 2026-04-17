use crate::backend::DisconnectNotifier;
use crate::{TryIntoEndpoint, codec::*};
use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::message::*;
use crate::reconnect::{ReconnectConfig, ReconnectHandle};
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{async_rt, CaptureSocket, SocketOptions};
use crate::{
    MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketSend, SocketType, ZmqError,
};
use crate::heartbeat::{ActivityTracker, HeartbeatConfig, spawn_heartbeat_task, HeartbeatHandle};

use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use futures::{select, FutureExt, SinkExt, StreamExt};
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
    /// Heartbeat activity trackers and tasks per peer
    heartbeats: Mutex<HashMap<PeerIdentity, (ActivityTracker, Option<HeartbeatHandle>)>>,
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

        // Initialize heartbeat tracking (conditionally spawned based on socket options)
        let (activity_tracker, hb_handle) = if self.socket_options.heartbeat_enabled {
            let config = self.socket_options.heartbeat_config.clone()
                .unwrap_or_default();
            let activity_tracker = ActivityTracker::new(config.clone());

            // Spawn heartbeat task for this peer to send PINGs independently
            let backend_weak = Arc::downgrade(&self);
            let peer_id_for_task = peer_id.clone();
            let heartbeat_clone = activity_tracker.clone();

            let send_ping_callback = {
                let peer_id = peer_id.clone();
                let backend_weak = backend_weak.clone();
                let config = config.clone();

                move || {
                    let peer_id = peer_id.clone();
                    let backend_weak = backend_weak.clone();

                    Box::pin(async move {
                        if let Some(backend) = backend_weak.upgrade() {
                            if let Some(mut sub) = backend.subscribers.get_async(&peer_id).await {
                                let ping = ZmqCommand::ping(config.ttl, None);
                                sub.send_queue.send(Message::Command(ping)).await
                                    .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))
                            } else {
                                Err(std::io::Error::new(
                                    ErrorKind::ConnectionAborted,
                                    "Subscriber not found",
                                ))
                            }
                        } else {
                            Err(std::io::Error::new(
                                ErrorKind::Other,
                                "Backend dropped",
                            ))
                        }
                    }) as BoxFuture<'static, Result<(), std::io::Error>>
                }
            };

            let on_timeout_callback = {
                let peer_id = peer_id.clone();
                let backend_weak = backend_weak.clone();

                move || {
                    if let Some(backend) = backend_weak.upgrade() {
                        backend.peer_disconnected(&peer_id);
                        true // Signal to break the heartbeat loop
                    } else {
                        true // Can't reach backend, exit
                    }
                }
            };

            let hb_handle = spawn_heartbeat_task(heartbeat_clone, peer_id_for_task.clone(), send_ping_callback, on_timeout_callback);
            (activity_tracker, Some(hb_handle))
        } else {
            // Heartbeat disabled - Create tracker for PONG responses only
            let activity_tracker = ActivityTracker::new(HeartbeatConfig::default());
            (activity_tracker, None)
        };

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

        // Store heartbeat handle if spawned
        self.heartbeats.lock().insert(peer_id.clone(), (activity_tracker.clone(), hb_handle));

        let backend = self.clone();
        let peer_id_clone = peer_id.clone();
        async_rt::task::spawn(async move {
            let mut stop_receiver = stop_receiver.fuse();
            loop {
                select! {
                     _ = stop_receiver => {
                         log::debug!("Pub peer stopped: {:?}", peer_id_clone);
                         break;
                     },
                     message = recv_queue.next().fuse() => {
                        match message {
                            Some(Ok(msg)) => {
                                // Record activity and handle heartbeat commands
                                activity_tracker.record_activity();

                                if let Message::Command(cmd) = &msg {
                                    match cmd.name {
                                        ZmqCommandName::PING => {
                                            log::debug!("Received PING from peer {:?}", peer_id_clone);
                                            // Record activity and TTL if provided
                                            activity_tracker.received_ping(cmd.ttl);
                                            // Respond to PING with PONG
                                            if let Some(mut sub) = backend.subscribers.get_async(&peer_id_clone).await {
                                                let pong = ZmqCommand::pong(cmd.context.clone());
                                                if let Err(e) = sub.send_queue.send(Message::Command(pong)).await {
                                                    log::warn!("Failed to send PONG to peer {:?}: {:?}", peer_id_clone, e);
                                                    backend.peer_disconnected(&peer_id_clone);
                                                    break;
                                                }
                                            }
                                            continue;
                                        }
                                        ZmqCommandName::PONG => {
                                            log::debug!("Received PONG from peer {:?}", peer_id_clone);
                                            // Record activity on PONG
                                            activity_tracker.received_pong();
                                            continue;
                                        }
                                        _ => {}
                                    }
                                }

                                backend.message_received(&peer_id_clone, msg);
                            }
                            Some(Err(e)) => {
                                log::debug!("Error receiving message from peer {:?}: {:?}", peer_id_clone, e);
                                backend.peer_disconnected(&peer_id_clone);
                                break;
                            }
                            None => {
                                log::debug!("Peer {:?} closed connection", peer_id_clone);
                                backend.peer_disconnected(&peer_id_clone);
                                break
                            }
                        }

                        // Check if connection is dead due to heartbeat timeout
                        if activity_tracker.is_dead() {
                            log::warn!("Heartbeat timeout for peer {:?}", peer_id_clone);
                            backend.peer_disconnected(&peer_id_clone);
                            break;
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

        // Clean up heartbeat (drops the HeartbeatHandle which signals shutdown)
        self.heartbeats.lock().remove(peer_id);

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
                heartbeats: Mutex::new(HashMap::new()),
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
