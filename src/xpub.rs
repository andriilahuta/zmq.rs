use crate::codec::*;
use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::fair_queue::{FairQueue, QueueInner};
use crate::heartbeat::{ActivityTracker, HeartbeatConfig, spawn_heartbeat_task, HeartbeatHandle};
use crate::message::*;
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{CaptureSocket, SocketOptions};
use crate::{
    MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketRecv, SocketSend, SocketType,
    ZmqError,
};

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) struct XPubSubscriber {
    pub(crate) subscriptions: Vec<Vec<u8>>,
    pub(crate) send_queue: Pin<Box<ZmqFramedWrite>>,
}

pub(crate) struct XPubSocketBackend {
    subscribers: scc::HashMap<PeerIdentity, XPubSubscriber>,
    fair_queue_inner: Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: SocketOptions,
    /// Heartbeat activity trackers and tasks per peer
    pub(crate) heartbeats: Mutex<HashMap<PeerIdentity, (ActivityTracker, Option<HeartbeatHandle>)>>,
}

impl XPubSocketBackend {
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

impl SocketBackend for XPubSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::XPUB
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
impl MultiPeerBackend for XPubSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, send_queue) = io.into_parts();

        self.subscribers
            .upsert_async(
                peer_id.clone(),
                XPubSubscriber {
                    subscriptions: vec![],
                    send_queue: Box::pin(send_queue),
                },
            )
            .await;

        // Initialize heartbeat tracker for this peer
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
                    let config = config.clone();

                    Box::pin(async move {
                        if let Some(backend) = backend_weak.upgrade() {
                            if let Some(mut sub) = backend.subscribers.get_async(&peer_id).await {
                                let ping = ZmqCommand::ping(config.ttl, None);
                                sub.send_queue.send(Message::Command(ping)).await
                                    .map_err(|e| std::io::Error::new(ErrorKind::Other, e.to_string()))
                            } else {
                                Err(std::io::Error::new(ErrorKind::ConnectionAborted, "Peer not found"))
                            }
                        } else {
                            Err(std::io::Error::new(ErrorKind::Other, "Backend dropped"))
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
                    }
                    true // Signal to break the heartbeat loop
                }
            };

            let hb_handle = spawn_heartbeat_task(heartbeat_clone, peer_id_for_task.clone(), send_ping_callback, on_timeout_callback);
            (activity_tracker, Some(hb_handle))
        } else {
            // Heartbeat disabled - Create tracker for PONG responses only
            let activity_tracker = ActivityTracker::new(HeartbeatConfig::default());
            (activity_tracker, None)
        };

        self.heartbeats.lock().insert(peer_id.clone(), (activity_tracker.clone(), hb_handle));

        self.fair_queue_inner
            .lock()
            .insert(peer_id.clone(), recv_queue);
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        log::info!("Client disconnected {:?}", peer_id);
        self.subscribers.remove_sync(peer_id);
        self.heartbeats.lock().remove(peer_id);
        self.fair_queue_inner.lock().remove(peer_id);
    }
}

pub struct XPubSocket {
    pub(crate) backend: Arc<XPubSocketBackend>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for XPubSocket {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

#[async_trait]
impl SocketSend for XPubSocket {
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
                                log::error!("Error sending message: {:?}", e);
                            }
                        }
                        Err(ZmqError::BufferFull(_)) => {
                            // Silently drop the message if the queue for a subscriber is full.
                            // https://rfc.zeromq.org/spec/29/
                            log::debug!("Queue for subscriber is full");
                        }
                        Err(e) => {
                            log::error!("Error sending message: {:?}", e);
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

#[async_trait]
impl SocketRecv for XPubSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((peer_id, Ok(Message::Message(message)))) => {
                    // Record heartbeat activity on message reception
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                        heartbeat_tuple.0.record_activity();
                    }
                    // Process the subscription message internally to update tracking
                    self.backend
                        .message_received(&peer_id, Message::Message(message.clone()));
                    // Also expose it to the application
                    return Ok(message);
                }
                Some((peer_id, Ok(Message::Command(cmd)))) => {
                    // Handle heartbeat commands
                    match cmd.name {
                        ZmqCommandName::PING => {
                            if let Some(mut subscriber) = self.backend.subscribers.get_async(&peer_id).await {
                                let pong = ZmqCommand::pong(cmd.context.clone());
                                let _ = subscriber.send_queue.send(Message::Command(pong)).await;
                            }
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.received_ping(cmd.ttl);
                            }
                        }
                        ZmqCommandName::PONG => {
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.received_pong();
                            }
                        }
                        _ => {
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.record_activity();
                            }
                        }
                    }
                }
                Some((peer_id, Ok(Message::Greeting(_)))) => {
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                        heartbeat_tuple.0.record_activity();
                    }
                }
                Some((peer_id, Err(e))) => {
                    self.backend.peer_disconnected(&peer_id);
                    return Err(e.into());
                }
                None => {
                    return Err(ZmqError::NoMessage);
                }
            };

            // Check if any peers have timed out
            let mut timed_out_peers = Vec::new();
            self.backend.heartbeats.lock().iter().for_each(|(peer_id, heartbeat_tuple)| {
                if heartbeat_tuple.0.is_dead() {
                    timed_out_peers.push(peer_id.clone());
                }
            });
            for peer_id in timed_out_peers {
                log::warn!("Heartbeat timeout for peer {:?}", peer_id);
                self.backend.peer_disconnected(&peer_id);
            }
        }
    }
}

impl CaptureSocket for XPubSocket {}

#[async_trait]
impl Socket for XPubSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(XPubSocketBackend {
            subscribers: scc::HashMap::new(),
            fair_queue_inner: fair_queue.inner(),
            socket_monitor: Mutex::new(None),
            socket_options: options,
            heartbeats: Mutex::new(HashMap::new()),
        });

        let backend_weak = Arc::downgrade(&backend);
        fair_queue.set_on_disconnect(move |peer_id: PeerIdentity| {
            if let Some(backend) = backend_weak.upgrade() {
                backend.peer_disconnected(&peer_id);
            }
        });

        Self {
            backend,
            fair_queue,
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle> {
        &mut self.binds
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
    use crate::async_rt;
    use crate::util::tests::{
        test_bind_to_any_port_helper, test_bind_to_unspecified_interface_helper,
    };
    use crate::ZmqResult;
    use std::net::IpAddr;

    #[async_rt::test]
    async fn test_bind_to_any_port() -> ZmqResult<()> {
        let s = XPubSocket::new();
        test_bind_to_any_port_helper(s).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv4_interface() -> ZmqResult<()> {
        let any_ipv4: IpAddr = "0.0.0.0".parse().unwrap();
        let s = XPubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv4, s, 4020).await
    }

    #[async_rt::test]
    async fn test_bind_to_any_ipv6_interface() -> ZmqResult<()> {
        let any_ipv6: IpAddr = "::".parse().unwrap();
        let s = XPubSocket::new();
        test_bind_to_unspecified_interface_helper(any_ipv6, s, 4030).await
    }
}
