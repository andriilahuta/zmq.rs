use crate::backend::{DisconnectNotifier, Peer};
use crate::codec::{FramedIo, Message, ZmqCommand, ZmqFramedRead};
use crate::endpoint::Endpoint;
use crate::error::ZmqResult;
use crate::fair_queue::FairQueue;
use crate::fair_queue::QueueInner;
use crate::heartbeat::{ActivityTracker, HeartbeatConfig, spawn_heartbeat_task, HeartbeatHandle};
use crate::message::ZmqMessage;
use crate::reconnect::{ReconnectConfig, ReconnectHandle};
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{
    MultiPeerBackend, Socket, SocketBackend, SocketEvent, SocketOptions, SocketRecv, SocketType,
    TryIntoEndpoint,
};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use crossbeam_queue::SegQueue;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::Arc;

pub enum SubBackendMsgType {
    UNSUBSCRIBE = 0,
    SUBSCRIBE = 1,
}

pub(crate) struct SubSocketBackend {
    pub(crate) peers: scc::HashMap<PeerIdentity, Peer>,
    fair_queue_inner: Option<Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>>,
    pub(crate) round_robin: SegQueue<PeerIdentity>,
    socket_type: SocketType,
    socket_options: SocketOptions,
    pub(crate) socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    subs: Mutex<HashSet<String>>,
    /// Notifiers for reconnection tasks - keyed by `peer_id`
    disconnect_notifiers: Mutex<HashMap<PeerIdentity, DisconnectNotifier>>,
    /// Heartbeat activity trackers and tasks per peer
    heartbeats: Mutex<HashMap<PeerIdentity, (ActivityTracker, Option<HeartbeatHandle>)>>,
}

impl SubSocketBackend {
    pub(crate) fn with_options(
        fair_queue_inner: Option<Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>>,
        socket_type: SocketType,
        options: SocketOptions,
    ) -> Self {
        Self {
            peers: scc::HashMap::new(),
            fair_queue_inner,
            round_robin: SegQueue::new(),
            socket_type,
            socket_options: options,
            socket_monitor: Mutex::new(None),
            subs: Mutex::new(HashSet::new()),
            disconnect_notifiers: Mutex::new(HashMap::new()),
            heartbeats: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_subs_message(subscription: &str, msg_type: SubBackendMsgType) -> ZmqMessage {
        let mut buf = BytesMut::with_capacity(subscription.len() + 1);
        buf.put_u8(msg_type as u8);
        buf.extend_from_slice(subscription.as_bytes());

        buf.freeze().into()
    }

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
}

impl SocketBackend for SubSocketBackend {
    fn socket_type(&self) -> SocketType {
        self.socket_type
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.peers.clear_sync();
        // Clear fair_queue streams to ensure TCP connections are closed
        // even when reconnect tasks still hold Arc references to the backend
        if let Some(inner) = &self.fair_queue_inner {
            inner.lock().clear();
        }
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl MultiPeerBackend for SubSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, mut send_queue) = io.into_parts();

        let subs_msgs: Vec<ZmqMessage> = self
            .subs
            .lock()
            .iter()
            .map(|x| SubSocketBackend::create_subs_message(x, SubBackendMsgType::SUBSCRIBE))
            .collect();

        for message in subs_msgs {
            if let Err(e) = send_queue.send(Message::Message(message)).await {
                log::error!("Failed to send subscription to peer {:?}: {:?}", peer_id, e);
                return;
            }
        }

        self.peers
            .upsert_async(peer_id.clone(), Peer { send_queue })
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
                        use crate::codec::ZmqCommand;
                        if let Some(backend) = backend_weak.upgrade() {
                            if let Some(mut peer) = backend.peers.get_async(&peer_id).await {
                                let ping = ZmqCommand::ping(config.ttl, None);
                                peer.send_queue.send(Message::Command(ping)).await
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

        self.round_robin.push(peer_id.clone());
        match &self.fair_queue_inner {
            None => {}
            Some(inner) => {
                inner.lock().insert(peer_id.clone(), recv_queue);
            }
        };
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }
        self.peers.remove_sync(peer_id);
        self.heartbeats.lock().remove(peer_id);
        match &self.fair_queue_inner {
            None => {}
            Some(inner) => {
                inner.lock().remove(peer_id);
            }
        };

        // Notify reconnection task if registered
        if let Some(mut notifier) = self.disconnect_notifiers.lock().remove(peer_id) {
            // Use try_send to avoid blocking - if channel is full, the reconnect task
            // will eventually notice the peer is gone
            let _ = notifier.try_send(peer_id.clone());
        }
    }
}

pub struct SubSocket {
    backend: Arc<SubSocketBackend>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
    /// Handles to background reconnection tasks
    reconnect_handles: Vec<ReconnectHandle>,
}

impl Drop for SubSocket {
    fn drop(&mut self) {
        // Shutdown all reconnection tasks
        for handle in self.reconnect_handles.drain(..) {
            handle.shutdown();
        }
        self.backend.shutdown();
    }
}

impl SubSocket {
    pub async fn subscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        self.backend.subs.lock().insert(subscription.to_string());
        self.process_subs(subscription, SubBackendMsgType::SUBSCRIBE)
            .await
    }

    pub async fn unsubscribe(&mut self, subscription: &str) -> ZmqResult<()> {
        self.backend.subs.lock().remove(subscription);
        self.process_subs(subscription, SubBackendMsgType::UNSUBSCRIBE)
            .await
    }

    async fn process_subs(
        &mut self,
        subscription: &str,
        msg_type: SubBackendMsgType,
    ) -> ZmqResult<()> {
        let message: ZmqMessage = SubSocketBackend::create_subs_message(subscription, msg_type);
        let mut iter = self.backend.peers.begin_async().await;

        while let Some(mut peer) = iter {
            peer.send_queue
                .send(Message::Message(message.clone()))
                .await?;
            iter = peer.next_async().await;
        }
        Ok(())
    }
}

#[async_trait]
impl Socket for SubSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(SubSocketBackend::with_options(
            Some(fair_queue.inner()),
            SocketType::SUB,
            options,
        ));

        // Set callback to notify backend when a stream ends (peer disconnected)
        // Use Weak to avoid Arc cycle: backend -> fair_queue_inner -> callback -> backend
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
    /// is lost. On reconnection, subscriptions are automatically re-sent
    /// to the peer.
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

#[async_trait]
impl SocketRecv for SubSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        use crate::codec::ZmqCommandName;

        loop {
            match self.fair_queue.next().await {
                Some((peer_id, Ok(Message::Message(message)))) => {
                    // Record heartbeat activity on message reception
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                        heartbeat_tuple.0.record_activity();
                    }
                    return Ok(message);
                }
                Some((peer_id, Ok(Message::Command(cmd)))) => {
                    // Handle heartbeat commands
                    match cmd.name {
                        ZmqCommandName::PING => {
                            // Handle PING with TTL tracking
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.received_ping(cmd.ttl);
                            }
                            // Respond with PONG
                            if let Some(mut peer) = self.backend.peers.get_async(&peer_id).await {
                                let pong = ZmqCommand::pong(cmd.context.clone());
                                let _ = peer.send_queue.send(Message::Command(pong)).await;
                            }
                        }
                        ZmqCommandName::PONG => {
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.received_pong();
                            }
                        }
                        _ => {
                            // Other commands are unexpected
                            if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                                heartbeat_tuple.0.record_activity();
                            }
                        }
                    }

                }
                Some((peer_id, Ok(Message::Greeting(_)))) => {
                    // Record activity but skip greeting
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id) {
                        heartbeat_tuple.0.record_activity();
                    }
                }
                Some((peer_id, Err(e))) => {
                    self.backend.peer_disconnected(&peer_id);
                    // Handle potential errors from the fair queue
                    return Err(e.into());
                }
                None => {
                    // All clients disconnected
                    let mut peer_ids = Vec::with_capacity(self.backend.peers.len());
                    self.backend.peers.iter_sync(|peer_id, _peer| {
                        peer_ids.push(peer_id.clone());
                        true
                    });
                    for peer_id in peer_ids {
                        self.backend.peer_disconnected(&peer_id);
                    }
                }
            }

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
