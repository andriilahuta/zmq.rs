use crate::codec::{FramedIo, Message, ZmqCommand, ZmqFramedRead, ZmqFramedWrite};
use crate::fair_queue::QueueInner;
use crate::heartbeat::{ActivityTracker, HeartbeatConfig, spawn_heartbeat_task, HeartbeatHandle};
use crate::util::PeerIdentity;
use crate::{
    MultiPeerBackend, SocketBackend, SocketEvent, SocketOptions, SocketType, ZmqError, ZmqResult,
};

use async_trait::async_trait;
use crossbeam_queue::SegQueue;
use futures::channel::mpsc;
use futures::SinkExt;
use futures::future::BoxFuture;
use parking_lot::Mutex;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

/// Sender for notifying reconnection tasks when a peer disconnects.
pub(crate) type DisconnectNotifier = mpsc::Sender<PeerIdentity>;

pub(crate) struct Peer {
    pub(crate) send_queue: ZmqFramedWrite,
}

pub(crate) struct GenericSocketBackend {
    pub(crate) peers: scc::HashMap<PeerIdentity, Peer>,
    fair_queue_inner: Option<Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>>,
    pub(crate) round_robin: SegQueue<PeerIdentity>,
    socket_type: SocketType,
    socket_options: SocketOptions,
    pub(crate) socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    /// Notifiers for reconnection tasks - keyed by `peer_id`
    disconnect_notifiers: Mutex<HashMap<PeerIdentity, DisconnectNotifier>>,
    /// Heartbeat activity trackers and tasks per peer
    pub(crate) heartbeats: Mutex<HashMap<PeerIdentity, (ActivityTracker, Option<HeartbeatHandle>)>>,
}

impl GenericSocketBackend {
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
            disconnect_notifiers: Mutex::new(HashMap::new()),
            heartbeats: Mutex::new(HashMap::new()),
        }
    }

    /// Register a notifier to be called when a peer disconnects.
    ///
    /// Used by reconnection tasks to be notified when they should attempt reconnection.
    #[allow(dead_code)] // Will be used when reconnection is added to more socket types
    pub(crate) fn register_disconnect_notifier(
        &self,
        peer_id: PeerIdentity,
        notifier: DisconnectNotifier,
    ) {
        self.disconnect_notifiers.lock().insert(peer_id, notifier);
    }

    /// Unregister a disconnect notifier for a peer.
    #[allow(dead_code)] // Will be used when reconnection is added to more socket types
    pub(crate) fn unregister_disconnect_notifier(&self, peer_id: &PeerIdentity) {
        self.disconnect_notifiers.lock().remove(peer_id);
    }

    pub(crate) async fn send_round_robin(&self, message: Message) -> ZmqResult<PeerIdentity> {
        // In normal scenario this will always be only 1 iteration
        // There can be special case when peer has disconnected and his id is still in
        // RR queue This happens because SegQueue don't have an api to delete
        // items from queue. So in such case we'll just pop item and skip it if
        // we don't have a matching peer in peers map
        loop {
            let next_peer_id = match self.round_robin.pop() {
                Some(peer) => peer,
                None => match message {
                    Message::Greeting(_) => {
                        return Err(ZmqError::Socket("Sending greeting is not supported"))
                    }
                    Message::Command(_) => {
                        return Err(ZmqError::Socket("Sending commands is not supported"))
                    }
                    Message::Message(m) => {
                        return Err(ZmqError::ReturnToSender {
                            reason: "Not connected to peers. Unable to send messages",
                            message: m,
                        })
                    }
                },
            };
            let send_result = match self.peers.get_async(&next_peer_id).await {
                Some(mut peer) => peer.send_queue.send(message).await,
                None => continue,
            };
            return match send_result {
                Ok(()) => {
                    self.round_robin.push(next_peer_id.clone());
                    Ok(next_peer_id)
                }
                Err(e) => {
                    self.peer_disconnected(&next_peer_id);
                    Err(e.into())
                }
            };
        }
    }
}

impl SocketBackend for GenericSocketBackend {
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
impl MultiPeerBackend for GenericSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, send_queue) = io.into_parts();
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

                    Box::pin(async move {
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

        // Store heartbeat handle if spawned
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
