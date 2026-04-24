use crate::backend::DisconnectNotifier;
use crate::codec::*;
use crate::endpoint::Endpoint;
use crate::error::*;
use crate::fair_queue::{FairQueue, QueueInner};
use crate::heartbeat::{spawn_heartbeat_task, ActivityTracker, HeartbeatHandle};
use crate::reconnect::{ReconnectConfig, ReconnectHandle};
use crate::transport::AcceptStopHandle;
use crate::*;
use crate::{SocketType, ZmqResult};

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

struct RepPeer {
    pub(crate) _identity: PeerIdentity,
    pub(crate) send_queue: ZmqFramedWrite,
}

struct RepSocketBackend {
    pub(crate) peers: scc::HashMap<PeerIdentity, RepPeer>,
    fair_queue_inner: Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: SocketOptions,
    /// Notifiers for reconnection tasks - keyed by `peer_id`
    disconnect_notifiers: Mutex<HashMap<PeerIdentity, DisconnectNotifier>>,
    /// Heartbeat activity trackers and tasks - keyed by `peer_id`
    heartbeats: Mutex<HashMap<PeerIdentity, (ActivityTracker, Option<HeartbeatHandle>)>>,
}

impl RepSocketBackend {
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

pub struct RepSocket {
    backend: Arc<RepSocketBackend>,
    envelope: Option<ZmqMessage>,
    current_request: Option<PeerIdentity>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
    /// Handles to background reconnection tasks
    reconnect_handles: Vec<ReconnectHandle>,
}

impl Drop for RepSocket {
    fn drop(&mut self) {
        // Shutdown all reconnection tasks
        for handle in self.reconnect_handles.drain(..) {
            handle.shutdown();
        }
        self.backend.shutdown();
    }
}

#[async_trait]
impl Socket for RepSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(RepSocketBackend {
            peers: scc::HashMap::new(),
            fair_queue_inner: fair_queue.inner(),
            socket_monitor: Mutex::new(None),
            socket_options: options,
            disconnect_notifiers: Mutex::new(HashMap::new()),
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
            envelope: None,
            current_request: None,
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

#[async_trait]
impl MultiPeerBackend for RepSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, send_queue) = io.into_parts();

        self.peers
            .upsert_async(
                peer_id.clone(),
                RepPeer {
                    _identity: peer_id.clone(),
                    send_queue,
                },
            )
            .await;

        // Initialize heartbeat tracker for this peer
        let heartbeat_config = self
            .socket_options
            .heartbeat_config
            .clone()
            .unwrap_or_default();
        let (activity_tracker, hb_handle) = if self.socket_options.heartbeat_enabled {
            let activity_tracker = ActivityTracker::new(heartbeat_config.clone());

            let backend_weak = Arc::downgrade(&self);
            let peer_id_for_task = peer_id.clone();
            let heartbeat_clone = activity_tracker.clone();

            let send_ping_callback =
                {
                    let peer_id = peer_id.clone();
                    let backend_weak = backend_weak.clone();
                    let config = heartbeat_config.clone();

                    move || {
                        let peer_id = peer_id.clone();
                        let backend_weak = backend_weak.clone();
                        let config = config.clone();

                        Box::pin(async move {
                            if let Some(backend) = backend_weak.upgrade() {
                                if let Some(mut peer) = backend.peers.get_async(&peer_id).await {
                                    let ping = ZmqCommand::ping(config.ttl, None);
                                    peer.send_queue.send(Message::Command(ping)).await.map_err(
                                        |e| std::io::Error::new(ErrorKind::Other, e.to_string()),
                                    )
                                } else {
                                    Err(std::io::Error::new(
                                        ErrorKind::ConnectionAborted,
                                        "Peer not found",
                                    ))
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

            let hb_handle = spawn_heartbeat_task(
                heartbeat_clone,
                peer_id_for_task.clone(),
                send_ping_callback,
                on_timeout_callback,
            );
            (activity_tracker, Some(hb_handle))
        } else {
            // Heartbeat disabled - Create tracker for PONG responses only
            let activity_tracker = ActivityTracker::new(heartbeat_config);
            (activity_tracker, None)
        };

        self.heartbeats
            .lock()
            .insert(peer_id.clone(), (activity_tracker.clone(), hb_handle));

        self.fair_queue_inner
            .lock()
            .insert(peer_id.clone(), recv_queue);
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }
        self.peers.remove_sync(peer_id);
        self.fair_queue_inner.lock().remove(peer_id);

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

impl SocketBackend for RepSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::REP
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.peers.clear_sync();
        // Clear fair_queue streams to ensure TCP connections are closed
        // even when reconnect tasks still hold Arc references to the backend
        self.fair_queue_inner.lock().clear();
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl SocketSend for RepSocket {
    async fn send(&mut self, mut message: ZmqMessage) -> ZmqResult<()> {
        match self.current_request.take() {
            Some(peer_id) => {
                if let Some(mut peer) = self.backend.peers.get_async(&peer_id).await {
                    if let Some(envelope) = self.envelope.take() {
                        message.prepend(&envelope);
                    }
                    peer.send_queue.send(Message::Message(message)).await?;
                    Ok(())
                } else {
                    Err(ZmqError::ReturnToSender {
                        reason: "Client disconnected",
                        message,
                    })
                }
            }
            None => Err(ZmqError::ReturnToSender {
                reason: "Unable to send reply. No request in progress",
                message,
            }),
        }
    }
}

#[async_trait]
impl SocketRecv for RepSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((peer_id, Ok(message))) => {
                    match message {
                        Message::Message(mut m) => {
                            if let Some(heartbeat_tuple) =
                                self.backend.heartbeats.lock().get_mut(&peer_id)
                            {
                                heartbeat_tuple.0.record_activity();
                            }
                            let mut at = 1;
                            for (index, frame) in m.iter().enumerate() {
                                if frame.is_empty() {
                                    // Include delimiter in envelope.
                                    at = index + 1;
                                    break;
                                }
                            }
                            let data = m.split_off(at);
                            self.envelope = Some(m);
                            self.current_request = Some(peer_id);
                            return Ok(data);
                        }
                        Message::Command(cmd) =>
                        {
                            #[expect(clippy::match_wildcard_for_single_variants)]
                            match cmd.name {
                                ZmqCommandName::PING => {
                                    if let Some(heartbeat_tuple) =
                                        self.backend.heartbeats.lock().get_mut(&peer_id)
                                    {
                                        heartbeat_tuple.0.received_ping(cmd.ttl);
                                    }
                                    if let Some(mut peer) =
                                        self.backend.peers.get_async(&peer_id).await
                                    {
                                        let pong = ZmqCommand::pong(cmd.context.clone());
                                        if let Err(e) =
                                            peer.send_queue.send(Message::Command(pong)).await
                                        {
                                            log::warn!(
                                                "Failed to send PONG to peer {:?}: {}",
                                                peer_id,
                                                e
                                            );
                                        }
                                    }
                                }
                                ZmqCommandName::PONG => {
                                    if let Some(heartbeat_tuple) =
                                        self.backend.heartbeats.lock().get_mut(&peer_id)
                                    {
                                        heartbeat_tuple.0.received_pong();
                                    }
                                }
                                _ => {
                                    if let Some(heartbeat_tuple) =
                                        self.backend.heartbeats.lock().get_mut(&peer_id)
                                    {
                                        heartbeat_tuple.0.record_activity();
                                    }
                                }
                            }
                        }
                        Message::Greeting(_) => {
                            if let Some(heartbeat_tuple) =
                                self.backend.heartbeats.lock().get_mut(&peer_id)
                            {
                                heartbeat_tuple.0.record_activity();
                            }
                        }
                    }
                }
                Some((peer_id, Err(e))) => {
                    self.backend.peer_disconnected(&peer_id);
                    return Err(e.into());
                }
                None => {}
            };

            // Check if any peers have timed out
            let mut timed_out_peers = Vec::new();
            self.backend
                .heartbeats
                .lock()
                .iter()
                .for_each(|(peer_id, heartbeat_tuple)| {
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
