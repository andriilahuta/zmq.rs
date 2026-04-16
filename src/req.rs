use crate::backend::DisconnectNotifier;
use crate::codec::*;
use crate::endpoint::Endpoint;
use crate::error::*;
use crate::reconnect::{ReconnectConfig, ReconnectHandle};
use crate::transport::AcceptStopHandle;
use crate::util::{Peer, PeerIdentity};
use crate::*;
use crate::{SocketType, ZmqResult};

use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_queue::SegQueue;
use futures::{SinkExt, StreamExt};

use std::collections::HashMap;
use std::sync::Arc;

struct ReqSocketBackend {
    pub(crate) peers: scc::HashMap<PeerIdentity, Peer>,
    pub(crate) round_robin: SegQueue<PeerIdentity>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: SocketOptions,
    /// Notifiers for reconnection tasks - keyed by `peer_id`
    disconnect_notifiers: Mutex<HashMap<PeerIdentity, DisconnectNotifier>>,
}

impl ReqSocketBackend {
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

pub struct ReqSocket {
    backend: Arc<ReqSocketBackend>,
    current_request: Option<PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
    /// Handles to background reconnection tasks
    reconnect_handles: Vec<ReconnectHandle>,
}

impl Drop for ReqSocket {
    fn drop(&mut self) {
        // Shutdown all reconnection tasks
        for handle in self.reconnect_handles.drain(..) {
            handle.shutdown();
        }
        self.backend.shutdown();
    }
}

#[async_trait]
impl SocketSend for ReqSocket {
    async fn send(&mut self, mut message: ZmqMessage) -> ZmqResult<()> {
        if self.current_request.is_some() {
            return Err(ZmqError::ReturnToSender {
                reason: "Unable to send message. Request already in progress",
                message,
            });
        }
        // In normal scenario this will always be only 1 iteration
        // There can be special case when peer has disconnected and his id is still in
        // RR queue This happens because SegQueue don't have an api to delete
        // items from queue. So in such case we'll just pop item and skip it if
        // we don't have a matching peer in peers map
        loop {
            let next_peer_id = match self.backend.round_robin.pop() {
                Some(peer) => peer,
                None => {
                    return Err(ZmqError::ReturnToSender {
                        reason: "Not connected to peers. Unable to send messages",
                        message,
                    })
                }
            };
            if let Some(mut peer) = self.backend.peers.get_async(&next_peer_id).await {
                self.backend.round_robin.push(next_peer_id.clone());
                message.push_front(Bytes::new());
                peer.send_queue.send(Message::Message(message)).await?;
                self.current_request = Some(next_peer_id);
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl SocketRecv for ReqSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        match self.current_request.take() {
            Some(peer_id) => {
                if let Some(mut peer) = self.backend.peers.get_async(&peer_id).await {
                    match peer.recv_queue.next().await {
                        Some(Ok(Message::Message(mut m))) => {
                            if m.len() < 2 {
                                return Err(ZmqError::Other(
                                    "Invalid message format: too few frames",
                                ));
                            }
                            if !m.pop_front().unwrap().is_empty() {
                                return Err(ZmqError::Other(
                                    "Invalid message format: missing delimiter",
                                ));
                            }
                            Ok(m)
                        }
                        Some(Ok(_)) => {
                            // Non-message frames should be ignored by the caller
                            Err(ZmqError::Other("Received non-message frame"))
                        }
                        Some(Err(error)) => Err(error.into()),
                        None => Err(ZmqError::NoMessage),
                    }
                } else {
                    Err(ZmqError::Other("Server disconnected"))
                }
            }
            None => Err(ZmqError::Other("Unable to recv. No request in progress")),
        }
    }
}

#[async_trait]
impl Socket for ReqSocket {
    fn with_options(options: SocketOptions) -> Self {
        Self {
            backend: Arc::new(ReqSocketBackend {
                peers: scc::HashMap::new(),
                round_robin: SegQueue::new(),
                socket_monitor: Mutex::new(None),
                socket_options: options,
                disconnect_notifiers: Mutex::new(HashMap::new()),
            }),
            current_request: None,
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
impl MultiPeerBackend for ReqSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, send_queue) = io.into_parts();
        self.peers
            .upsert_async(
                peer_id.clone(),
                Peer {
                    _identity: peer_id.clone(),
                    send_queue,
                    recv_queue,
                },
            )
            .await;
        self.round_robin.push(peer_id.clone());
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }

        self.peers.remove_sync(peer_id);

        // Notify reconnection task if registered
        if let Some(mut notifier) = self.disconnect_notifiers.lock().remove(peer_id) {
            // Use try_send to avoid blocking - if channel is full, the reconnect task
            // will eventually notice the peer is gone
            let _ = notifier.try_send(peer_id.clone());
        }
    }
}

impl SocketBackend for ReqSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::REQ
    }

    fn socket_options(&self) -> &SocketOptions {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.peers.clear_sync();
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}
