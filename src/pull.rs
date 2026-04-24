use crate::backend::GenericSocketBackend;
use crate::codec::{Message, ZmqCommand, ZmqCommandName, ZmqFramedRead};
use crate::fair_queue::FairQueue;
use crate::transport::AcceptStopHandle;
use crate::util::PeerIdentity;
use crate::{
    Endpoint, MultiPeerBackend, Socket, SocketEvent, SocketOptions, SocketRecv, SocketType,
    ZmqMessage, ZmqResult,
};

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PullSocket {
    backend: Arc<GenericSocketBackend>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

#[async_trait]
impl Socket for PullSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(GenericSocketBackend::with_options(
            Some(fair_queue.inner()),
            SocketType::PULL,
            options,
        ));

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

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle, RandomState> {
        &mut self.binds
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[async_trait]
impl SocketRecv for PullSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((peer_id, Ok(Message::Message(message)))) => {
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id)
                    {
                        heartbeat_tuple.0.record_activity();
                    }
                    return Ok(message);
                }
                Some((peer_id, Ok(Message::Command(cmd)))) =>
                {
                    #[expect(clippy::match_wildcard_for_single_variants)]
                    match cmd.name {
                        ZmqCommandName::PING => {
                            if let Some(heartbeat_tuple) =
                                self.backend.heartbeats.lock().get_mut(&peer_id)
                            {
                                heartbeat_tuple.0.received_ping(cmd.ttl);
                            }
                            if let Some(mut peer) = self.backend.peers.get_async(&peer_id).await {
                                let pong = ZmqCommand::pong(cmd.context.clone());
                                if let Err(e) = peer.send_queue.send(Message::Command(pong)).await {
                                    log::warn!("Failed to send PONG to peer {:?}: {}", peer_id, e);
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
                Some((peer_id, Ok(Message::Greeting(_)))) => {
                    if let Some(heartbeat_tuple) = self.backend.heartbeats.lock().get_mut(&peer_id)
                    {
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
                self.backend.peer_disconnected(&peer_id);
            }
        }
    }
}
