// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{BatchTransactionMap, SlotTransactionReply};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::Digest;
use crypto::PublicKey;
use futures::sink::SinkExt;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use primary::timer::Timer;
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::time::DelayQueue;

#[derive(Copy, Clone, PartialEq, std::fmt::Debug)]
pub enum AsyncEffectType {
    Off = 0,
    TempBlip = 1,
    Failure = 2,
    Partition = 3,
    Egress = 4,
    Slowdown = 5,
}

fn uint_to_enum(v: u8) -> AsyncEffectType {
    unsafe { std::mem::transmute(v) }
}

struct PendingReply {
    client_id: u8,
    reply: SlotTransactionReply,
}

/// The ReplySender component receives SlotCommittedMessage notifications from the primary
/// and sends transaction replies to clients using the same connection pattern that clients
/// use to send transactions to workers.
pub struct ReplySender {
    /// Receiver for slot committed messages containing (slot, batch_digests).
    rx_slot_committed: Receiver<(u64, HashSet<Digest>)>,
    /// Shared batch metadata cache populated by processors.
    batch_to_transactions: BatchTransactionMap,
    /// Committee configuration for looking up client addresses.
    /// This follows the same pattern as workers looking up addresses from committee.
    committee: Committee,
    /// The worker identifier (used to annotate replies).
    worker_id: WorkerId,
    /// The worker's public key (used as reply identifier).
    worker_key: PublicKey,
    /// Async simulation configuration.
    simulate_asynchrony: bool,
    asynchrony_type: VecDeque<u8>,
    asynchrony_start: VecDeque<u64>,
    asynchrony_duration: VecDeque<u64>,
    affected_nodes: VecDeque<u64>,
    keys: Vec<PublicKey>,
    during_simulated_asynchrony: bool,
    current_effect_type: AsyncEffectType,
    should_simulate_egress: bool,
    async_timer_futures: FuturesUnordered<Pin<Box<Timer>>>,
    egress_penalty: u64,
    egress_delay_queue: DelayQueue<PendingReply>,
}

impl ReplySender {
    async fn handle_slot_committed(
        &mut self,
        slot: u64,
        batch_digests: HashSet<Digest>,
        client_transports: &mut HashMap<u8, Framed<TcpStream, LengthDelimitedCodec>>,
    ) {
        debug!(
            "ReplySender: received slot {} with {} batch digests",
            slot,
            batch_digests.len()
        );

        // Group transactions by client_id
        let mut client_transactions: HashMap<u8, Vec<u64>> = HashMap::new();

        let mut processed_digests: Vec<Digest> = Vec::new();
        let mut digest_metadata: Vec<(Digest, Arc<Vec<(u8, u64)>>)> = Vec::new();
        {
            let map = self.batch_to_transactions.lock().unwrap();
            for digest in &batch_digests {
                if let Some(entries) = map.get(digest) {
                    digest_metadata.push((digest.clone(), entries.clone()));
                } else {
                    warn!(
                        "ReplySender: missing metadata for digest {:?}, skipping reply",
                        digest
                    );
                }
            }
        }

        for (digest, entries) in &digest_metadata {
            for (client_id, counter) in entries.iter() {
                client_transactions
                    .entry(*client_id)
                    .or_insert_with(Vec::new)
                    .push(*counter);
            }
            processed_digests.push(digest.clone());
        }

        for (client_id, counters) in client_transactions {
            let reply = SlotTransactionReply {
                slot,
                worker_key: self.worker_key,
                committed_transactions: counters,
            };

            if self.should_delay_egress() {
                self.egress_delay_queue.insert(
                    PendingReply { client_id, reply },
                    Duration::from_millis(self.egress_penalty),
                );
            } else {
                self.send_reply_with_transport(client_id, reply, client_transports)
                    .await;
            }
        }

        if !processed_digests.is_empty() {
            let mut map = self.batch_to_transactions.lock().unwrap();
            for digest in processed_digests {
                map.remove(&digest);
            }
        }
    }

    async fn send_reply_with_transport(
        &self,
        client_id: u8,
        reply: SlotTransactionReply,
        client_transports: &mut HashMap<u8, Framed<TcpStream, LengthDelimitedCodec>>,
    ) {
        if !client_transports.contains_key(&client_id) {
            // Try to establish connection lazily
            if let Ok(client_info) = self.committee.client(&client_id) {
                match TcpStream::connect(&client_info.replies).await {
                    Ok(stream) => {
                        let transport = Framed::new(stream, LengthDelimitedCodec::new());
                        client_transports.insert(client_id, transport);
                        info!(
                            "ReplySender connected to client {} at {} (lazy connection)",
                            client_id, client_info.replies
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Failed to connect to client {} at {}: {}",
                            client_id, client_info.replies, e
                        );
                        return;
                    }
                }
            } else {
                debug!("Client {} not found in committee", client_id);
                return;
            }
        }

        if let Some(transport) = client_transports.get_mut(&client_id) {
            if let Err(e) = self
                .send_reply_to_client(client_id, reply, transport)
                .await
            {
                debug!("Failed to send reply to client {}: {}", client_id, e);
            }
        }
    }

    fn should_delay_egress(&self) -> bool {
        self.during_simulated_asynchrony
            && self.current_effect_type == AsyncEffectType::Egress
            && self.should_simulate_egress
    }

    pub fn spawn(
        rx_slot_committed: Receiver<(u64, HashSet<Digest>)>,
        batch_to_transactions: BatchTransactionMap,
        committee: Committee,
        worker_id: WorkerId,
        worker_key: PublicKey,
        simulate_asynchrony: bool,
        asynchrony_type: VecDeque<u8>,
        asynchrony_start: VecDeque<u64>,
        asynchrony_duration: VecDeque<u64>,
        affected_nodes: VecDeque<u64>,
        egress_penalty: u64,
    ) {
        tokio::spawn(async move {
            let mut keys: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
            keys.sort();

            let mut reply_sender = Self {
                rx_slot_committed,
                batch_to_transactions,
                committee,
                worker_id,
                worker_key,
                simulate_asynchrony,
                asynchrony_type,
                asynchrony_start,
                asynchrony_duration,
                affected_nodes,
                keys,
                during_simulated_asynchrony: false,
                current_effect_type: AsyncEffectType::Off,
                should_simulate_egress: false,
                async_timer_futures: FuturesUnordered::new(),
                egress_penalty,
                egress_delay_queue: DelayQueue::new(),
            };

            if reply_sender.simulate_asynchrony {
                for i in 0..reply_sender.asynchrony_start.len() {
                    let effect_type = uint_to_enum(reply_sender.asynchrony_type[i]);
                    if effect_type == AsyncEffectType::Egress {
                        if let Ok(index) = reply_sender.keys.binary_search(&reply_sender.worker_key) {
                            if index < reply_sender.affected_nodes[i] as usize {
                                reply_sender.should_simulate_egress = true;
                                debug!(
                                    "ReplySender will simulate egress delay during async period {}",
                                    i
                                );
                            }
                        }
                    }
                }
            }

            reply_sender.run().await;
        });
    }

    async fn run(&mut self) {
        info!(
            "ReplySender started with {} clients in committee",
            self.committee.clients.len()
        );

        if self.simulate_asynchrony {
            let configured_types: Vec<_> = self.asynchrony_type.iter().copied().collect();
            let configured_starts: Vec<_> = self.asynchrony_start.iter().copied().collect();
            let configured_durations: Vec<_> = self.asynchrony_duration.iter().copied().collect();
            let configured_affected: Vec<_> = self.affected_nodes.iter().copied().collect();
            let self_index = self.keys.binary_search(&self.worker_key).unwrap();
            let mut scheduled_types = VecDeque::new();
            let mut scheduled_durations = VecDeque::new();

            for i in 0..configured_starts.len() {
                let effect_type = uint_to_enum(configured_types[i]);
                let start_offset = configured_starts[i] * 1000;
                let duration = configured_durations[i] * 1000;
                let affected = configured_affected[i] as usize;

                if matches!(
                    effect_type,
                    AsyncEffectType::Failure | AsyncEffectType::Egress | AsyncEffectType::Slowdown
                ) && self_index >= affected
                {
                    continue;
                }

                if effect_type == AsyncEffectType::Slowdown {
                    let async_start = Timer::new(0, 0, start_offset);
                    self.async_timer_futures.push(Box::pin(async_start));
                } else {
                    let async_start = Timer::new(0, 0, start_offset);
                    let async_end = Timer::new(0, 0, start_offset + duration);

                    self.async_timer_futures.push(Box::pin(async_start));
                    self.async_timer_futures.push(Box::pin(async_end));
                }

                scheduled_types.push_back(configured_types[i]);
                scheduled_durations.push_back(duration);
            }

            self.asynchrony_type = scheduled_types;
            self.asynchrony_duration = scheduled_durations;
        }

        // Initialize empty connection map - connections will be established lazily
        let mut client_transports = HashMap::new();

        loop {
            tokio::select! {
                Some((slot, batch_digests)) = self.rx_slot_committed.recv() => {
                    self.handle_slot_committed(slot, batch_digests, &mut client_transports).await;
                }
                Some((_, _)) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;
                    if self.during_simulated_asynchrony {
                        if let Some(effect_raw) = self.asynchrony_type.pop_front() {
                            self.current_effect_type = uint_to_enum(effect_raw);
                            let duration = self.asynchrony_duration.pop_front().unwrap_or(0);
                            debug!("ReplySender async period started with effect type: {:?}", self.current_effect_type);

                            if self.current_effect_type == AsyncEffectType::Slowdown {
                                if duration > 0 {
                                    sleep(Duration::from_millis(duration)).await;
                                }
                                self.during_simulated_asynchrony = false;
                                self.current_effect_type = AsyncEffectType::Off;
                            }
                        }
                    } else {
                        debug!("ReplySender async period ended, effect was: {:?}", self.current_effect_type);
                        self.current_effect_type = AsyncEffectType::Off;
                    }
                }
                Some(result) = self.egress_delay_queue.next() => {
                    match result {
                        Ok(item) => {
                            let pending = item.into_inner();
                            self.send_reply_with_transport(pending.client_id, pending.reply, &mut client_transports).await;
                        }
                        Err(e) => warn!("ReplySender egress delay timer error: {}", e),
                    }
                }
                else => {
                    break;
                }
            }
        }
    }

    async fn send_reply_to_client(
        &self,
        client_id: u8,
        reply: SlotTransactionReply,
        transport: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(
            "Sending reply to client {}: slot {} with {} transactions",
            client_id,
            reply.slot,
            reply.committed_transactions.len()
        );

        // Serialize the reply
        let message = bincode::serialize(&reply)?;
        let bytes = Bytes::from(message);

        // Send the reply using the existing transport
        transport.send(bytes).await?;

        info!(
            "Sent SlotTransactionReply to client {} for slot {} with {} transactions",
            client_id,
            reply.slot,
            reply.committed_transactions.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::ClientAddresses;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_reply_sender_creation() {
        let (tx, rx) = mpsc::channel(10);
        let batch_to_transactions: BatchTransactionMap =
            Arc::new(std::sync::Mutex::new(HashMap::new()));

        // Create a committee with test clients
        let mut committee = Committee::new(vec![]);
        committee.add_client(
            0,
            ClientAddresses {
                replies: "127.0.0.1:8000".parse().unwrap(),
                transaction_acks: "127.0.0.1:8001".parse().unwrap(),
            },
        );

        // This should not panic
        ReplySender::spawn(
            rx,
            batch_to_transactions,
            committee,
            0,
            PublicKey::default(),
            false,
            VecDeque::new(),
            VecDeque::new(),
            VecDeque::new(),
            VecDeque::new(),
            0,
        );
    }
}
