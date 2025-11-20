// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, warn};
use network::CancelHandler;
use network::ReliableSender;
use network::SimpleSender;
use primary::timer::Timer;
use primary::Height;
use primary::PrimaryWorkerMessage;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::time::DelayQueue;

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

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

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 100;

enum DelayedSyncMessage {
    Direct(SocketAddr, Bytes),
    Broadcast(Vec<SocketAddr>, Bytes),
}

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    //network: ReliableSender,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,

    cancel_handlers: HashMap<Digest, Vec<CancelHandler>>,

    // Failure simulation fields
    simulate_asynchrony: bool,
    asynchrony_type: VecDeque<u8>,
    asynchrony_start: VecDeque<u64>,
    asynchrony_duration: VecDeque<u64>,
    affected_nodes: VecDeque<u64>,
    keys: Vec<PublicKey>,
    during_simulated_asynchrony: bool,
    current_effect_type: AsyncEffectType,
    should_simulate_failure: bool,
    should_simulate_egress: bool,
    egress_penalty: u64,
    egress_delay_queue: DelayQueue<DelayedSyncMessage>,
    // Timers for async period transitions
    async_timer_futures: FuturesUnordered<Pin<Box<Timer>>>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
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

            let mut synchronizer = Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                //network: ReliableSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
                cancel_handlers: HashMap::new(),
                simulate_asynchrony,
                asynchrony_type,
                asynchrony_start,
                asynchrony_duration,
                affected_nodes,
                keys,
                during_simulated_asynchrony: false,
                current_effect_type: AsyncEffectType::Off,
                should_simulate_failure: false,
                should_simulate_egress: false,
                egress_penalty,
                egress_delay_queue: DelayQueue::new(),
                async_timer_futures: FuturesUnordered::new(),
            };

            // Determine if this worker should simulate failure
            if synchronizer.simulate_asynchrony {
                for i in 0..synchronizer.asynchrony_start.len() {
                    let effect_type = uint_to_enum(synchronizer.asynchrony_type[i]);
                    if effect_type == AsyncEffectType::Failure {
                        let index = synchronizer.keys.binary_search(&synchronizer.name).unwrap();
                        if index < synchronizer.affected_nodes[i] as usize {
                            synchronizer.should_simulate_failure = true;
                            debug!(
                                "Synchronizer will simulate failure during async period {}",
                                i
                            );
                        }
                    } else if effect_type == AsyncEffectType::Egress {
                        let index = synchronizer.keys.binary_search(&synchronizer.name).unwrap();
                        if index < synchronizer.affected_nodes[i] as usize {
                            synchronizer.should_simulate_egress = true;
                            debug!(
                                "Synchronizer will simulate egress delay during async period {}",
                                i
                            );
                        }
                    }
                }
            }

            synchronizer.run().await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        // Set up async period timers if simulation is enabled
        if self.simulate_asynchrony {
            let configured_types: Vec<_> = self.asynchrony_type.iter().copied().collect();
            let configured_starts: Vec<_> = self.asynchrony_start.iter().copied().collect();
            let configured_durations: Vec<_> = self.asynchrony_duration.iter().copied().collect();
            let configured_affected: Vec<_> = self.affected_nodes.iter().copied().collect();
            let self_index = self.keys.binary_search(&self.name).unwrap();
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
                    // Create start and end timers for this async period
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

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digest_worker_pairs, target) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();
                        debug!("Received sync request for {:?} batches", digest_worker_pairs.len());
                        let mut missing = Vec::new();
                        for (digest, worker_id) in digest_worker_pairs {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Check if we have the batch from the specific worker using composite key.
                            //let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {} from worker {}", digest, worker_id);
                                },
                                Ok(Some(_)) => {
                                    debug!("already have batch {} from worker {}", digest, worker_id);
                                    // The batch arrived in the meantime: no need to request it.
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        // Send sync request to a single node. If this fails, we will send it
                        // to other nodes when a timer times out.
                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        let message = WorkerMessage::BatchRequest(missing.clone(), self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        let bytes = Bytes::from(serialized);

                        debug!("Requesting sync for missing {:?}, address is {:?}", missing, address);

                        // Check if we should drop or delay messages during simulation
                        if self.during_simulated_asynchrony &&
                           self.current_effect_type == AsyncEffectType::Failure &&
                           self.should_simulate_failure {
                            debug!("Synchronizer failure simulation: dropping sync request during failure period");
                            // Don't send any sync requests - simulate complete failure
                        } else if self.during_simulated_asynchrony &&
                                  self.current_effect_type == AsyncEffectType::Egress &&
                                  self.should_simulate_egress {
                            debug!("Synchronizer egress simulation: delaying sync request during egress period");
                            self.egress_delay_queue.insert(
                                DelayedSyncMessage::Direct(address, bytes),
                                Duration::from_millis(self.egress_penalty),
                            );
                        } else {
                            self.network.send(address, bytes).await;
                        }

                    },
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        /*self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);*/
                    },
                    _ => {},
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        debug!("Got from helper batch {}", digest);
                        self.pending.remove(&digest);
                        //self.cancel_handlers.remove(&digest);
                    },
                    Ok(None) => {
                        // The sync request for this batch has been canceled.
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        // Check if we should drop messages during failure simulation
                        if self.during_simulated_asynchrony &&
                           self.current_effect_type == AsyncEffectType::Failure &&
                           self.should_simulate_failure {
                            debug!("Synchronizer failure simulation: dropping retry sync requests during failure period");
                            // Don't send any retry sync requests - simulate complete failure
                        } else {
                            let addresses = self.committee
                                .others_workers(&self.name, &self.id)
                                .iter().map(|(_, address)| address.worker_to_worker)
                                .collect();
                            let message = WorkerMessage::BatchRequest(retry.clone(), self.name);
                            let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                            let bytes = Bytes::from(serialized);
                            debug!("Sending retry sync requests for {:?}", retry);

                            if self.during_simulated_asynchrony &&
                               self.current_effect_type == AsyncEffectType::Egress &&
                               self.should_simulate_egress {
                                debug!("Synchronizer egress simulation: delaying retry sync request during egress period");
                                self.egress_delay_queue.insert(
                                    DelayedSyncMessage::Broadcast(addresses, bytes),
                                    Duration::from_millis(self.egress_penalty),
                                );
                            } else {
                                self.network
                                    .lucky_broadcast(addresses, bytes, self.sync_retry_nodes)
                                    .await;
                            }
                        }
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },

                // Handle async period timer events
                Some((slot, view)) = self.async_timer_futures.next() => {
                    // Toggle the async period state
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;

                    if self.during_simulated_asynchrony {
                        // Starting a new async period - pop the next effect type
                        if let Some(effect_raw) = self.asynchrony_type.pop_front() {
                            self.current_effect_type = uint_to_enum(effect_raw);
                            let duration = self.asynchrony_duration.pop_front().unwrap_or(0);
                            debug!("Synchronizer async period started with effect type: {:?}", self.current_effect_type);

                            if self.current_effect_type == AsyncEffectType::Slowdown {
                                if duration > 0 {
                                    sleep(Duration::from_millis(duration)).await;
                                }
                                self.during_simulated_asynchrony = false;
                                self.current_effect_type = AsyncEffectType::Off;
                            }
                        }
                    } else {
                        // Ending async period
                        debug!("Synchronizer async period ended, effect was: {:?}", self.current_effect_type);
                        self.current_effect_type = AsyncEffectType::Off;
                    }
                },

                Some(result) = self.egress_delay_queue.next() => {
                    match result {
                        Ok(item) => match item.into_inner() {
                            DelayedSyncMessage::Direct(address, bytes) => {
                                self.network.send(address, bytes).await;
                            }
                            DelayedSyncMessage::Broadcast(addresses, bytes) => {
                                self.network
                                    .lucky_broadcast(addresses, bytes, self.sync_retry_nodes)
                                    .await;
                            }
                        },
                        Err(e) => warn!("Synchronizer egress delay timer error: {}", e),
                    }
                }
            }
        }
    }
}
