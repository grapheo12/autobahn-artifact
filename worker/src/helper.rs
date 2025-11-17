// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, warn};
use network::CancelHandler;
use network::{ReliableSender, SimpleSender};
use primary::timer::Timer;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use store::Store;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, Duration};
use tokio_util::time::DelayQueue;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

#[derive(Clone, PartialEq, std::fmt::Debug)]
pub enum AsyncEffectType {
    Off = 0,
    TempBlip = 1,
    Failure = 2,
    Partition = 3,
    Egress = 4,
}

fn uint_to_enum(v: u8) -> AsyncEffectType {
    unsafe { std::mem::transmute(v) }
}

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
    //network: ReliableSender,
    // Cancel handlers
    cancel_handlers: Vec<CancelHandler>,
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
    egress_delay_queue: DelayQueue<(SocketAddr, Bytes)>,
    // Timers for async period transitions
    async_timer_futures: FuturesUnordered<Pin<Box<Timer>>>,
}

impl Helper {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
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

            let mut helper = Self {
                name,
                id,
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
                //network: ReliableSender::new(),
                cancel_handlers: Vec::new(),
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
            if helper.simulate_asynchrony {
                for i in 0..helper.asynchrony_start.len() {
                    let effect_type = uint_to_enum(helper.asynchrony_type[i]);
                    if effect_type == AsyncEffectType::Failure {
                        let index = helper.keys.binary_search(&helper.name).unwrap();
                        if index < helper.affected_nodes[i] as usize {
                            helper.should_simulate_failure = true;
                            debug!("Helper will simulate failure during async period {}", i);
                        }
                    } else if effect_type == AsyncEffectType::Egress {
                        let index = helper.keys.binary_search(&helper.name).unwrap();
                        if index < helper.affected_nodes[i] as usize {
                            helper.should_simulate_egress = true;
                            debug!("Helper will simulate egress delay during async period {}", i);
                        }
                    }
                }
            }

            helper.run().await;
        });
    }

    async fn run(&mut self) {
        // Set up async period timers if simulation is enabled
        if self.simulate_asynchrony {
            for i in 0..self.asynchrony_start.len() {
                let start_offset = self.asynchrony_start[i] * 1000; // Convert seconds to milliseconds
                let end_offset = start_offset + (self.asynchrony_duration[i] * 1000);

                // Create start and end timers for this async period
                let async_start = Timer::new(0, 0, start_offset);
                let async_end = Timer::new(0, 0, end_offset);

                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));
            }
        }

        loop {
            tokio::select! {
                // Handle batch requests
                Some((digests, origin)) = self.rx_request.recv() => {
                    // TODO [issue #7]: Do some accounting to prevent bad nodes from monopolizing our resources.
                    debug!("Received helper batch request {:?}", digests);

                    // Get the requestor's address
                    let address = match self.committee.worker(&origin, &self.id) {
                        Ok(x) => x.worker_to_worker,
                        Err(e) => {
                            warn!("Unexpected batch request: {}", e);
                            continue;
                        }
                    };

                    // Check if we should drop messages during failure simulation
                    if self.during_simulated_asynchrony &&
                       self.current_effect_type == AsyncEffectType::Failure &&
                       self.should_simulate_failure {
                        debug!("Helper failure simulation: dropping batch response during failure period");
                        // Don't send any responses - simulate complete failure
                        continue;
                    }

                    // Reply to the request (the best we can)
                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => {
                                debug!("have digest {:?} in store", digest);
                                let bytes = Bytes::from(data);
                                if self.during_simulated_asynchrony
                                    && self.current_effect_type == AsyncEffectType::Egress
                                    && self.should_simulate_egress
                                {
                                    self.egress_delay_queue.insert(
                                        (address, bytes),
                                        Duration::from_millis(self.egress_penalty),
                                    );
                                } else {
                                    self.network.send(address, bytes).await;
                                }
                            },
                            Ok(None) => {
                                debug!("don't have digest {:?} in store", digest);
                            },
                            Err(e) => error!("{}", e),
                        }
                    }
                },

                // Handle async period timer events
                Some((slot, view)) = self.async_timer_futures.next() => {
                    // Toggle the async period state
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;

                    if self.during_simulated_asynchrony {
                        // Starting a new async period - pop the next effect type
                        if !self.asynchrony_type.is_empty() {
                            self.current_effect_type = uint_to_enum(self.asynchrony_type.pop_front().unwrap());
                            debug!("Helper async period started with effect type: {:?}", self.current_effect_type);
                        }
                    } else {
                        // Ending async period
                        debug!("Helper async period ended, effect was: {:?}", self.current_effect_type);
                        self.current_effect_type = AsyncEffectType::Off;
                    }
                },

                Some(result) = self.egress_delay_queue.next() => {
                    match result {
                        Ok(item) => {
                            let (address, bytes) = item.into_inner();
                            self.network.send(address, bytes).await;
                        }
                        Err(e) => warn!("Helper egress delay timer error: {}", e),
                    }
                }
            }
        }
    }
}
