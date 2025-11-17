#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
//#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
//#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, warn};
//#[cfg(feature = "benchmark")]
use log::info;
use network::{ReliableSender, SimpleSender};
use primary::{Primary, PrimaryWorkerMessage, WorkerPrimaryMessage};
use std::collections::{HashMap, HashSet, VecDeque};
//#[cfg(feature = "benchmark")]

use primary::timer::Timer;
use std::convert::TryInto as _;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_util::time::DelayQueue;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

//The message type received by clients
pub type Transaction = Vec<u8>;
//The message type forwarded to quorum waiters
pub type Batch = Vec<Transaction>;

/// The view number (of consensus)
pub type View = u64;
// The slot (sequence) number of consensus
pub type Slot = u64;

#[derive(Clone, PartialEq, std::fmt::Debug)]
pub enum AsyncEffectType {
    Off = 0,
    TempBlip = 1,  //Send nothing for x seconds, and then release all messages
    Failure = 2,   //Send nothing for x seconds  //TODO: Combine with TempBlip?
    Partition = 3, //Send nothing to partitioned replicas for x seconds, then release all
    Egress = 4,    //For x seconds, delay all outbound messages by some amount
}
fn uint_to_enum(v: u8) -> AsyncEffectType {
    unsafe { std::mem::transmute(v) }
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,

    //tx_message: Sender<QuorumWaiterMessage>,  /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_batch: Sender<Vec<u8>>, // channel to forward batch digest to processor in order for primary to propose.

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    //network: ReliableSender,
    // Currently during asynchrony
    during_simulated_asynchrony: bool,
    // Current async effect type
    current_effect_type: AsyncEffectType,
    // Should simulate failure (determined at startup based on node index)
    should_simulate_failure: bool,
    // Partition public keys
    partition_public_keys: HashSet<PublicKey>,
    // Partition queue for batch requests
    partition_queue: VecDeque<WorkerMessage>,
    // Per-worker egress penalty (ms)
    egress_penalty: u64,
    // Store
    store: Store,
    // Quorum waiter
    tx_message: Sender<QuorumWaiterMessage>,
    //Simulating an async event
    pub simulate_asynchrony: bool,
    //Type of effects: 0 for delay full async duration, 1 for partition, 2 for  failure, 3 for egress delay. Will start #type many blips.
    pub asynchrony_type: VecDeque<u8>,
    //Start of async period   //offset from current time (in seconds) when to start next async effect
    pub asynchrony_start: VecDeque<u64>,
    //Duration of async period
    pub asynchrony_duration: VecDeque<u64>,
    ////first k nodes experience specified async behavior
    pub affected_nodes: VecDeque<u64>,
    ////public keys of the other works
    pub keys: Vec<PublicKey>,
    //name of the worker
    pub name: PublicKey,
    // async timer futures
    pub async_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = (Slot, View)> + Send>>>,
    // Whether this worker should apply egress delays
    should_simulate_egress: bool,
    // Queue for delaying outgoing messages during egress simulation
    egress_delay_queue: DelayQueue<(Vec<SocketAddr>, Bytes)>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>, //receiver channel from worker.TxReceiverHandler
        tx_message: Sender<QuorumWaiterMessage>, //sender channel to worker.QuorumWaiter
        tx_batch: Sender<Vec<u8>>,             // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        //partition_public_keys: HashSet<PublicKey>,
        mut store: Store,
        simulate_asynchrony: bool,
        asynchrony_type: VecDeque<u8>,
        asynchrony_start: VecDeque<u64>,
        asynchrony_duration: VecDeque<u64>,
        affected_nodes: VecDeque<u64>,
        keys: Vec<PublicKey>,
        name: PublicKey,
        egress_penalty: u64,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message, //previously forwarded batch to Quorum_waiter; now skipping this step.
                tx_batch,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: SimpleSender::new(),
                //network: ReliableSender::new(),
                during_simulated_asynchrony: false,
                current_effect_type: AsyncEffectType::Off,
                should_simulate_failure: false,
                //partition_public_keys,
                partition_public_keys: HashSet::new(),
                partition_queue: VecDeque::new(),
                egress_penalty,
                store,
                simulate_asynchrony,
                asynchrony_type,
                asynchrony_start,
                asynchrony_duration,
                affected_nodes,
                keys,
                name,
                async_timer_futures: FuturesUnordered::new(),
                should_simulate_egress: false,
                egress_delay_queue: DelayQueue::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let mut transaction_count = 0u64;
        let mut batch_count = 0u64;
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        if self.simulate_asynchrony {
            for i in 0..self.asynchrony_start.len() {
                let start_offset = self.asynchrony_start[i];
                let end_offset = start_offset + self.asynchrony_duration[i];

                let async_start = Timer::new(0, 0, start_offset);
                let async_end = Timer::new(0, 0, end_offset);

                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));

                let effect_type = uint_to_enum(self.asynchrony_type[i]);

                if effect_type == AsyncEffectType::Partition {
                    self.keys.sort();
                    let index = self.keys.binary_search(&self.name).unwrap();

                    // Figure out which partition we are in, partition_nodes indicates when the left partition ends
                    let mut start: usize = 0;
                    let mut end: usize = 0;

                    // We are in the right partition
                    if index > self.affected_nodes[i] as usize - 1 {
                        start = self.affected_nodes[i] as usize;
                        end = self.keys.len();
                    } else {
                        // We are in the left partition
                        start = 0;
                        end = self.affected_nodes[i] as usize;
                    }

                    // These are the nodes in our side of the partition
                    for j in start..end {
                        self.partition_public_keys.insert(self.keys[j]);
                    }

                    debug!("partition pks are {:?}", self.partition_public_keys);
                } else if effect_type == AsyncEffectType::Failure {
                    // Check if this worker should simulate failure
                    self.keys.sort();
                    let index = self.keys.binary_search(&self.name).unwrap();

                    // Only workers for nodes below affected_nodes threshold simulate failure
                    if index < self.affected_nodes[i] as usize {
                        self.should_simulate_failure = true;
                        debug!("Worker will simulate failure during async period {}", i);
                    }
                } else if effect_type == AsyncEffectType::Egress {
                    self.keys.sort();
                    let index = self.keys.binary_search(&self.name).unwrap();

                    if index < self.affected_nodes[i] as usize {
                        self.should_simulate_egress = true;
                        debug!("Worker will simulate egress delay during async period {}", i);
                    }
                }
            }
        }

        /*let timer1 = sleep(Duration::from_secs(10));
        tokio::pin!(timer1);
        let timer2 = sleep(Duration::from_secs(30));
        tokio::pin!(timer2);*/

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    transaction_count += 1;

                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal(&mut batch_count).await;

                        timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    //info!("BatchMaker: max batch delay timer triggered");
                    if !self.current_batch.is_empty() {
                        self.seal(&mut batch_count).await;
                    }

                    timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_batch_delay));
                },

                Some((slot, view)) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;

                    if self.during_simulated_asynchrony {
                        // Starting async period - determine effect type
                        if !self.asynchrony_type.is_empty() {
                            self.current_effect_type = uint_to_enum(self.asynchrony_type.pop_front().unwrap());
                            debug!("Worker async period started with effect type: {:?}", self.current_effect_type);
                        }
                    } else {
                        // Ending async period
                        debug!("Worker async period ended, effect was: {:?}", self.current_effect_type);
                        if self.current_effect_type == AsyncEffectType::Partition {
                            debug!("partition queue size is {:?}", self.partition_queue.len());
                        }
                        self.current_effect_type = AsyncEffectType::Off;
                    }
                },
                Some(result) = self.egress_delay_queue.next() => {
                    match result {
                        Ok(item) => {
                            let (addresses, bytes) = item.into_inner();
                            self.network.broadcast(addresses, bytes).await;
                        }
                        Err(e) => warn!("BatchMaker egress delay timer error: {}", e),
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                /*() = &mut timer1 => {
                    debug!("BatchMaker: partition delay timer 1 triggered");
                    self.during_simulated_asynchrony = true;
                    timer1.as_mut().reset(Instant::now() + Duration::from_secs(100));
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer2 => {
                    debug!("BatchMaker: partition delay timer 2 triggered");
                    debug!("partition queue size is {:?}", self.partition_queue.len());
                    self.during_simulated_asynchrony = false;
                    timer2.as_mut().reset(Instant::now() + Duration::from_secs(100));
                },*/



            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self, batch_count: &mut u64) {
        *batch_count += 1;
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for all txs and gather their client_id and counter.
        // Sample txs start with 0, non-sample txs start with 1.
        #[cfg(feature = "benchmark")]
        let all_tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.len() > 9 && (tx[0] == 0u8 || tx[0] == 1u8))
            .filter_map(|tx| {
                let tx_type = tx[0]; // 0 for sample, 1 for non-sample
                let client_id = tx[1];
                let counter_bytes: [u8; 8] = tx[2..10].try_into().ok()?;
                let counter = u64::from_be_bytes(counter_bytes);
                Some((tx_type, client_id, counter))
            })
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch, self.name);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            let sample_count = all_tx_ids
                .iter()
                .filter(|(tx_type, _, _)| *tx_type == 0u8)
                .count();
            let regular_count = all_tx_ids.len() - sample_count;
            if log::log_enabled!(log::Level::Debug) && (!all_tx_ids.is_empty()) {
                debug!(
                    "Batch {:?} aggregates {} sample tx(s) and {} regular tx(s)",
                    digest, sample_count, regular_count
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.

        //NEW:
        //Best-effort broadcast only. Any failure is correlated with the primary operating this node (running on same machine)

        let bytes = Bytes::from(serialized.clone());
        let digest = Digest(
            Sha512::digest(&serialized).as_slice()[..32]
                .try_into()
                .unwrap(),
        );

        // Store the batch.
        self.store.write(digest.to_vec(), serialized.clone()).await;

        // Send batch to processor
        if let Err(e) = self.tx_batch.send(serialized.clone()).await {
            error!("BatchMaker: Failed to send batch to processor: {}", e);
            panic!("BatchMaker channel to processor is closed");
        }
        if self.during_simulated_asynchrony {
            match self.current_effect_type {
                AsyncEffectType::Failure => {
                    // During failure, drop all outgoing messages if this worker should fail
                    if self.should_simulate_failure {
                        debug!("Worker failure simulation: dropping batch during failure period");
                        // Don't broadcast anything - simulate complete failure
                    } else {
                        // This worker is not affected by failure, send normally
                        let (_, addresses): (Vec<_>, _) =
                            self.workers_addresses.iter().cloned().unzip();
                        self.network.broadcast(addresses, bytes).await;
                    }
                }
                AsyncEffectType::Partition => {
                    debug!("BatchMaker: Simulated partition enabled. Only sending to partitioned keys from broadcast");
                    let new_addresses: Vec<_> = self
                        .workers_addresses
                        .iter()
                        .filter(|(pk, _)| self.partition_public_keys.contains(pk))
                        .map(|(_, addr)| addr)
                        .cloned()
                        .collect();
                    //let (_, addresses) = new_addresses.iter().cloned().unzip();
                    //debug!("addresses is {:?}", new_addresses);
                    self.partition_queue.push_back(message);
                    debug!("partition queue size is {:?}", self.partition_queue.len());
                    self.network.broadcast(new_addresses, bytes).await;
                }
                AsyncEffectType::Egress => {
                    if self.should_simulate_egress {
                        let (_, addresses): (Vec<_>, _) =
                            self.workers_addresses.iter().cloned().unzip();
                        self.egress_delay_queue.insert(
                            (addresses, bytes.clone()),
                            Duration::from_millis(self.egress_penalty),
                        );
                    } else {
                        let (_, addresses): (Vec<_>, _) =
                            self.workers_addresses.iter().cloned().unzip();
                        self.network.broadcast(addresses, bytes).await;
                    }
                }
                _ => {
                    // For other async types, send normally (could be extended for other effects)
                    let (_, addresses): (Vec<_>, _) =
                        self.workers_addresses.iter().cloned().unzip();
                    self.network.broadcast(addresses, bytes).await;
                }
            }
        } else {
            //debug!("sending batch normally");
            let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
            self.network.broadcast(addresses, bytes).await;
        }

        /*let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap());
        self.store.write(digest.to_vec(), serialized.clone()).await;
        self.tx_batch.send(serialized.clone()).await.expect("Failed to deliver batch");

        //OLD:
        //This uses reliable sender. The receiver worker will reply with an ack. The Reply Handler is passed to Quorum Waiter.
        let (names, addresses): (Vec<_>, Vec<_>) = self.workers_addresses.iter().cloned().unzip();
        let new_addresses: Vec<_> = self.workers_addresses.iter().filter(|(pk, _)| self.partition_public_keys.contains(pk)).map(|(_, addr)| addr).cloned().collect();
        let bytes = Bytes::from(serialized.clone());
        //let handlers = self.network.broadcast(addresses, bytes).await;
        let handlers = self.network.broadcast(new_addresses, bytes).await;

        // // Send the batch through the deliver channel for further processing.
        self.tx_message
             .send(QuorumWaiterMessage {
                batch: serialized,
                 handlers: names.into_iter().zip(handlers.into_iter()).collect(),
             })
             .await
            .expect("Failed to deliver batch");*/
    }
}
