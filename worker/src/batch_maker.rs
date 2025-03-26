#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::{Digest, Hash, PublicKey};
use ed25519_dalek::{Digest as _, Sha512};
// #[cfg(feature = "benchmark")]
// use ed25519_dalek::{Digest as _, Sha512};
use log::{debug, info};
use network::{ReliableSender, SimpleSender};
use tokio::sync::oneshot;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use std::convert::TryInto;
use rand::prelude::*;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

//The message type received by clients
pub type Transaction = Vec<u8>;
//The message type forwarded to quorum waiters
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<(Transaction, oneshot::Sender<()>)>,

    rx_batch_commit: Receiver<Digest>,
   
    //tx_message: Sender<QuorumWaiterMessage>,  /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_batch: Sender<Vec<u8>>,   // channel to forward batch digest to processor in order for primary to propose.

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    current_batch_waiters: Vec<oneshot::Sender<()>>,
    all_batch_waiters: HashMap<Digest, Vec<oneshot::Sender<()>>>, // batch hash => vec of waiting chans
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<(Transaction, oneshot::Sender<()>)>, //receiver channel from worker.TxReceiverHandler 
        //tx_message: Sender<QuorumWaiterMessage>, //sender channel to worker.QuorumWaiter
        tx_batch: Sender<Vec<u8>>,   // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        rx_batch_commit: Receiver<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                //tx_message, //previously forwarded batch to Quorum_waiter; now skipping this step.
                tx_batch,  
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_waiters: Vec::with_capacity(batch_size * 2),
                all_batch_waiters: HashMap::new(),
                current_batch_size: 0,
                network: SimpleSender::new(),
                rx_batch_commit
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.0.len();
                    self.current_batch.push(transaction.0);
                    self.current_batch_waiters.push(transaction.1);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;

                        debug!("batch ready it took {:?} ms", current_time.elapsed().as_millis());
                        current_time = Instant::now();

                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                Some(digest) = self.rx_batch_commit.recv() => {
                    info!("Batch worker got {}", digest);
                    self.reply_all(digest).await;
                }

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    debug!("BatchMaker: max batch delay timer triggered");
                    if self.current_batch.is_empty() {
                        self.current_batch.push(vec![rand::thread_rng().gen(); 128]);
                    }
                    // if !self.current_batch.is_empty() {
                        self.seal().await;
                    // }

                    current_time = Instant::now();
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    async fn reply_all(&mut self, digest: Digest) {
        if let Some(waiters) = self.all_batch_waiters.remove(&digest) {
            info!("Replying to {} waiters", waiters.len());
            for tx in waiters {
                let _ = tx.send(());
            }
        } else {
            info!("Missing batch: {}", digest);
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.

        //NEW:
        //Best-effort broadcast only. Any failure is correlated with the primary operating this node (running on same machine)
        let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        self.network.broadcast(addresses, bytes).await; 

        let digest = Digest(
            Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap()
        );

        self.tx_batch.send(serialized).await.expect("Failed to deliver batch");

        // Store all 
        let waiters = self.current_batch_waiters.drain(..).collect();
        info!("Inserting {}", digest);
        self.all_batch_waiters.insert(digest.clone(), waiters);

        // self.reply_all(digest).await;

        //OLD:
        //This uses reliable sender. The receiver worker will reply with an ack. The Reply Handler is passed to Quorum Waiter.
        // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        // let bytes = Bytes::from(serialized.clone());
        // let handlers = self.network.broadcast(addresses, bytes).await; 

        // // Send the batch through the deliver channel for further processing.
        // self.tx_message
        //     .send(QuorumWaiterMessage {
        //         batch: serialized,
        //         handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        //     })
        //     .await
        //     .expect("Failed to deliver batch");
    }
}
