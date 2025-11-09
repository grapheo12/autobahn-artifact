// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{BatchTransactionMap, SerializedBatchDigestMessage, WorkerMessage};
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::{debug, error, warn};
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>, //sender channel connects to PrimaryConnector
        batch_to_transactions: BatchTransactionMap,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
                debug!("Processor received batch {:?}", digest);

                if let Some(entries) = Self::extract_transaction_metadata(&batch) {
                    batch_to_transactions
                        .lock()
                        .unwrap()
                        .insert(digest.clone(), Arc::new(entries));
                }

                // Store the batch.
                store.write(digest.to_vec(), batch).await;

                //store.write(digest.to_vec(), Vec::default()).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => {
                        debug!(
                            "Processor worker {} creating OurBatch message for digest {}",
                            id, digest
                        );
                        WorkerPrimaryMessage::OurBatch(digest, id)
                    }
                    false => {
                        debug!(
                            "Processor worker {} creating OthersBatch message for digest {}",
                            id, digest
                        );
                        WorkerPrimaryMessage::OthersBatch(digest, id)
                    }
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                debug!(
                    "Processor worker {} serialized message (first 20 bytes): {:?}",
                    id,
                    &message[..message.len().min(20)]
                );

                // Send digest to PrimaryConnector
                if let Err(e) = tx_digest.send(message).await {
                    error!(
                        "Processor: Failed to send digest to PrimaryConnector: {}",
                        e
                    );
                    panic!("Processor channel to PrimaryConnector is closed");
                }
            }
        });
    }

    fn extract_transaction_metadata(batch: &[u8]) -> Option<Vec<(u8, u64)>> {
        match bincode::deserialize::<WorkerMessage>(batch) {
            Ok(WorkerMessage::Batch(transactions, _)) => {
                let entries = transactions
                    .iter()
                    .filter(|tx| tx.len() > 9 && (tx[0] == 0u8 || tx[0] == 1u8))
                    .filter_map(|tx| {
                        let client_id = tx[1];
                        let counter_bytes: [u8; 8] = tx[2..10].try_into().ok()?;
                        let counter = u64::from_be_bytes(counter_bytes);
                        Some((client_id, counter))
                    })
                    .collect();
                Some(entries)
            }
            Ok(_) => None,
            Err(e) => {
                warn!("Processor failed to deserialize batch for metadata extraction: {}", e);
                None
            }
        }
    }
}
