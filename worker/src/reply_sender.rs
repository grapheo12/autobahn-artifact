// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{BatchTransactionMap, SlotTransactionReply};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::Digest;
use crypto::PublicKey;
use futures::sink::SinkExt;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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
}

impl ReplySender {
    pub fn spawn(
        rx_slot_committed: Receiver<(u64, HashSet<Digest>)>,
        batch_to_transactions: BatchTransactionMap,
        committee: Committee,
        worker_id: WorkerId,
        worker_key: PublicKey,
    ) {
        tokio::spawn(async move {
            Self {
                rx_slot_committed,
                batch_to_transactions,
                committee,
                worker_id,
                worker_key,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        info!(
            "ReplySender started with {} clients in committee",
            self.committee.clients.len()
        );

        // Initialize empty connection map - connections will be established lazily
        let mut client_transports = HashMap::new();

        while let Some((slot, batch_digests)) = self.rx_slot_committed.recv().await {
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

            // Send batched replies to each client using persistent connections
            for (client_id, counters) in client_transactions {
                let reply = SlotTransactionReply {
                    slot,
                    worker_key: self.worker_key,
                    committed_transactions: counters,
                };

                // Check if we need to establish a connection for this client
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
                                continue; // Skip this client for now
                            }
                        }
                    } else {
                        debug!("Client {} not found in committee", client_id);
                        continue;
                    }
                }

                // Send using existing or newly created connection
                if let Some(transport) = client_transports.get_mut(&client_id) {
                    if let Err(e) = self.send_reply_to_client(client_id, reply, transport).await {
                        debug!("Failed to send reply to client {}: {}", client_id, e);
                        // Don't remove connection - let it fail naturally like TransactionSender
                    }
                }
            }

            if !processed_digests.is_empty() {
                let mut map = self.batch_to_transactions.lock().unwrap();
                for digest in processed_digests {
                    map.remove(&digest);
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
        );
    }
}