// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{SlotTransactionReply, WorkerMessage};
use bytes::Bytes;
use config::Committee;
use crypto::Digest;
use futures::sink::SinkExt;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// The ReplySender component receives SlotCommittedMessage notifications from the primary
/// and sends transaction replies to clients using the same connection pattern that clients
/// use to send transactions to workers.
pub struct ReplySender {
    /// Receiver for slot committed messages containing (slot, batch_digests).
    rx_slot_committed: Receiver<(u64, HashSet<Digest>)>,
    /// The persistent storage for reading batches.
    store: Store,
    // Client transaction set
    client_transactions: HashSet<Vec<u8>>,
    /// Committee configuration for looking up client addresses.
    /// This follows the same pattern as workers looking up addresses from committee.
    committee: Committee,
}

impl ReplySender {
    pub fn spawn(
        rx_slot_committed: Receiver<(u64, HashSet<Digest>)>,
        store: Store,
        committee: Committee,
    ) {
        tokio::spawn(async move {
            Self {
                rx_slot_committed,
                store,
                committee,
                client_transactions: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        info!("ReplySender started with {} clients in committee", self.committee.clients.len());
        
        // Initialize empty connection map - connections will be established lazily
        let mut client_transports = HashMap::new();
        
        while let Some((slot, batch_digests)) = self.rx_slot_committed.recv().await {
            debug!("ReplySender: received slot {} with {} batch digests", slot, batch_digests.len());
            
            // Group transactions by client_id
            let mut client_transactions: HashMap<u8, Vec<u64>> = HashMap::new();
            
            // Read batches from store and extract transaction IDs
            for digest in batch_digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        // Deserialize the WorkerMessage
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch, _)) => {
                                // Extract transaction IDs from each transaction in the batch
                                for transaction in batch {
                                    if transaction.len() >= 10 {
                                        if self.client_transactions.contains(&transaction) {
                                            continue;
                                        }
                                        
                                        let client_id = transaction[1];
                                        let counter = u64::from_be_bytes(
                                            transaction[2..10].try_into().unwrap()
                                        );
                                        
                                        client_transactions
                                            .entry(client_id)
                                            .or_insert_with(Vec::new)
                                            .push(counter);
                                        self.client_transactions.insert(transaction);
                                    }
                                }
                            }
                            Ok(_) => {
                                debug!("ReplySender: unexpected message type in batch store for digest {:?}", digest);
                            }
                            Err(e) => {
                                debug!("ReplySender: failed to deserialize batch for digest {:?}: {}", digest, e);
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("ReplySender: batch digest {:?} not found in store", digest);
                    }
                    Err(e) => {
                        debug!("ReplySender: error reading batch from store for digest {:?}: {}", digest, e);
                    }
                }
            }
            
            // Send batched replies to each client using persistent connections
            for (client_id, counters) in client_transactions {
                let reply = SlotTransactionReply {
                    slot,
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
                                info!("ReplySender connected to client {} at {} (lazy connection)", 
                                      client_id, client_info.replies);
                            }
                            Err(e) => {
                                debug!("Failed to connect to client {} at {}: {}", 
                                       client_id, client_info.replies, e);
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
        }
    }
    
    async fn send_reply_to_client(
        &self, 
        client_id: u8, 
        reply: SlotTransactionReply,
        transport: &mut Framed<TcpStream, LengthDelimitedCodec>
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("Sending reply to client {}: slot {} with {} transactions", 
               client_id, reply.slot, reply.committed_transactions.len());
        
        // Serialize the reply
        let message = bincode::serialize(&reply)?;
        let bytes = Bytes::from(message);
        
        // Send the reply using the existing transport
        transport.send(bytes).await?;
        
        info!("Sent SlotTransactionReply to client {} for slot {} with {} transactions",
              client_id, reply.slot, reply.committed_transactions.len());
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::Store;
    use tokio::sync::mpsc;
    use config::ClientAddresses;

    #[tokio::test]
    async fn test_reply_sender_creation() {
        let (tx, rx) = mpsc::channel(10);
        let store_path = ".db_test_reply_sender";
        let store = Store::new(store_path).unwrap();
        
        // Create a committee with test clients
        let mut committee = Committee::new(vec![]);
        committee.add_client(0, ClientAddresses {
            replies: "127.0.0.1:8000".parse().unwrap(),
        });
        
        // This should not panic
        ReplySender::spawn(rx, store, committee);
        
        // Cleanup
        let _ = std::fs::remove_dir_all(store_path);
    }
}

