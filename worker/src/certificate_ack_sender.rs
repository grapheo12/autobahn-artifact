// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::BatchTransactionMap;
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Message sent from worker to client when certificate forms (early ACK).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateAck {
    pub acked_transactions: Vec<u64>,
}

/// The CertificateAckSender component receives CertificateFormed notifications from the primary
/// and sends early ACKs to the co-located client for transactions included in the certificate.
pub struct CertificateAckSender {
    /// The public key of this authority (primary)
    name: PublicKey,
    /// Worker ID of this worker
    worker_id: WorkerId,
    /// Total number of clients in the system
    total_clients: usize,
    /// Receiver for certificate formed messages containing batch_digests.
    rx_cert_formed: Receiver<HashSet<Digest>>,
    /// Shared map from batch digest to transaction metadata.
    batch_to_transactions: BatchTransactionMap,
    /// Committee configuration for looking up client addresses.
    committee: Committee,
}

impl CertificateAckSender {
    pub fn spawn(
        name: PublicKey,
        worker_id: WorkerId,
        total_clients: usize,
        rx_cert_formed: Receiver<HashSet<Digest>>,
        batch_to_transactions: BatchTransactionMap,
        committee: Committee,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                worker_id,
                total_clients,
                rx_cert_formed,
                batch_to_transactions,
                committee,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // Get this authority's ID from the committee (matches client ID for co-location)
        let authority_id = self
            .committee
            .authorities
            .get(&self.name)
            .expect("This authority not found in committee")
            .id;

        // Determine co-located client ID: authority ID matches client ID
        // Client 0 sends to worker at authority 0, client 1 to authority 1, etc.
        let co_located_client_id = authority_id;

        info!(
            "CertificateAckSender for authority {} worker {} started with {} clients, co-located with client {}",
            authority_id,
            self.worker_id,
            self.total_clients,
            co_located_client_id
        );

        // Initialize connection to co-located client (lazy connection)
        let mut client_transport: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;

        while let Some(batch_digests) = self.rx_cert_formed.recv().await {
            debug!(
                "CertificateAckSender: worker {} received {} batch digests",
                self.worker_id,
                batch_digests.len()
            );

            // Collect transactions for co-located client only
            let mut co_located_transactions: Vec<u64> = Vec::new();

            // Extract all relevant transaction entries with a single lock acquisition per certificate
            let mut extracted_entries: Vec<Arc<Vec<(u8, u64)>>> = Vec::new();
            {
                let map = self.batch_to_transactions.lock().unwrap();
                for digest in &batch_digests {
                    match map.get(digest) {
                        Some(entries) => extracted_entries.push(entries.clone()),
                        None => {
                            warn!(
                                "CertificateAckSender: digest {:?} missing from batch_to_transactions",
                                digest
                            );
                        }
                    }
                }
            }

            for entries in extracted_entries {
                for &(client_id, counter) in entries.iter() {
                    if client_id as usize == co_located_client_id {
                        co_located_transactions.push(counter);
                    }
                }
            }

            // Send ACK to co-located client if we have transactions
            if !co_located_transactions.is_empty() {
                // Establish connection if needed
                if client_transport.is_none() {
                    if let Ok(client_info) = self.committee.client(&(co_located_client_id as u8)) {
                        match TcpStream::connect(&client_info.transaction_acks).await {
                            Ok(stream) => {
                                let transport = Framed::new(stream, LengthDelimitedCodec::new());
                                client_transport = Some(transport);
                                info!(
                                    "CertificateAckSender connected to client {} at {}",
                                    co_located_client_id, client_info.transaction_acks
                                );
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to connect to client {} at {}: {}",
                                    co_located_client_id, client_info.transaction_acks, e
                                );
                                continue;
                            }
                        }
                    } else {
                        debug!("Client {} not found in committee", co_located_client_id);
                        continue;
                    }
                }

                // Send using existing connection
                if let Some(transport) = &mut client_transport {
                    let ack = CertificateAck {
                        acked_transactions: co_located_transactions.clone(),
                    };

                    if let Err(e) = self
                        .send_ack_to_client(co_located_client_id as u8, ack, transport)
                        .await
                    {
                        debug!(
                            "Failed to send ACK to client {}: {}",
                            co_located_client_id, e
                        );
                        // Connection may be broken, clear it to retry next time
                        client_transport = None;
                    }
                }
            }
        }
    }

    async fn send_ack_to_client(
        &self,
        client_id: u8,
        ack: CertificateAck,
        transport: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!(
            "Sending ACK to client {}: {} transactions",
            client_id,
            ack.acked_transactions.len()
        );

        // Serialize the ACK
        let message = bincode::serialize(&ack)?;
        let bytes = Bytes::from(message);

        // Send the ACK using the existing transport
        transport.send(bytes).await?;

        info!(
            "Sent CertificateAck to client {} with {} transactions",
            client_id,
            ack.acked_transactions.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::ClientAddresses;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_certificate_ack_sender_creation() {
        let (_tx, rx) = mpsc::channel::<HashSet<Digest>>(10);
        let batch_to_transactions: BatchTransactionMap =
            Arc::new(Mutex::new(HashMap::new()));
        let name = PublicKey([0; 32]);

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
        CertificateAckSender::spawn(
            name,
            1,
            committee.clients.len(),
            rx,
            batch_to_transactions,
            committee,
        );
    }
}
