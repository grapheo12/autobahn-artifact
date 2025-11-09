// Copyright(C) Facebook, Inc. and its affiliates.
use crate::metrics::MetricsCollector;
use crate::reply_processor::ReplyProcessor;
use crate::transaction_sender::TransactionSender;
use async_trait::async_trait;
use bytes::Bytes;
use config::{ClientId, Committee};
use crypto::PublicKey;
use log::{info, warn};
use network::{MessageHandler, Receiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Sender};

/// Reply channel capacity sized for >200k tx/s workloads.
pub const CHANNEL_CAPACITY: usize = 1_000_000;
const ACK_CHANNEL_CAPACITY: usize = 1_000_000;

/// Transaction reply message received from workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTransactionReply {
    pub slot: u64,
    pub worker_key: PublicKey,
    pub committed_transactions: Vec<u64>,
}

/// Early ACK message received from co-located worker when certificate forms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateAck {
    pub acked_transactions: Vec<u64>,
}

/// Client configuration parameters.
#[derive(Clone)]
pub struct ClientParameters {
    /// The size of each transaction in bytes.
    pub transaction_size: usize,
    /// The rate (txs/s) at which to send transactions.
    pub transaction_rate: u64,
    /// The number of workers to send transactions to.
    pub worker_count: usize,
    /// The number of reply confirmations required before marking a transaction as committed.
    pub threshold: usize,
    /// How long the client should send transactions before initiating shutdown.
    pub duration: Duration,
    /// Timeout duration in milliseconds for waiting for early ACKs before retrying to remaining workers.
    /// If 0, timeout mechanism is disabled.
    pub transaction_timeout: u64,
}

impl Default for ClientParameters {
    fn default() -> Self {
        Self {
            transaction_size: 512,
            transaction_rate: 1000,
            worker_count: 1,
            threshold: 1,
            duration: Duration::from_secs(20),
            transaction_timeout: 150, // 150ms default timeout
        }
    }
}

/// The Client process that sends transactions and receives replies.
pub struct Client {
    /// The unique identifier for this client (0-255).
    client_id: ClientId,
    /// The committee information for discovering worker and client addresses.
    committee: Committee,
    /// The client configuration parameters.
    parameters: ClientParameters,
    /// The address where this client listens for replies.
    reply_address: SocketAddr,
    /// The address where this client listens for early ACKs.
    ack_address: SocketAddr,
    /// Metrics collector responsible for persisting latency information.
    metrics: MetricsCollector,
}

impl Client {
    pub fn spawn(
        client_id: ClientId,
        committee: Committee,
        parameters: ClientParameters,
        reply_address: SocketAddr,
        ack_address: SocketAddr,
        metrics_path: PathBuf,
    ) -> MetricsCollector {
        let metrics = match MetricsCollector::spawn(client_id, metrics_path) {
            Ok(collector) => collector,
            Err(e) => {
                warn!(
                    "Failed to initialise metrics collector for client {}: {}. Falling back to no-op collector.",
                    client_id, e
                );
                MetricsCollector::noop(client_id)
            }
        };

        // Define a client instance.
        let client = Self {
            client_id,
            committee,
            parameters,
            reply_address,
            ack_address,
            metrics,
        };

        // NOTE: These log entries are used to compute performance.
        info!(
            "Transactions size: {} B",
            client.parameters.transaction_size
        );
        info!(
            "Transactions rate: {} tx/s",
            client.parameters.transaction_rate
        );

        // Add this client to the committee configuration if not already present
        // This allows workers to find this client's reply address
        // In production, this would be done through proper service registration

        // Spawn client tasks
        let ack_tx = client.handle_ack_receiving();
        client.handle_transaction_sending(ack_tx);
        client.handle_reply_receiving();

        // NOTE: This log entry is used to compute performance.
        info!(
            "Client {} successfully started, listening for replies on {}",
            client_id, reply_address
        );

        client.metrics.clone()
    }

    /// Spawn the ACK receiving component and return the receiver for ACKs.
    fn handle_ack_receiving(&self) -> tokio::sync::mpsc::Receiver<Vec<u64>> {
        let (tx_ack, rx_ack) = tokio::sync::mpsc::channel(ACK_CHANNEL_CAPACITY);

        // Listen for early ACKs from co-located worker
        let mut address = self.ack_address;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(address, AckReceiverHandler { tx_ack });

        info!(
            "Client {} listening for early ACKs on {}",
            self.client_id, address
        );

        rx_ack
    }

    /// Spawn the transaction sending component.
    fn handle_transaction_sending(&self, ack_rx: tokio::sync::mpsc::Receiver<Vec<u64>>) {
        // Starting transaction sender

        // Get worker addresses from committee in ID order for same-ID mapping
        let mut authorities_by_id: Vec<_> = self.committee.authorities.values().collect();
        authorities_by_id.sort_by_key(|authority| authority.id);

        let worker_addresses: Vec<SocketAddr> = authorities_by_id
            .iter()
            .flat_map(|authority| authority.workers.values())
            .map(|worker| worker.transactions)
            .collect();

        TransactionSender::spawn(
            self.client_id,
            worker_addresses,
            self.parameters.clone(),
            self.metrics.clone(),
            ack_rx,
        );
    }

    /// Spawn the reply receiving and processing components.
    fn handle_reply_receiving(&self) {
        let (tx_reply_processor, rx_reply_processor) = channel(CHANNEL_CAPACITY);

        // Listen for replies from workers
        let mut address = self.reply_address;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(address, ReplyReceiverHandler { tx_reply_processor });

        // Process received replies
        ReplyProcessor::spawn(
            self.client_id,
            rx_reply_processor,
            self.parameters.threshold,
            self.metrics.clone(),
        );

        // Listening for replies
    }
}

/// Handles incoming reply messages from workers.
#[derive(Clone)]
struct ReplyReceiverHandler {
    tx_reply_processor: Sender<SlotTransactionReply>,
}

#[async_trait]
impl MessageHandler for ReplyReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize the reply message
        match bincode::deserialize::<SlotTransactionReply>(&message) {
            Ok(reply) => {
                // Received reply for slot
                // Log every reply received
                let capacity = self.tx_reply_processor.capacity();
                info!(
                    "RX: Client got slot {} with {} txs (ch cap: {})",
                    reply.slot,
                    reply.committed_transactions.len(),
                    capacity
                );

                // Check if channel is full
                if capacity == 0 {
                    warn!("BOTTLENECK: Reply channel FULL!");
                }

                // Send to reply processor - don't panic on error
                if let Err(_) = self.tx_reply_processor.send(reply).await {
                    warn!("DROPPED: Failed to send reply to processor!");
                }

                // Send to reply processor
                /*self.tx_reply_processor
                .send(reply)
                .await
                .expect("Failed to send reply to processor");*/
            }
            Err(e) => {
                warn!("Failed to deserialize reply message: {}", e);
            }
        }

        // Give the chance to schedule other tasks
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Handles incoming early ACK messages from the co-located worker.
#[derive(Clone)]
struct AckReceiverHandler {
    tx_ack: tokio::sync::mpsc::Sender<Vec<u64>>,
}

#[async_trait]
impl MessageHandler for AckReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize the ACK message
        match bincode::deserialize::<CertificateAck>(&message) {
            Ok(ack) => {
                // Received early ACK - forward to TransactionSender
                if let Err(_) = self.tx_ack.send(ack.acked_transactions).await {
                    warn!("Failed to send ACK to TransactionSender");
                }
            }
            Err(e) => {
                warn!("Failed to deserialize ACK message: {}", e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_parameters_default() {
        let params = ClientParameters::default();
        assert_eq!(params.transaction_size, 512);
        assert_eq!(params.transaction_rate, 1000);
        assert_eq!(params.worker_count, 1);
        assert_eq!(params.threshold, 1);
        assert_eq!(params.duration, Duration::from_secs(20));
    }

    #[tokio::test]
    async fn test_client_creation() {
        let committee = Committee::new(vec![]);
        let parameters = ClientParameters::default();
        let reply_address = "127.0.0.1:8000".parse().unwrap();
        let ack_address = "127.0.0.1:8001".parse().unwrap();

        // This should not panic
        let metrics_path = PathBuf::from("client-test.metrics");
        let metrics = Client::spawn(
            0,
            committee,
            parameters,
            reply_address,
            ack_address,
            metrics_path,
        );
        metrics.shutdown().await;
    }
}
