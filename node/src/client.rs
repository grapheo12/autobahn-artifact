// Copyright(C) Facebook, Inc. and its affiliates.
use crate::reply_processor::ReplyProcessor;
use crate::transaction_sender::TransactionSender;
use async_trait::async_trait;
use bytes::Bytes;
use config::{ClientId, Committee, Parameters};
use crypto::PublicKey;
use futures::sink::SinkExt as _;
use log::{debug, error, info, warn};
use network::{MessageHandler, Receiver, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{channel, Receiver as OtherReceiver, Sender};

/// The default channel capacity for each channel of the client.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Transaction reply message received from workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTransactionReply {
    pub slot: u64,
    pub committed_transactions: Vec<u64>,
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
}

impl Default for ClientParameters {
    fn default() -> Self {
        Self {
            transaction_size: 512,
            transaction_rate: 1000,
            worker_count: 1,
            threshold: 1,
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
}

impl Client {
    pub fn spawn(
        client_id: ClientId,
        committee: Committee,
        parameters: ClientParameters,
        reply_address: SocketAddr,
    ) {
        // Define a client instance.
        let client = Self {
            client_id,
            committee,
            parameters,
            reply_address,
        };

        // NOTE: These log entries are used to compute performance.
        info!("Transactions size: {} B", client.parameters.transaction_size);
        info!("Transactions rate: {} tx/s", client.parameters.transaction_rate);

        // Add this client to the committee configuration if not already present
        // This allows workers to find this client's reply address
        // In production, this would be done through proper service registration
        
        // Spawn client tasks
        client.handle_transaction_sending();
        client.handle_reply_receiving();

        // NOTE: This log entry is used to compute performance.
        info!(
            "Client {} successfully started, listening for replies on {}",
            client_id, reply_address
        );
    }

    /// Spawn the transaction sending component.
    fn handle_transaction_sending(&self) {
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
        );
    }

    /// Spawn the reply receiving and processing components.
    fn handle_reply_receiving(&self) {
        let (tx_reply_processor, rx_reply_processor) = channel(CHANNEL_CAPACITY);

        // Listen for replies from workers
        let mut address = self.reply_address;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            ReplyReceiverHandler { tx_reply_processor },
        );

        // Process received replies
        ReplyProcessor::spawn(
            self.client_id,
            rx_reply_processor,
            self.parameters.threshold,
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
                
                // Send to reply processor
                self.tx_reply_processor
                    .send(reply)
                    .await
                    .expect("Failed to send reply to processor");
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
    }

    #[tokio::test]
    async fn test_client_creation() {
        let committee = Committee::new(vec![]);
        let parameters = ClientParameters::default();
        let reply_address = "127.0.0.1:8000".parse().unwrap();
        
        // This should not panic
        Client::spawn(0, committee, parameters, reply_address);
    }
}
