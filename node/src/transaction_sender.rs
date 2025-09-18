// Copyright(C) Facebook, Inc. and its affiliates.
use crate::client::ClientParameters;
use bytes::{BufMut as _, BytesMut};
use config::ClientId;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use rand::Rng;
use rand::seq::SliceRandom;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// The TransactionSender component sends transactions to workers.
/// This follows the same pattern as the existing benchmark_client transaction sending logic.
pub struct TransactionSender {
    /// The unique identifier for this client.
    client_id: ClientId,
    /// List of worker addresses to send transactions to.
    worker_addresses: Vec<SocketAddr>,
    /// Configuration parameters for transaction sending.
    parameters: ClientParameters,
}

impl TransactionSender {
    pub fn spawn(
        client_id: ClientId,
        worker_addresses: Vec<SocketAddr>,
        parameters: ClientParameters,
    ) {
        tokio::spawn(async move {
            Self {
                client_id,
                worker_addresses,
                parameters,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        if let Err(e) = self.send_transactions().await {
            warn!("TransactionSender error: {}", e);
        }
    }

    /// Select workers to send transactions to, based on the worker_count parameter.
    /// Uses same-ID mapping for primary worker, then round-robin for additional workers.
    fn select_workers(&self) -> Vec<SocketAddr> {
        let mut selected_workers = Vec::new();
        
        if self.worker_addresses.is_empty() {
            warn!("No worker addresses available");
            return selected_workers;
        }

        // If we want to send to all workers or more workers than available
        if self.parameters.worker_count >= self.worker_addresses.len() {
            return self.worker_addresses.clone();
        }

        // Use same-ID mapping for the first worker (client 0 -> worker 0, etc.)
        let total_workers = self.worker_addresses.len();
        let first_worker_index = (self.client_id as usize) % total_workers;
        selected_workers.push(self.worker_addresses[first_worker_index]);
        
        // For additional workers, use round-robin starting from the next worker
        for i in 1..self.parameters.worker_count {
            let worker_index = (first_worker_index + i) % total_workers;
            selected_workers.push(self.worker_addresses[worker_index]);
        }

        selected_workers
    }

    /// Send transactions to selected workers.
    /// This is adapted from the existing benchmark_client.send() method.
    async fn send_transactions(&self) -> Result<(), Box<dyn std::error::Error>> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 10 bytes (1 byte type + 1 byte client_id + 8 bytes counter).
        if self.parameters.transaction_size < 10 {
            return Err("Transaction size must be at least 10 bytes".into());
        }

        // Select workers to send to
        let selected_workers = self.select_workers();

        // Connect to all selected workers
        let mut transports = Vec::new();
        for worker_addr in &selected_workers {
            /*let stream = TcpStream::connect(worker_addr)
                                    .await
                                    .context(format!("failed to connect to {}", worker_addr))?;
            let transport = Framed::new(stream, LengthDelimitedCodec::new());
            transports.push(transport);
            info!("Client {} connected to worker at {}", self.client_id, worker_addr);*/
        
            match TcpStream::connect(worker_addr).await {
                Ok(stream) => {
                    let transport = Framed::new(stream, LengthDelimitedCodec::new());
                    transports.push(transport);
                }
                Err(e) => {
                    warn!("Client {} failed to connect to worker {}", self.client_id, worker_addr);
                }
            }
        }

        if transports.is_empty() {
            return Err("No workers available for connection".into());
        }

        // Submit all transactions
        let burst = self.parameters.transaction_rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.parameters.transaction_size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Client {} Start sending transactions", self.client_id);

        loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Client {} sending sample transaction {} from client {}", self.client_id, counter, self.client_id);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u8(self.client_id); // Client ID for uniqueness.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    // NOTE: This log entry is used to compute performance.
                    info!("Client {} sending regular transaction {} from client {}", self.client_id, r, self.client_id);
                    
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u8(self.client_id); // Client ID for uniqueness.
                    tx.put_u64(r); // Ensures all clients send different txs.
                }

                tx.resize(self.parameters.transaction_size, 0u8); // Pad to target size
                let bytes = tx.split().freeze(); // Move content and make immutable for sharing
                
                // Send to all connected workers concurrently
                let send_futures = transports.iter_mut().enumerate().map(|(i, transport)| {
                    let bytes_clone = bytes.clone();
                    let client_id = self.client_id;
                    async move {
                        let result = transport.send(bytes_clone).await;
                        if let Err(ref e) = result {
                            debug!("Client {} failed to send to worker {}: {}", 
                                   client_id, i, e);
                        }
                        (i, result)
                    }
                });

                let results: Vec<(usize, Result<(), _>)> = join_all(send_futures).await;
                let mut failed_workers = Vec::new();
                let mut send_error = None;
                
                for (worker_idx, result) in results {
                    if let Err(e) = result {
                        failed_workers.push(worker_idx);
                        if send_error.is_none() {
                            send_error = Some(e);
                        }
                    }
                }
                
                if let Some(e) = send_error {
                    warn!("Client {} failed to send transaction to workers {:?}: {}", 
                          self.client_id, failed_workers, e);
                    debug!("Client {} stopping after counter = {}, r = {}", self.client_id, counter, r);
                    break;
                }
                
            }

            let elapsed_ms = now.elapsed().as_millis();
            if elapsed_ms > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Client {} transaction rate too high for this client (took {}ms, expected {}ms)", 
                      self.client_id, elapsed_ms, BURST_DURATION);
            }
            
            // Debug print every 1000 transactions to monitor progress
            if counter % 1000 == 0 {
                debug!("Client {} sent {} batches ({} transactions), current r = {}", 
                       self.client_id, counter, counter * burst, r);
            }

            counter += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_workers() {
        let worker_addresses = vec![
            "127.0.0.1:2000".parse().unwrap(),
            "127.0.0.1:2001".parse().unwrap(),
            "127.0.0.1:2002".parse().unwrap(),
        ];
        
        let mut parameters = ClientParameters::default();
        parameters.worker_count = 2;
        
        let sender = TransactionSender {
            client_id: 0,
            worker_addresses: worker_addresses.clone(),
            parameters,
        };

        let selected = sender.select_workers();
        assert_eq!(selected.len(), 2);
        // Client 0 should select workers starting from index 0
        assert_eq!(selected[0], worker_addresses[0]);
        assert_eq!(selected[1], worker_addresses[1]);
    }

    #[test]
    fn test_deterministic_worker_distribution() {
        let worker_addresses = vec![
            "127.0.0.1:2000".parse().unwrap(),
            "127.0.0.1:2001".parse().unwrap(),
            "127.0.0.1:2002".parse().unwrap(),
            "127.0.0.1:2003".parse().unwrap(),
        ];
        
        let mut parameters = ClientParameters::default();
        parameters.worker_count = 1;
        
        // Test that different clients get different workers
        for client_id in 0..4 {
            let sender = TransactionSender {
                client_id,
                worker_addresses: worker_addresses.clone(),
                parameters: parameters.clone(),
            };
            
            let selected = sender.select_workers();
            assert_eq!(selected.len(), 1);
            // Each client should get a different worker
            assert_eq!(selected[0], worker_addresses[client_id as usize]);
        }
    }

    #[test]
    fn test_select_all_workers() {
        let worker_addresses = vec![
            "127.0.0.1:2000".parse().unwrap(),
            "127.0.0.1:2001".parse().unwrap(),
        ];
        
        let mut parameters = ClientParameters::default();
        parameters.worker_count = 5; // More than available
        
        let sender = TransactionSender {
            client_id: 0,
            worker_addresses: worker_addresses.clone(),
            parameters,
        };

        let selected = sender.select_workers();
        assert_eq!(selected.len(), 2); // Should get all available workers
        assert_eq!(selected, worker_addresses);
    }
}
