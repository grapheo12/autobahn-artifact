// Copyright(C) Facebook, Inc. and its affiliates.
use crate::client::ClientParameters;
use crate::metrics::MetricsCollector;
use bytes::{BufMut as _, Bytes, BytesMut};
use config::ClientId;
use futures::sink::SinkExt as _;
use futures::StreamExt;
use log::{debug, info, warn};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Instant as StdInstant, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::time::{delay_queue::Key as DelayKey, DelayQueue};

/// Message sent to control timeout behavior
enum TimeoutControl {
    CancelTimeouts(Vec<u64>), // tx_ids - ACKs received, cancel timeouts (batched)
    ScheduleTimeout {
        tx_id: u64,
        tx_bytes: Bytes,
        sent_at: Instant,
    }, // register timeout with delay queue
}

/// Stores information about a pending transaction that may need to be retried
struct PendingTransaction {
    tx_bytes: Bytes,
    sent_at: Instant,
    delay_key: DelayKey,
}

/// Holds a retry transport and its address for logging/removal.
struct RetryConnection {
    address: SocketAddr,
    transport: Framed<TcpStream, LengthDelimitedCodec>,
}

// Allow high-rate workloads to enqueue retries without blocking immediately.
const TIMEOUT_CHANNEL_CAPACITY: usize = 1_000_000;

/// The TransactionSender component sends transactions to workers.
/// This follows the same pattern as the existing benchmark_client transaction sending logic.
pub struct TransactionSender {
    /// The unique identifier for this client.
    client_id: ClientId,
    /// List of worker addresses to send transactions to.
    worker_addresses: Vec<SocketAddr>,
    /// Configuration parameters for transaction sending.
    parameters: ClientParameters,
    /// Metrics collector used to persist latency information.
    metrics: MetricsCollector,
    /// Sender for timeout control messages (used internally to cancel timeouts)
    timeout_tx: mpsc::Sender<TimeoutControl>,
    /// Receiver for early ACKs from co-located worker
    ack_rx: mpsc::Receiver<Vec<u64>>,
}

impl TransactionSender {
    pub fn spawn(
        client_id: ClientId,
        worker_addresses: Vec<SocketAddr>,
        parameters: ClientParameters,
        metrics: MetricsCollector,
        ack_rx: mpsc::Receiver<Vec<u64>>,
    ) {
        let (timeout_tx, timeout_rx) = mpsc::channel(TIMEOUT_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut sender = Self {
                client_id,
                worker_addresses,
                parameters,
                metrics,
                timeout_tx,
                ack_rx,
            };
            sender.run(timeout_rx).await;
        });
    }

    async fn run(&mut self, timeout_rx: mpsc::Receiver<TimeoutControl>) {
        // Spawn ACK handler task if timeout is enabled
        if self.parameters.transaction_timeout > 0 {
            let (dummy_ack_tx, dummy_ack_rx) = mpsc::channel(1);
            drop(dummy_ack_tx);
            let ack_rx = std::mem::replace(&mut self.ack_rx, dummy_ack_rx);
            let timeout_tx = self.timeout_tx.clone();
            let client_id = self.client_id;

            tokio::spawn(async move {
                Self::handle_acks(ack_rx, timeout_tx, client_id).await;
            });
        } else {
            self.ack_rx.close();
        }

        if let Err(e) = self.send_transactions(timeout_rx).await {
            warn!("TransactionSender error: {}", e);
        }
    }

    /// Handle incoming early ACKs and cancel corresponding timeouts
    async fn handle_acks(
        mut ack_rx: mpsc::Receiver<Vec<u64>>,
        timeout_tx: mpsc::Sender<TimeoutControl>,
        client_id: ClientId,
    ) {
        let mut total_acks_received = 0;
        while let Some(acked_tx_ids) = ack_rx.recv().await {
            let batch_size = acked_tx_ids.len();
            total_acks_received += batch_size;
            debug!(
                "Client {} received early ACKs for {} transactions (total: {})",
                client_id, batch_size, total_acks_received
            );

            // Send batched cancel message - much more efficient than one message per ACK
            if let Err(e) = timeout_tx
                .send(TimeoutControl::CancelTimeouts(acked_tx_ids))
                .await
            {
                warn!(
                    "Client {} failed to notify timeout task about ACKs: {}",
                    client_id, e
                );
                break;
            }
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
    async fn send_transactions(
        &self,
        timeout_rx: mpsc::Receiver<TimeoutControl>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;
        const SAMPLE_LOG_INTERVAL: u64 = 1_000; // Limit high-volume sample logs.
        const SAMPLES_PER_BURST: u64 = 100; // Number of sample transactions per burst interval.

        // The transaction size must be at least 10 bytes (1 byte type + 1 byte client_id + 8 bytes counter).
        if self.parameters.transaction_size < 10 {
            return Err("Transaction size must be at least 10 bytes".into());
        }

        // Select workers to connect to for fault tolerance (worker_count workers)
        let selected_workers = self.select_workers();

        // Establish persistent connections to ALL selected workers upfront.
        // The first (co-located) worker is reserved for the main send loop.
        let mut primary_transport: Option<(SocketAddr, Framed<TcpStream, LengthDelimitedCodec>)> =
            None;
        let mut retry_connections: Vec<RetryConnection> = Vec::new();

        for (index, worker_addr) in selected_workers.iter().copied().enumerate() {
            match TcpStream::connect(worker_addr).await {
                Ok(stream) => {
                    let transport = Framed::new(stream, LengthDelimitedCodec::new());
                    if index == 0 && primary_transport.is_none() {
                        debug!(
                            "Client {} established primary (co-located) connection to worker {}",
                            self.client_id, worker_addr
                        );
                        primary_transport = Some((worker_addr, transport));
                    } else {
                        debug!(
                            "Client {} established fallback connection to worker {}",
                            self.client_id, worker_addr
                        );
                        retry_connections.push(RetryConnection {
                            address: worker_addr,
                            transport,
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        "Client {} failed to connect to worker {}: {}",
                        self.client_id, worker_addr, e
                    );
                }
            }
        }

        let (primary_addr, mut primary_transport) = if let Some((addr, transport)) =
            primary_transport
        {
            (addr, transport)
        } else if !retry_connections.is_empty() {
            let connection = retry_connections.remove(0);
            if let Some(co_located_addr) = selected_workers.first() {
                warn!(
                    "Client {} failed to connect to co-located worker {}. Using fallback worker {} as primary",
                    self.client_id,
                    co_located_addr,
                    connection.address
                );
            } else {
                warn!(
                    "Client {} had no co-located worker configured; using worker {} as primary",
                    self.client_id, connection.address
                );
            }
            (connection.address, connection.transport)
        } else {
            return Err("No workers available for connection".into());
        };

        let fallback_addrs: Vec<SocketAddr> = retry_connections
            .iter()
            .map(|connection| connection.address)
            .collect();

        debug!(
            "Client {} established persistent connections (primary: {}, fallbacks: {:?})",
            self.client_id, primary_addr, fallback_addrs
        );

        // Spawn timeout handler task to process retry events
        if self.parameters.transaction_timeout > 0 {
            let client_id = self.client_id;
            let timeout_duration = Duration::from_millis(self.parameters.transaction_timeout);
            let max_retry_workers = self.parameters.worker_count.saturating_sub(1);

            tokio::spawn(async move {
                Self::handle_timeout_retries(
                    timeout_rx,
                    retry_connections,
                    client_id,
                    timeout_duration,
                    max_retry_workers,
                )
                .await;
            });
        } else {
            drop(timeout_rx);
        }

        // Submit all transactions
        let burst = self.parameters.transaction_rate / PRECISION;
        let sample_interval = burst / SAMPLES_PER_BURST; // Sample every Nth transaction within a burst
        let mut tx = BytesMut::with_capacity(self.parameters.transaction_size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);
        let duration_limit = self.parameters.duration;
        let start = StdInstant::now();
        let mut stopped_due_to_duration = false;
        let mut stopped_due_to_error = false;

        // NOTE: This log entry is used to compute performance.
        info!("Client {} Start sending transactions", self.client_id);

        'send_loop: loop {
            if !duration_limit.is_zero() && start.elapsed() >= duration_limit {
                stopped_due_to_duration = true;
                break 'send_loop;
            }

            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                // Sample every sample_interval transactions to get SAMPLES_PER_BURST samples per burst
                if x % sample_interval == 0 {
                    /*if log::log_enabled!(log::Level::Info)
                        && (counter < 10 || counter % SAMPLE_LOG_INTERVAL == 0)
                    {
                        info!(
                            "Client {} sending sample transaction {} from client {}",
                            self.client_id, counter, self.client_id
                        );
                    }*/

                    // Use unique ID: counter * burst + x to ensure each sample has a unique ID
                    let sample_id = counter * burst + x;

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u8(self.client_id); // Client ID for uniqueness.
                    tx.put_u64(sample_id); // This counter identifies the tx.

                    let send_instant = StdInstant::now();
                    let send_time = SystemTime::now();
                    self.metrics
                        .record_send(sample_id, send_instant, send_time, true)
                        .await;
                } else {
                    r += 1;

                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u8(self.client_id); // Client ID for uniqueness.
                    tx.put_u64(r); // Ensures all clients send different txs.

                    let send_instant = StdInstant::now();
                    let send_time = SystemTime::now();
                    self.metrics
                        .record_send(r, send_instant, send_time, false)
                        .await;
                }

                // tx.resize(self.parameters.transaction_size, 0u8); // Pad to target size
                if tx.len() < self.parameters.transaction_size {
                    let remaining_size = self.parameters.transaction_size - tx.len();
                    let mut random_bytes = vec![0u8; remaining_size];
                    rand::thread_rng().fill(&mut random_bytes[..]);
                    tx.extend_from_slice(&random_bytes);
                }
                let bytes = tx.split().freeze(); // Move content and make immutable for sharing

                // Optimistic send: Send only to the first worker (co-located)
                // If timeout occurs, we'll retry to the remaining workers
                let send_result = primary_transport.send(bytes.clone()).await;

                let successful_send = send_result.is_ok();
                if !successful_send {
                    warn!(
                        "Client {} FAILED to send to primary worker at burst {}",
                        self.client_id, counter
                    );
                }

                // Register timeout if timeout is enabled and we successfully sent
                let current_tx_id = if x % sample_interval == 0 {
                    counter * burst + x
                } else {
                    r
                };
                if self.parameters.transaction_timeout > 0 && successful_send {
                    // Enqueue timeout event handled centrally by delay queue worker.
                    if let Err(e) = self
                        .timeout_tx
                        .send(TimeoutControl::ScheduleTimeout {
                            tx_id: current_tx_id,
                            tx_bytes: bytes.clone(),
                            sent_at: Instant::now(),
                        })
                        .await
                    {
                        warn!(
                            "Client {} failed to schedule timeout for tx {}: {}",
                            self.client_id, current_tx_id, e
                        );
                        stopped_due_to_error = true;
                        break 'send_loop;
                    }
                }

                // Check for fatal send errors
                if let Err(e) = send_result {
                    warn!(
                        "Client {} failed to send transaction: {}",
                        self.client_id, e
                    );
                    debug!(
                        "Client {} stopping after counter = {}, r = {}",
                        self.client_id, counter, r
                    );
                    stopped_due_to_error = true;
                    break 'send_loop;
                }
            }

            let elapsed_ms = now.elapsed().as_millis();
            if elapsed_ms > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Client {} transaction rate too high for this client (took {}ms, expected {}ms)", 
                      self.client_id, elapsed_ms, BURST_DURATION);
            }

            // Debug print every 1000 transactions to monitor progress
            // Log progress every 100 bursts to monitor progress
            if counter % 100 == 0 {
                debug!(
                    "Client {} sent {} bursts ({} txs total), current r = {}",
                    self.client_id,
                    counter,
                    counter * burst,
                    r
                );
            }

            counter += 1;
        }

        if stopped_due_to_duration {
            info!(
                "Client {} completed configured duration of {:?} after {} bursts",
                self.client_id, duration_limit, counter
            );
        } else if stopped_due_to_error {
            warn!(
                "Client {} exiting transaction sender early after {} bursts due to send failures",
                self.client_id, counter
            );
        } else {
            info!(
                "Client {} transaction sender exiting after {} bursts",
                self.client_id, counter
            );
        }

        Ok(())
    }

    /// Handle timeout events and retry transactions using persistent connections
    async fn handle_timeout_retries(
        mut timeout_rx: mpsc::Receiver<TimeoutControl>,
        mut transports: Vec<RetryConnection>,
        client_id: ClientId,
        timeout_duration: Duration,
        max_retry_workers: usize,
    ) {
        let mut total_cancelled = 0;
        let mut total_retried = 0;
        let mut delay_queue = DelayQueue::new();
        let mut pending: HashMap<u64, PendingTransaction> = HashMap::new();
        let mut cancelled_before_insert: HashSet<u64> = HashSet::new();
        let mut rx_closed = false;
        let mut retries_since_last_log = 0usize;

        loop {
            tokio::select! {
                maybe_msg = timeout_rx.recv(), if !rx_closed => {
                    match maybe_msg {
                        Some(TimeoutControl::CancelTimeouts(tx_ids)) => {
                            let batch_size = tx_ids.len();
                            let debug_enabled = log::log_enabled!(log::Level::Debug);
                            let mut cancelled_in_batch = 0usize;
                            let mut total_latency_ms = 0u64;
                            let mut min_latency_ms = u64::MAX;
                            let mut max_latency_ms = 0u64;

                            for tx_id in tx_ids {
                                if let Some(pending_tx) = pending.remove(&tx_id) {
                                    let _ = delay_queue.remove(&pending_tx.delay_key);
                                    cancelled_in_batch += 1;

                                    if debug_enabled {
                                        let latency_ms = pending_tx.sent_at.elapsed().as_millis() as u64;
                                        total_latency_ms += latency_ms;
                                        min_latency_ms = min_latency_ms.min(latency_ms);
                                        max_latency_ms = max_latency_ms.max(latency_ms);
                                    }
                                } else {
                                    cancelled_before_insert.insert(tx_id);
                                }
                            }

                            total_cancelled += cancelled_in_batch;

                            if debug_enabled && cancelled_in_batch > 0 {
                                let avg_latency_ms = total_latency_ms / cancelled_in_batch as u64;
                                let min_latency_ms = if min_latency_ms == u64::MAX {
                                    avg_latency_ms
                                } else {
                                    min_latency_ms
                                };

                                debug!(
                                    "Client {} cancelled {} timeouts (batch size {}) - avg: {}ms, min: {}ms, max: {}ms (total cancelled: {})",
                                    client_id,
                                    cancelled_in_batch,
                                    batch_size,
                                    avg_latency_ms,
                                    min_latency_ms,
                                    max_latency_ms,
                                    total_cancelled
                                );
                            }
                        }
                        Some(TimeoutControl::ScheduleTimeout { tx_id, tx_bytes, sent_at }) => {
                            if cancelled_before_insert.remove(&tx_id) {
                                continue;
                            }

                            if pending.contains_key(&tx_id) {
                                debug!("Client {} received duplicate schedule for tx {}", client_id, tx_id);
                                continue;
                            }

                            let key = delay_queue.insert(tx_id, timeout_duration);
                            pending.insert(
                                tx_id,
                                PendingTransaction {
                                    tx_bytes,
                                    sent_at,
                                    delay_key: key,
                                },
                            );
                        }
                        None => rx_closed = true,
                    }
                }
                maybe_expired = delay_queue.next(), if !delay_queue.is_empty() => {
                    match maybe_expired {
                        Some(Ok(expired)) => {
                            let tx_id = expired.into_inner();
                            if let Some(pending_tx) = pending.remove(&tx_id) {
                                let num_retry_workers = transports.len().min(max_retry_workers);

                                if num_retry_workers == 0 {
                                    debug!(
                                        "Client {} timeout fired for tx {} but no retry transports are available",
                                        client_id,
                                        tx_id
                                    );
                                    continue;
                                }

                                total_retried += 1;
                                retries_since_last_log += 1;

                                let debug_enabled = log::log_enabled!(log::Level::Debug);
                                let mut failed_indexes = Vec::new();
                                let mut successful_workers = 0usize;

                                if num_retry_workers == 1 && transports.len() > 1 {
                                    let index = {
                                        let mut rng = rand::thread_rng();
                                        rng.gen_range(0, transports.len())
                                    };

                                    if let Some(connection) = transports.get_mut(index) {
                                        match connection
                                            .transport
                                            .send(pending_tx.tx_bytes.clone())
                                            .await
                                        {
                                            Ok(()) => successful_workers += 1,
                                            Err(e) => {
                                                warn!(
                                                    "Client {} failed to send retry for tx {} to worker {}: {}",
                                                    client_id,
                                                    tx_id,
                                                    connection.address,
                                                    e
                                                );
                                                failed_indexes.push(index);
                                            }
                                        }
                                    }
                                } else {
                                    for (index, connection) in transports
                                        .iter_mut()
                                        .enumerate()
                                        .take(num_retry_workers)
                                    {
                                        match connection
                                            .transport
                                            .send(pending_tx.tx_bytes.clone())
                                            .await
                                        {
                                            Ok(()) => successful_workers += 1,
                                            Err(e) => {
                                                warn!(
                                                    "Client {} failed to send retry for tx {} to worker {}: {}",
                                                    client_id,
                                                    tx_id,
                                                    connection.address,
                                                    e
                                                );
                                                failed_indexes.push(index);
                                            }
                                        }
                                    }
                                }

                                failed_indexes.sort_unstable();
                                for index in failed_indexes.into_iter().rev() {
                                    if index < transports.len() {
                                        let failed = transports.remove(index);
                                        warn!(
                                            "Client {} removed retry transport to worker {} after send failure",
                                            client_id,
                                            failed.address
                                        );
                                    }
                                }

                                // Log stats every 100 retries
                                if debug_enabled && retries_since_last_log >= 100 {
                                    debug!(
                                        "Client {} TIMEOUT batch: {} transactions retried across up to {} workers (successful workers this batch: {}, total retried: {})",
                                        client_id,
                                        retries_since_last_log,
                                        num_retry_workers,
                                        successful_workers,
                                        total_retried
                                    );
                                    retries_since_last_log = 0;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(
                                "Client {} delay queue error while waiting for timeout: {})",
                                client_id,
                                e
                            );
                        }
                        None => {}
                    }
                }
            }

            if rx_closed && delay_queue.is_empty() {
                break;
            }
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

        let (timeout_tx, _timeout_rx) = mpsc::channel(1);
        let (_ack_tx, ack_rx) = mpsc::channel(1);

        let sender = TransactionSender {
            client_id: 0,
            worker_addresses: worker_addresses.clone(),
            parameters,
            metrics: MetricsCollector::noop(0),
            timeout_tx,
            ack_rx,
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
            let (timeout_tx, _timeout_rx) = mpsc::channel(1);
            let (_ack_tx, ack_rx) = mpsc::channel(1);

            let sender = TransactionSender {
                client_id,
                worker_addresses: worker_addresses.clone(),
                parameters: parameters.clone(),
                metrics: MetricsCollector::noop(client_id),
                timeout_tx,
                ack_rx,
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

        let (timeout_tx, _timeout_rx) = mpsc::channel(1);
        let (_ack_tx, ack_rx) = mpsc::channel(1);

        let sender = TransactionSender {
            client_id: 0,
            worker_addresses: worker_addresses.clone(),
            parameters,
            metrics: MetricsCollector::noop(0),
            timeout_tx,
            ack_rx,
        };

        let selected = sender.select_workers();
        assert_eq!(selected.len(), 2); // Should get all available workers
        assert_eq!(selected, worker_addresses);
    }
}
