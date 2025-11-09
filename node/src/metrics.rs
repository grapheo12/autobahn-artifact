// Copyright(C) Facebook, Inc. and its affiliates.

use config::ClientId;
use itoa::Buffer as ItoaBuffer;
use log::{debug, warn};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Events produced by the client pipeline that are consumed by the metrics thread.
enum MetricsEvent {
    Send {
        tx_id: u64,
        send_instant: Instant,
        send_time: SystemTime,
        is_sample: bool,
    },
    Commit {
        tx_id: u64,
        commit_instant: Instant,
        commit_time: SystemTime,
    },
    Shutdown,
}

#[derive(Clone)]
struct MetricsSender {
    client_id: ClientId,
    sender: Option<Sender<MetricsEvent>>,
}

impl MetricsSender {
    fn new(client_id: ClientId, sender: Sender<MetricsEvent>) -> Self {
        Self {
            client_id,
            sender: Some(sender),
        }
    }

    fn noop(client_id: ClientId) -> Self {
        Self {
            client_id,
            sender: None,
        }
    }

    async fn send_event(&self, event: MetricsEvent) {
        if let Some(sender) = &self.sender {
            if sender.send(event).await.is_err() {
                debug!(
                    "Metrics channel closed for client {}; dropping event",
                    self.client_id
                );
            }
        }
    }
}

/// Collects send/commit events and emits latency records to disk.
#[derive(Clone)]
pub struct MetricsCollector {
    inner: MetricsSender,
}

impl MetricsCollector {
    /// Spawn a background metrics thread that writes latency records for the given client.
    pub fn spawn(client_id: ClientId, metrics_path: PathBuf) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel(METRICS_CHANNEL_CAPACITY);
        MetricsWriter::spawn_task(client_id, metrics_path, rx);

        Ok(Self {
            inner: MetricsSender::new(client_id, tx),
        })
    }

    /// Create a no-op collector when the metrics pipeline fails to initialise.
    pub fn noop(client_id: ClientId) -> Self {
        Self {
            inner: MetricsSender::noop(client_id),
        }
    }

    /// Record that a transaction was sent.
    pub async fn record_send(
        &self,
        tx_id: u64,
        send_instant: Instant,
        send_time: SystemTime,
        is_sample: bool,
    ) {
        self.inner
            .send_event(MetricsEvent::Send {
                tx_id,
                send_instant,
                send_time,
                is_sample,
            })
            .await;
    }

    /// Record that a transaction was committed.
    pub async fn record_commit(
        &self,
        tx_id: u64,
        commit_instant: Instant,
        commit_time: SystemTime,
    ) {
        self.inner
            .send_event(MetricsEvent::Commit {
                tx_id,
                commit_instant,
                commit_time,
            })
            .await;
    }

    /// Trigger a flush of all buffered metrics and stop the writer thread.
    pub async fn shutdown(&self) {
        self.inner.send_event(MetricsEvent::Shutdown).await;
    }
}

/// State for transactions that have been sent but not yet committed.
struct InFlightTransaction {
    send_instant: Instant,
    send_time: SystemTime,
    is_sample: bool,
}

const METRICS_BUFFER_BYTES: usize = 2 * 1024 * 1024;
// Room for ~5 seconds of backlog at 200k tx/s
const METRICS_CHANNEL_CAPACITY: usize = 3_000_000;
const METRIC_LINE_CAPACITY: usize = 128;

struct MetricsWriter {
    client_id: ClientId,
    output_path: PathBuf,
    rx: Receiver<MetricsEvent>,
    writer: Option<BufWriter<File>>,
    records_written: u64,
    line_buffer: Vec<u8>,
    itoa_buffer: ItoaBuffer,
}

impl MetricsWriter {
    fn spawn_task(client_id: ClientId, output_path: PathBuf, rx: Receiver<MetricsEvent>) {
        let writer = MetricsWriter {
            client_id,
            output_path,
            rx,
            writer: None,
            records_written: 0,
            line_buffer: Vec::with_capacity(METRIC_LINE_CAPACITY),
            itoa_buffer: ItoaBuffer::new(),
        };

        tokio::spawn(async move {
            if let Err(e) = writer.run().await {
                warn!(
                    "Metrics writer for client {} exited with error: {}",
                    client_id, e
                );
            }
        });
    }

    fn ensure_writer(&mut self) -> std::io::Result<&mut BufWriter<File>> {
        if self.writer.is_none() {
            warn!(
                "METRICS CREATE DIR: Client {} about to create parent directory for {:?}",
                self.client_id, self.output_path
            );
            if let Some(parent) = self.output_path.parent() {
                fs::create_dir_all(parent)?;
            }
            warn!(
                "METRICS CREATE FILE: Client {} about to create file {:?}",
                self.client_id, self.output_path
            );
            let file = File::create(&self.output_path)?;
            let mut writer = BufWriter::with_capacity(METRICS_BUFFER_BYTES, file);
            writer.write_all(b"client_id,tx_id,is_sample,send_ns,commit_ns,latency_ns\n")?;
            warn!(
                "METRICS FILE CREATED: Client {} file handle created, starting streaming writes",
                self.client_id
            );
            self.writer = Some(writer);
        }
        Ok(self.writer.as_mut().unwrap())
    }

    fn write_record(
        &mut self,
        tx_id: u64,
        is_sample: bool,
        send_time: SystemTime,
        commit_time: SystemTime,
        latency: Duration,
    ) -> std::io::Result<()> {
        let send_ns = to_unix_nanos(send_time);
        let commit_ns = to_unix_nanos(commit_time);
        let latency_ns = latency.as_nanos();
        let is_sample = if is_sample { 1_u8 } else { 0_u8 };

        let mut line = std::mem::take(&mut self.line_buffer);
        line.clear();
        self.write_int(&mut line, self.client_id as u64);
        line.push(b',');
        self.write_int(&mut line, tx_id);
        line.push(b',');
        self.write_int(&mut line, is_sample);
        line.push(b',');
        self.write_int(&mut line, send_ns);
        line.push(b',');
        self.write_int(&mut line, commit_ns);
        line.push(b',');
        self.write_int(&mut line, latency_ns);
        line.push(b'\n');

        {
            let writer = self.ensure_writer()?;
            writer.write_all(&line)?;
        }

        line.clear();
        self.line_buffer = line;
        self.records_written += 1;
        Ok(())
    }

    fn write_int<T: itoa::Integer>(&mut self, buf: &mut Vec<u8>, value: T) {
        let formatted = self.itoa_buffer.format(value);
        buf.extend_from_slice(formatted.as_bytes());
    }

    async fn run(mut self) -> std::io::Result<()> {
        warn!(
            "METRICS TASK START: Client {} metrics writer starting",
            self.client_id
        );

        // Pre-allocate to prevent HashMap rehashing during benchmark
        let mut in_flight: HashMap<u64, InFlightTransaction> = HashMap::with_capacity(2_000_000);

        while let Some(event) = self.rx.recv().await {
            match event {
                MetricsEvent::Send {
                    tx_id,
                    send_instant,
                    send_time,
                    is_sample,
                } => {
                    // Log sample tx sends
                    if is_sample && tx_id < 500 {
                        debug!(
                            "METRICS SEND: Client {} recording send for sample tx_id {}",
                            self.client_id, tx_id
                        );
                    }
                    in_flight.insert(
                        tx_id,
                        InFlightTransaction {
                            send_instant,
                            send_time,
                            is_sample,
                        },
                    );
                }
                MetricsEvent::Commit {
                    tx_id,
                    commit_instant,
                    commit_time,
                } => {
                    // Log sample tx commits
                    if tx_id < 500 {
                        debug!(
                            "METRICS COMMIT: Client {} received commit for tx_id {}",
                            self.client_id, tx_id
                        );
                    }
                    if let Some(transaction) = in_flight.remove(&tx_id) {
                        let latency = commit_instant
                            .checked_duration_since(transaction.send_instant)
                            .unwrap_or_else(|| Duration::from_secs(0));

                        // Log successful match
                        if tx_id < 500 {
                            debug!(
                                "METRICS MATCH: Client {} matched tx_id {} (sample={})",
                                self.client_id, tx_id, transaction.is_sample
                            );
                        }

                        if let Err(e) = self.write_record(
                            tx_id,
                            transaction.is_sample,
                            transaction.send_time,
                            commit_time,
                            latency,
                        ) {
                            warn!(
                                "METRICS WRITE ERROR: Client {} failed to write record for tx {}: {}",
                                self.client_id, tx_id, e
                            );
                            return Err(e);
                        }
                    } else {
                        // Log missing sends - this is critical!
                        if tx_id < 500 {
                            warn!("METRICS ORPHAN: Client {} commit for tx_id {} has NO matching send!",
                                    self.client_id, tx_id);
                        }
                    }
                }
                MetricsEvent::Shutdown => {
                    warn!(
                        "METRICS SHUTDOWN: Client {} received Shutdown event",
                        self.client_id
                    );
                    break;
                }
            }
        }

        warn!(
            "METRICS LOOP EXIT: Client {} exited event loop, records_written={}, in_flight={}",
            self.client_id,
            self.records_written,
            in_flight.len()
        );

        if !in_flight.is_empty() {
            warn!(
                "Client {} metrics writer exiting with {} in-flight transactions remaining",
                self.client_id,
                in_flight.len()
            );

            // Log sample txs that were sent but never committed
            let orphaned_samples: Vec<u64> = in_flight
                .keys()
                .filter(|&&tx_id| tx_id < 500)
                .copied()
                .collect();
            if !orphaned_samples.is_empty() {
                warn!("METRICS ORPHANED SENDS: Client {} has sample tx_ids with sends but no commits: {:?}",
                       self.client_id, orphaned_samples);
            }
        }

        if self.records_written == 0 {
            warn!(
                "METRICS EMPTY: Client {} wrote no records, not writing file",
                self.client_id
            );
            return Ok(());
        }

        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        warn!(
            "METRICS WRITE COMPLETE: Client {} flushed {} records to {:?}",
            self.client_id, self.records_written, self.output_path
        );
        Ok(())
    }
}

fn to_unix_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos()
}
