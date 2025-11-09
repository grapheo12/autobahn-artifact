#![allow(dead_code)]
use std::collections::{BTreeMap, HashMap, HashSet};

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, ConsensusMessage, Header};
use crate::primary::{Height, PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, Hash, PublicKey, SignatureService};
use log::{debug, info, warn};
use network::{CancelHandler, ReliableSender};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information
    committee: Committee,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<Certificate>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    // Receives new consensus instance
    rx_instance: Receiver<ConsensusMessage>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
    /// Network sender for sending messages to workers.
    network: ReliableSender,

    /// The current height of this validator's chain
    height: Height,
    /// Holds the certificate waiting to be included in the next header
    last_parent: Option<Certificate>,
    // Holds the consensus info for the last special header
    consensus_instances: HashMap<Digest, ConsensusMessage>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// Tracks the payload of the most recently sent header
    last_header_payload: Option<BTreeMap<Digest, WorkerId>>,

    num_active_instances: usize,
    use_special_rule: bool,
    is_special: bool,
    /// Cancel handlers for network messages to workers.
    cancel_handlers: Vec<CancelHandler>,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<Certificate>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_instance: Receiver<ConsensusMessage>,
        tx_core: Sender<Header>,
    ) {
        /*let genesis: Vec<Digest> = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect();*/

        let genesis = Certificate::genesis_cert(&committee);

        let network = ReliableSender::new();

        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                rx_instance,
                tx_core,
                network,
                height: 0,
                last_parent: Some(genesis),
                consensus_instances: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                last_header_payload: None,
                num_active_instances: 0,
                use_special_rule: false,
                is_special: false,
                cancel_handlers: Vec::new(),
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        debug!("digests size before is {:?}", self.digests.len());
        /*let mut header: Header;
        if self.digests.len() > 0 {
            header = Header::new(
                self.name,
                self.height,
                self.digests.drain(..1).collect(),
                self.last_parent.clone().unwrap(),
                &mut self.signature_service,
                self.consensus_instances.clone(),
                self.num_active_instances,
            ).await;
        } else {
            header = Header::new(
                self.name,
                self.height,
                BTreeMap::new(),
                self.last_parent.clone().unwrap(),
                &mut self.signature_service,
                self.consensus_instances.clone(),
                self.num_active_instances,
            ).await;

        }*/

        // Collect the payload and save it for certificate notification
        let payload: BTreeMap<Digest, WorkerId> = self.digests.drain(..).collect();
        self.last_header_payload = Some(payload.clone());

        let mut header = Header::new(
            self.name,
            self.height,
            payload,
            self.last_parent.clone().unwrap(),
            &mut self.signature_service,
            self.consensus_instances.clone(),
            self.num_active_instances,
        )
        .await;

        if self.is_special {
            header.special = true;
            //TODO: need to also include the digest of the last proposal. Otherwise there is no gain in latency for that tx.
            // Instead of including Certificate as parent => include digest.
        }

        /*debug!("Created {:?}", header);

        for (digest, _) in &header.consensus_messages {
           debug!("Header has {:?}", digest);
        }*/

        debug!("Created own header at height {:?}", header.height);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Reset last parent
        self.last_parent = None;
        // Reset proposed consensus instances
        self.consensus_instances.clear();
        self.num_active_instances = 0;

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        //debug!("Dag starting at round {}", self.height);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            // 3. If it is a special block opportunity. That is when either a QC or TC from the previous view forms,
            // we have a ticket to propose a new block
            // For both normal blocks and special blocks, delegate the actual sending to the consensus module
            // in other words core should not be disseminating headers
            //let enough_parents = !self.last_parent.is_empty();
            let enough_parent = self.last_parent.is_some();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && (enough_parent || self.is_special) {
                if timer_expired {
                    info!("Timer expired for height {}", self.height);
                }

                debug!(
                    "New car proposed after {:?} ms",
                    current_time.elapsed().as_millis()
                );
                //debug!("is special is {:?}", self.is_special);
                current_time = Instant::now();

                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // Received info from consensus
                Some(info) = self.rx_instance.recv() => {
                    //debug!("received consensus info");

                    match &info {
                        ConsensusMessage::Prepare { slot, view, tc: _, qc_ticket: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                            //debug!("prepare has digest: {}", info.digest());
                        },
                        ConsensusMessage::Confirm { slot: _, view: _, qc: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                        },
                        _ => {},
                    }

                    self.consensus_instances.insert(info.digest(), info);
                }

                // Receive own certificate from core (we are the author)
                Some(parent) = self.rx_core.recv() => {
                    //debug!("   received parent from height {:?}", parent.height);

                    if parent.height < self.height {
                        warn!("Received parent from a previous height: {} < {}", parent.height, self.height);
                        continue;
                    }

                    // Send certificate notification to co-located worker (worker_id = 0) with last header's payload
                    if let Some(ref payload) = self.last_header_payload {
                        let batch_digests: HashSet<Digest> = payload.keys().cloned().collect();
                        debug!("Proposer: certificate formed for header with {} batch digests", batch_digests.len());
                        if !batch_digests.is_empty() {
                            // Get worker 0's address
                            if let Ok(worker_info) = self.committee.worker(&self.name, &0) {
                                let message = PrimaryWorkerMessage::CertificateFormed(batch_digests.clone());
                                if let Ok(bytes) = bincode::serialize(&message) {
                                    let address = worker_info.primary_to_worker;
                                    debug!("Proposer: sending CertificateFormed to worker 0 at {} with {} digests", address, batch_digests.len());
                                    let handler = self.network.send(address, Bytes::from(bytes)).await;
                                    self.cancel_handlers.push(handler);
                                } else {
                                    warn!("Proposer: failed to serialize CertificateFormed message");
                                }
                            } else {
                                warn!("Proposer: could not find worker 0 address");
                            }
                        } else {
                            debug!("Proposer: skipping CertificateFormed notification (empty payload)");
                        }
                    } else {
                        debug!("Proposer: no last_header_payload available for certificate notification");
                    }

                    // Advance to the next height.
                    self.height += 1;
                    debug!("Chain moved to height {}", self.height);

                    // Signal that we have a parent certificates to propose a new header.
                    self.last_parent = Some(parent.clone());
                }

                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    debug!("Proposer received OurBatch digest {} from worker {}", digest, worker_id);
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                    debug!("Proposer now has {} digests, total payload size: {}", self.digests.len(), self.payload_size);
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
