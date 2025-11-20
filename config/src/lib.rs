// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{
    generate_keypair, generate_production_keypair, Hash, PublicKey, SecretKey,
};
use log::info;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{vec_deque, BTreeMap, HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },

    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize + DeserializeOwned {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn read(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub type Stake = u32;
pub type WorkerId = u32;
pub type ClientId = u8;

fn default_start_slot_rounds() -> u64 {
    1
}
fn default_use_threshold_random_coin() -> bool {
    true
}

#[derive(Deserialize, Clone)]
pub struct Parameters {
    /// The timeout delay of the consensus protocol.
    pub timeout_delay: u64,
    /// The preferred header size. The primary creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the primary waits between generating two headers, even if the header
    /// did not reach `max_header_size`. Denominated in ms.
    pub max_header_delay: u64,
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,

    //Autobahn protocol config parameters
    pub use_optimistic_tips: bool, //default = true (TODO: implement non optimistic tip option)
    #[serde(default = "default_use_threshold_random_coin")]
    pub use_threshold_random_coin: bool,
    #[serde(default)]
    pub optimistic_leader_only: bool,

    pub use_parallel_proposals: bool, //default = true (TODO: implement sequential slot option)
    pub k: u64,                       //Max open conensus instances at a time.

    pub use_fast_path: bool, //default = false
    pub fast_path_timeout: u64,

    pub use_ride_share: bool,
    pub car_timeout: u64,

    //asynchrony simulation:
    // pub simulate_asynchrony: bool,
    // pub asynchrony_start: u64,
    // pub asynchrony_duration: u64,
    pub simulate_asynchrony: bool,          //Simulating an async event
    pub asynchrony_type: VecDeque<u8>, //Effect codes: 0=off/full delay, 1=temp blip, 2=failure, 3=partition, 4=egress delay, 5=slowdown stall
    pub asynchrony_start: VecDeque<u64>, //Start of async period   //offset from current time (in seconds) when to start next async effect
    pub asynchrony_duration: VecDeque<u64>, //Duration of async period
    pub affected_nodes: VecDeque<u64>,   ////first k nodes experience specified async behavior

    pub egress_penalty: u64, //ms of delay
    pub use_fast_sync: bool,
    pub use_exponential_timeouts: bool,

    // Number of start-slot rounds non-optimistic leaders must complete
    // before proposing in a slot. 1 preserves previous behavior.
    #[serde(default = "default_start_slot_rounds")]
    pub start_slot_rounds: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 1_000,
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,

            //Autobahn microbench configs
            use_optimistic_tips: true,
            use_threshold_random_coin: true,
            optimistic_leader_only: false,
            use_parallel_proposals: true,
            k: 4,
            use_fast_path: true,
            fast_path_timeout: 500,
            use_ride_share: false,
            car_timeout: 2000,

            //Async simulation:
            // simulate_asynchrony: false,
            // asynchrony_start: 20_000, //20 second in
            // asynchrony_duration: 10_000, //10 seconds

            //Async simulation:
            simulate_asynchrony: false,
            asynchrony_type: vec![0].into(),
            asynchrony_start: vec![20_000].into(), //20 second in
            asynchrony_duration: vec![10_000].into(), //10 seconds
            affected_nodes: vec![0].into(),

            egress_penalty: 0,
            use_fast_sync: false,
            use_exponential_timeouts: false,
            start_slot_rounds: 1,
        }
    }
}

impl Import for Parameters {}

impl Parameters {
    pub fn log(&self) {
        // NOTE: These log entries are needed to compute performance.
        info!("Timeout delay set to {} ms", self.timeout_delay);
        info!("Header size set to {} B", self.header_size);
        info!("Max header delay set to {} ms", self.max_header_delay);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);

        info!(
            "Fast path enabled? {}. Fast timeout: {}",
            self.use_fast_path, self.fast_path_timeout
        );
        info!("Optimistic tips enabled? {}", self.use_optimistic_tips);
        info!(
            "Threshold random coin enabled? {}",
            self.use_threshold_random_coin
        );
        info!(
            "Optimistic-leader-only mode enabled? {}",
            self.optimistic_leader_only
        );
        info!(
            "Parallel Proposals enabled? {}. K: {}",
            self.use_parallel_proposals, self.k
        );
        info!(
            "Ride share enabled? {}. Car timeout: {}",
            self.use_ride_share, self.car_timeout
        );
        info!("Start-slot rounds set to {}", self.start_slot_rounds);
    }
}

#[derive(Clone, Deserialize)]
pub struct ConsensusAddresses {
    /// Address to receive messages from other consensus nodes (WAN).
    pub consensus_to_consensus: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers (LAN).
    pub worker_to_primary: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers (WAN).
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary (LAN).
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct ClientAddresses {
    /// Address to receive transaction replies from workers (WAN).
    pub replies: SocketAddr,
    /// Address to receive early ACKs from workers when certificates form.
    pub transaction_acks: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the consensus protocol.
    pub consensus: ConsensusAddresses,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
    // Id for tss
    pub id: usize, // id of the node in the tss public key share set
}

#[derive(Clone, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
    /// Map of clients' id and their network addresses.
    pub clients: HashMap<ClientId, ClientAddresses>,
    //pub id_map: HashMap<PublicKey, u64>, //position
}

impl Import for Committee {}

impl Committee {
    pub fn new(info: Vec<(PublicKey, usize, Stake, SocketAddr)>) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, id, stake, address)| {
                    let authority = Authority {
                        stake,
                        id,
                        consensus: ConsensusAddresses {
                            consensus_to_consensus: address,
                        },
                        primary: PrimaryAddresses {
                            primary_to_primary: address,
                            worker_to_primary: address,
                        },
                        workers: HashMap::new(),
                    };
                    (name, authority)
                })
                .collect(),
            clients: HashMap::new(),
        }
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    pub fn random_coin_threshold(&self) -> Stake {
        /*let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes - 1) / 3 + 1*/
        self.quorum_threshold()
    }

    pub fn id(&self, name: PublicKey) -> usize {
        self.authorities.get(&name).unwrap().id
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    pub fn fast_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes
    }

    /// Returns the consensus addresses of the target consensus node.
    pub fn consensus(&self, to: &PublicKey) -> Result<ConsensusAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.consensus.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all consensus nodes except `myself`.
    pub fn others_consensus(&self, myself: &PublicKey) -> Vec<(PublicKey, ConsensusAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.consensus.clone()))
            .collect()
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }

    pub fn address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.consensus.consensus_to_consensus)
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.consensus.consensus_to_consensus))
            .collect()
    }

    /// Returns the addresses of a specific client.
    pub fn client(&self, id: &ClientId) -> Result<ClientAddresses, ConfigError> {
        self.clients
            .get(id)
            .cloned()
            .ok_or_else(|| ConfigError::UnknownWorker(*id as u32))
    }

    /// Returns the addresses of all clients.
    pub fn all_clients(&self) -> Vec<(ClientId, ClientAddresses)> {
        self.clients
            .iter()
            .map(|(id, addresses)| (*id, addresses.clone()))
            .collect()
    }

    /// Add a client to the committee (for dynamic client registration).
    pub fn add_client(&mut self, id: ClientId, addresses: ClientAddresses) {
        self.clients.insert(id, addresses);
    }

    /// Remove a client from the committee.
    pub fn remove_client(&mut self, id: &ClientId) -> Option<ClientAddresses> {
        self.clients.remove(id)
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    /// The node's public key (and identifier).
    pub name: PublicKey,
    /// The node's secret key.
    pub secret: SecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}
