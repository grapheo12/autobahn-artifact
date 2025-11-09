// Copyright(C) Facebook, Inc. and its affiliates.
use crate::client::SlotTransactionReply;
use crate::metrics::MetricsCollector;
use config::ClientId;
use crypto::PublicKey;
use log::debug;
use std::collections::{BTreeSet, HashMap};
use std::time::{Instant as StdInstant, SystemTime};
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, Instant};

/// Transaction state tracking for threshold-based confirmation.
#[derive(Debug, Clone, Default)]
struct TransactionState {
    /// Number of confirmations received for this transaction.
    confirmations: usize,
    /// Time when the first confirmation was received.
    first_confirmation_time: Option<Instant>,
    /// Time when the transaction was marked as committed (threshold reached).
    commit_time: Option<Instant>,
    /// Whether this transaction has been marked as committed.
    is_committed: bool,
}

/// The ReplyProcessor component processes incoming transaction replies and implements
/// threshold-based confirmation logic.
pub struct ReplyProcessor {
    /// The unique identifier for this client.
    client_id: ClientId,
    /// Receiver for incoming reply messages.
    rx_replies: Receiver<SlotTransactionReply>,
    /// Number of confirmations required before marking a transaction as committed.
    threshold: usize,
    /// Map tracking the state of each transaction, grouped by slot.
    slot_transaction_states: HashMap<u64, HashMap<u64, TransactionState>>,
    /// Tracks how many worker confirmations we have per slot.
    slot_confirmations: HashMap<u64, usize>,
    /// Slots that have already met the confirmation threshold.
    committed_slots: BTreeSet<u64>,
    /// Latest slot seen per worker to compute the watermark.
    latest_slots: HashMap<PublicKey, u64>,
    /// Metrics collector used to persist commit events.
    metrics: MetricsCollector,
}

impl ReplyProcessor {
    pub fn spawn(
        client_id: ClientId,
        rx_replies: Receiver<SlotTransactionReply>,
        threshold: usize,
        metrics: MetricsCollector,
    ) {
        tokio::spawn(async move {
            Self {
                client_id,
                rx_replies,
                threshold,
                slot_transaction_states: HashMap::with_capacity(1_000),
                slot_confirmations: HashMap::with_capacity(1_000),
                committed_slots: BTreeSet::new(),
                latest_slots: HashMap::new(),
                metrics,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(reply) = self.rx_replies.recv().await {
            self.process_reply(reply).await;
        }
    }

    /// Process a single reply message, updating transaction confirmation counts.
    async fn process_reply(&mut self, reply: SlotTransactionReply) {
        // Log sample tx_ids in this reply
        /*let sample_txs: Vec<u64> = reply
            .committed_transactions
            .iter()
            .filter(|&&tx| tx < 500) // Only log sample tx_ids
            .copied()
            .collect();
        if !sample_txs.is_empty() {
            debug!(
                "REPLY: Client {} processing slot {} with sample txs: {:?}",
                self.client_id, reply.slot, sample_txs
            );
        }*/

        self.update_worker_slot(reply.worker_key.clone(), reply.slot);

        if self.committed_slots.contains(&reply.slot) {
            return;
        }

        let confirmations = self
            .slot_confirmations
            .entry(reply.slot)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        if *confirmations >= self.threshold {
            self.slot_confirmations.remove(&reply.slot);
            self.committed_slots.insert(reply.slot);
        }

        let now = Instant::now();
        let slot_states = self
            .slot_transaction_states
            .entry(reply.slot)
            .or_insert_with(HashMap::new);
        let mut to_remove = Vec::new();

        for transaction_counter in reply.committed_transactions {
            let state = slot_states
                .entry(transaction_counter)
                .or_insert_with(TransactionState::default);

            state.confirmations += 1;
            if state.first_confirmation_time.is_none() {
                state.first_confirmation_time = Some(now);
            }

            if state.confirmations >= self.threshold {
                state.is_committed = true;
                state.commit_time = Some(now);

                let commit_instant = StdInstant::now();
                let commit_time = SystemTime::now();

                if transaction_counter < 500 {
                    debug!(
                        "COMMIT: Client {} committing tx_id {} at slot {}",
                        self.client_id, transaction_counter, reply.slot
                    );
                }
                self.metrics
                    .record_commit(transaction_counter, commit_instant, commit_time)
                    .await;

                to_remove.push(transaction_counter);
            }
        }

        for tx_id in to_remove {
            slot_states.remove(&tx_id);
        }
        if slot_states.is_empty() {
            self.slot_transaction_states.remove(&reply.slot);
        }
        if self.committed_slots.contains(&reply.slot) {
            self.slot_transaction_states.remove(&reply.slot);
        }

        self.garbage_collect_committed_slots();

        // Periodically clean up old transaction states to prevent memory growth
        //self.cleanup_old_transactions().await;
    }

    fn update_worker_slot(&mut self, worker_key: PublicKey, slot: u64) {
        self.latest_slots
            .entry(worker_key)
            .and_modify(|latest| {
                if slot > *latest {
                    *latest = slot;
                }
            })
            .or_insert(slot);
    }

    fn current_watermark(&self) -> Option<u64> {
        self.latest_slots.values().min().copied()
    }

    fn garbage_collect_committed_slots(&mut self) {
        let watermark = match self.current_watermark() {
            Some(value) => value,
            None => return,
        };

        let removable: Vec<u64> = self
            .committed_slots
            .iter()
            .copied()
            .take_while(|slot| *slot < watermark)
            .collect();

        for slot in removable {
            self.committed_slots.remove(&slot);
        }
    }

    /// Remove old committed transactions from the state map to prevent memory growth.
    async fn cleanup_old_transactions(&mut self) {
        const CLEANUP_INTERVAL_TRANSACTIONS: usize = 1000;
        const MAX_AGE_SECONDS: u64 = 300; // 5 minutes

        let total_tracked: usize = self
            .slot_transaction_states
            .values()
            .map(|states| states.len())
            .sum();
        if total_tracked % CLEANUP_INTERVAL_TRANSACTIONS != 0 {
            return;
        }

        let now = Instant::now();
        let cutoff_time = now - Duration::from_secs(MAX_AGE_SECONDS);

        self.slot_transaction_states.retain(|_, states| {
            states.retain(|_, state| {
                !state.is_committed
                    || state
                        .commit_time
                        .map_or(true, |commit_time| commit_time > cutoff_time)
            });
            !states.is_empty()
        });
    }

    /// Get statistics about transaction confirmation for monitoring.
    pub fn get_stats(&self) -> TransactionStats {
        let mut stats = TransactionStats::default();

        for states in self.slot_transaction_states.values() {
            for state in states.values() {
                stats.total_transactions += 1;

                if state.is_committed {
                    stats.committed_transactions += 1;
                } else {
                    stats.pending_transactions += 1;
                }

                if state.confirmations > 0 {
                    stats.transactions_with_confirmations += 1;
                }
            }
        }

        stats
    }
}

/// Statistics about transaction processing for monitoring.
#[derive(Debug, Default)]
pub struct TransactionStats {
    pub total_transactions: usize,
    pub committed_transactions: usize,
    pub pending_transactions: usize,
    pub transactions_with_confirmations: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricsCollector;
    use crypto::PublicKey;
    use tokio::sync::mpsc;

    fn new_processor(threshold: usize) -> ReplyProcessor {
        ReplyProcessor {
            client_id: 0,
            rx_replies: mpsc::channel(1).1,
            threshold,
            slot_transaction_states: HashMap::new(),
            slot_confirmations: HashMap::new(),
            committed_slots: BTreeSet::new(),
            latest_slots: HashMap::new(),
            metrics: MetricsCollector::noop(0),
        }
    }

    #[tokio::test]
    async fn test_reply_processor_threshold() {
        let mut processor = new_processor(2);

        let reply1 = SlotTransactionReply {
            slot: 1,
            worker_key: PublicKey::default(),
            committed_transactions: vec![100],
        };
        processor.process_reply(reply1).await;

        let slot_states = processor.slot_transaction_states.get(&1).unwrap();
        let state = slot_states.get(&100).unwrap();
        assert_eq!(state.confirmations, 1);
        assert!(!state.is_committed);

        let reply2 = SlotTransactionReply {
            slot: 1,
            worker_key: PublicKey::default(),
            committed_transactions: vec![100],
        };
        processor.process_reply(reply2).await;

        assert!(processor.committed_slots.contains(&1));
        assert!(!processor.slot_transaction_states.contains_key(&1));

        let reply3 = SlotTransactionReply {
            slot: 2,
            worker_key: PublicKey::default(),
            committed_transactions: vec![100],
        };
        processor.process_reply(reply3).await;

        assert!(!processor.committed_slots.contains(&1));
        assert!(!processor.committed_slots.contains(&2));
    }

    #[tokio::test]
    async fn test_reply_processor_slot_tracking() {
        let mut processor = new_processor(3);

        let reply1 = SlotTransactionReply {
            slot: 5,
            worker_key: PublicKey::default(),
            committed_transactions: vec![200],
        };
        processor.process_reply(reply1).await;

        assert!(processor
            .slot_transaction_states
            .get(&5)
            .and_then(|m| m.get(&200))
            .is_some());

        let reply2 = SlotTransactionReply {
            slot: 6,
            worker_key: PublicKey::default(),
            committed_transactions: vec![200],
        };
        processor.process_reply(reply2).await;

        assert!(processor
            .slot_transaction_states
            .get(&6)
            .and_then(|m| m.get(&200))
            .is_some());
        assert!(processor
            .slot_transaction_states
            .get(&5)
            .and_then(|m| m.get(&200))
            .is_some());

        let reply3 = SlotTransactionReply {
            slot: 4,
            worker_key: PublicKey::default(),
            committed_transactions: vec![200],
        };
        processor.process_reply(reply3).await;

        assert!(processor
            .slot_transaction_states
            .get(&4)
            .and_then(|m| m.get(&200))
            .is_some());
    }

    #[tokio::test]
    async fn test_reply_processor_commit_after_threshold_same_slot() {
        let mut processor = new_processor(3);

        for _ in 0..3 {
            let reply = SlotTransactionReply {
                slot: 4,
                worker_key: PublicKey::default(),
                committed_transactions: vec![300],
            };
            processor.process_reply(reply).await;
        }

        assert!(processor.committed_slots.contains(&4));
        assert!(!processor.slot_transaction_states.contains_key(&4));
    }

    #[test]
    fn test_transaction_stats() {
        let mut processor = new_processor(2);

        let mut slot1 = HashMap::new();
        let mut state1 = TransactionState::default();
        state1.confirmations = 2;
        state1.is_committed = true;
        slot1.insert(100, state1);
        processor.slot_transaction_states.insert(1, slot1);

        let mut slot2 = HashMap::new();
        let mut state2 = TransactionState::default();
        state2.confirmations = 1;
        slot2.insert(101, state2);
        processor.slot_transaction_states.insert(2, slot2);

        let stats = processor.get_stats();
        assert_eq!(stats.total_transactions, 2);
        assert_eq!(stats.committed_transactions, 1);
        assert_eq!(stats.pending_transactions, 1);
        assert_eq!(stats.transactions_with_confirmations, 2);
    }

    #[tokio::test]
    async fn test_watermark_garbage_collection() {
        let mut processor = new_processor(1);

        let reply1 = SlotTransactionReply {
            slot: 10,
            worker_key: PublicKey::default(),
            committed_transactions: vec![400],
        };
        processor.process_reply(reply1).await;
        assert!(processor.committed_slots.contains(&10));
        assert!(!processor.slot_transaction_states.contains_key(&10));

        let reply2 = SlotTransactionReply {
            slot: 12,
            worker_key: PublicKey::default(),
            committed_transactions: vec![401],
        };
        processor.process_reply(reply2).await;

        assert!(!processor.committed_slots.contains(&10));
        assert!(processor.committed_slots.contains(&12));
        assert!(!processor.slot_transaction_states.contains_key(&12));
    }
}
