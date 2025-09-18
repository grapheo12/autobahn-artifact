// Copyright(C) Facebook, Inc. and its affiliates.
use crate::client::SlotTransactionReply;
use config::ClientId;
use log::{debug, info, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, Instant};

/// Transaction state tracking for threshold-based confirmation.
#[derive(Debug, Clone)]
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

impl Default for TransactionState {
    fn default() -> Self {
        Self {
            confirmations: 0,
            first_confirmation_time: None,
            commit_time: None,
            is_committed: false,
        }
    }
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
    /// Map tracking the state of each transaction.
    transaction_states: HashMap<u64, TransactionState>,
}

impl ReplyProcessor {
    pub fn spawn(
        client_id: ClientId,
        rx_replies: Receiver<SlotTransactionReply>,
        threshold: usize,
    ) {
        tokio::spawn(async move {
            Self {
                client_id,
                rx_replies,
                threshold,
                transaction_states: HashMap::new(),
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
        // Processing reply for slot with transactions

        let now = Instant::now();

        for transaction_counter in reply.committed_transactions {
            let state = self.transaction_states
                .entry(transaction_counter)
                .or_insert_with(TransactionState::default);

            // Skip if already committed
            if state.is_committed {
                continue;
            }

            // Increment confirmation count
            state.confirmations += 1;

            // Record first confirmation time
            if state.first_confirmation_time.is_none() {
                state.first_confirmation_time = Some(now);
            }

            // Transaction confirmation tracking

            // Check if threshold is reached
            if state.confirmations >= self.threshold && !state.is_committed {
                state.is_committed = true;
                state.commit_time = Some(now);

                // NOTE: This log entry is used to compute performance.
                info!("Client {} transaction {} committed after {} confirmations",
                      self.client_id, transaction_counter, state.confirmations);
            }
        }

        // Periodically clean up old transaction states to prevent memory growth
        self.cleanup_old_transactions().await;
    }

    /// Remove old committed transactions from the state map to prevent memory growth.
    async fn cleanup_old_transactions(&mut self) {
        const CLEANUP_INTERVAL_TRANSACTIONS: usize = 1000;
        const MAX_AGE_SECONDS: u64 = 300; // 5 minutes

        // Only cleanup periodically
        if self.transaction_states.len() % CLEANUP_INTERVAL_TRANSACTIONS != 0 {
            return;
        }

        let now = Instant::now();
        let cutoff_time = now - Duration::from_secs(MAX_AGE_SECONDS);

        let initial_count = self.transaction_states.len();
        
        self.transaction_states.retain(|_, state| {
            // Keep transactions that are either:
            // 1. Not yet committed, or
            // 2. Committed recently (within MAX_AGE_SECONDS)
            !state.is_committed || 
            state.commit_time.map_or(true, |commit_time| commit_time > cutoff_time)
        });

        let cleaned_count = initial_count - self.transaction_states.len();
        // Cleaned up old transaction states
    }

    /// Get statistics about transaction confirmation for monitoring.
    pub fn get_stats(&self) -> TransactionStats {
        let mut stats = TransactionStats::default();
        
        for state in self.transaction_states.values() {
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
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_reply_processor_threshold() {
        let (tx, rx) = mpsc::channel(10);
        
        // Create processor with threshold of 2
        let mut processor = ReplyProcessor {
            client_id: 0,
            rx_replies: rx,
            threshold: 2,
            transaction_states: HashMap::new(),
        };

        // Send first confirmation
        let reply1 = SlotTransactionReply {
            slot: 1,
            committed_transactions: vec![100],
        };
        processor.process_reply(reply1).await;

        // Transaction should not be committed yet
        let state = processor.transaction_states.get(&100).unwrap();
        assert_eq!(state.confirmations, 1);
        assert!(!state.is_committed);

        // Send second confirmation
        let reply2 = SlotTransactionReply {
            slot: 2,
            committed_transactions: vec![100],
        };
        processor.process_reply(reply2).await;

        // Transaction should now be committed
        let state = processor.transaction_states.get(&100).unwrap();
        assert_eq!(state.confirmations, 2);
        assert!(state.is_committed);
        assert!(state.commit_time.is_some());
    }

    #[test]
    fn test_transaction_stats() {
        let mut processor = ReplyProcessor {
            client_id: 0,
            rx_replies: mpsc::channel(1).1, // Dummy receiver
            threshold: 2,
            transaction_states: HashMap::new(),
        };

        // Add some test states
        let mut state1 = TransactionState::default();
        state1.confirmations = 2;
        state1.is_committed = true;
        processor.transaction_states.insert(100, state1);

        let mut state2 = TransactionState::default();
        state2.confirmations = 1;
        processor.transaction_states.insert(101, state2);

        let stats = processor.get_stats();
        assert_eq!(stats.total_transactions, 2);
        assert_eq!(stats.committed_transactions, 1);
        assert_eq!(stats.pending_transactions, 1);
        assert_eq!(stats.transactions_with_confirmations, 2);
    }
}
