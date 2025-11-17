// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use std::collections::VecDeque;
use std::fs;
use tokio::sync::mpsc::channel;

fn new_store(path: &str) -> Store {
    let _ = fs::remove_dir_all(path);
    Store::new(path).unwrap()
}

fn default_async_config() -> (
    bool,
    VecDeque<u8>,
    VecDeque<u64>,
    VecDeque<u64>,
    VecDeque<u64>,
) {
    (
        false,
        VecDeque::new(),
        VecDeque::new(),
        VecDeque::new(),
        VecDeque::new(),
    )
}

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let (tx_batch, _rx_batch) = channel(1);
    let dummy_key = PublicKey::default();
    let dummy_addresses = vec![(dummy_key, "127.0.0.1:0".parse().unwrap())];
    let store = new_store(".db_test_batch_maker_make_batch");
    let (simulate_asynchrony, asynchrony_type, asynchrony_start, asynchrony_duration, affected) =
        default_async_config();

    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        tx_message,
        tx_batch,
        dummy_addresses.clone(),
        store,
        simulate_asynchrony,
        asynchrony_type,
        asynchrony_start,
        asynchrony_duration,
        affected,
        vec![dummy_key],
        dummy_key,
        0,
    );

    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction(), transaction()];
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch, _) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let (tx_batch, _rx_batch) = channel(1);
    let dummy_key = PublicKey::default();
    let dummy_addresses = vec![(dummy_key, "127.0.0.1:1".parse().unwrap())];
    let store = new_store(".db_test_batch_maker_timeout");
    let (simulate_asynchrony, asynchrony_type, asynchrony_start, asynchrony_duration, affected) =
        default_async_config();

    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50,
        rx_transaction,
        tx_message,
        tx_batch,
        dummy_addresses.clone(),
        store,
        simulate_asynchrony,
        asynchrony_type,
        asynchrony_start,
        asynchrony_duration,
        affected,
        vec![dummy_key],
        dummy_key,
        0,
    );

    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction()];
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch, _) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}
