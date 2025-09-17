// Copyright(C) Facebook, Inc. and its affiliates.
mod client;
mod reply_processor;
mod transaction_sender;

use client::{Client, ClientParameters};
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings};
use config::{Committee, Import};
use env_logger::Env;
use log::info;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish - now supports replies!")
        .args_from_usage("--client-id=<INT> 'Unique identifier for this client (0-255)'")
        .args_from_usage("--reply-addr=<ADDR> 'The address for receiving replies'")
        .args_from_usage("--committee=<FILE> 'The file containing committee information'")
        .args_from_usage("--size=[INT] 'The size of each transaction in bytes (default: 512)'")
        .args_from_usage("--rate=[INT] 'The rate (txs/s) at which to send transactions (default: 1000)'")
        .args_from_usage("--workers=[INT] 'The number of workers to send to (default: 1)'")
        .args_from_usage("--threshold=[INT] 'Number of confirmations required (default: 1)'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let client_id = matches
        .value_of("client-id")
        .unwrap()
        .parse::<u8>()
        .context("The client ID must be between 0 and 255")?;

    let reply_addr = matches
        .value_of("reply-addr")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid reply address format")?;

    let committee_file = matches.value_of("committee").unwrap();
    let committee = Committee::import(committee_file)
        .context("Failed to load committee information")?;

    let transaction_size = matches
        .value_of("size")
        .unwrap_or("512")
        .parse::<usize>()
        .context("Transaction size must be a positive integer")?;

    let transaction_rate = matches
        .value_of("rate")
        .unwrap_or("1000")
        .parse::<u64>()
        .context("Transaction rate must be a positive integer")?;

    let worker_count = matches
        .value_of("workers")
        .unwrap_or("1")
        .parse::<usize>()
        .context("Worker count must be a positive integer")?;

    let threshold = matches
        .value_of("threshold")
        .unwrap_or("1")
        .parse::<usize>()
        .context("Threshold must be a positive integer")?;

    // NOTE: These log entries are used to compute performance.
    info!("Client {} starting", client_id);
    info!("Reply address: {}", reply_addr);
    info!("Transactions size: {} B", transaction_size);
    info!("Transactions rate: {} tx/s", transaction_rate);
    info!("Worker count: {}", worker_count);
    info!("Confirmation threshold: {}", threshold);

    let client_parameters = ClientParameters {
        transaction_size,
        transaction_rate,
        worker_count,
        threshold,
    };

    // Spawn the client process
    Client::spawn(client_id, committee, client_parameters, reply_addr);

    // Keep the main process alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}
