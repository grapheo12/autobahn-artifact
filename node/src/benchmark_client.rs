// Copyright(C) Facebook, Inc. and its affiliates.
mod client;
mod metrics;
mod reply_processor;
mod transaction_sender;

use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings};
use client::{Client, ClientParameters};
use config::{Committee, Import};
use env_logger::Env;
use log::info;
use metrics::MetricsCollector;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::time::Duration;

const CLIENT_DRAIN_GRACE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish - now supports replies!")
        .args_from_usage("--client-id=<INT> 'Unique identifier for this client (0-255)'")
        .args_from_usage("--reply-addr=<ADDR> 'The address for receiving replies'")
        .args_from_usage("--ack-addr=<ADDR> 'The address for receiving early ACKs'")
        .args_from_usage("--committee=<FILE> 'The file containing committee information'")
        .args_from_usage("--size=[INT] 'The size of each transaction in bytes (default: 512)'")
        .args_from_usage("--rate=[INT] 'The rate (txs/s) at which to send transactions (default: 1000)'")
        .args_from_usage("--workers=[INT] 'The number of workers to send to (default: 1)'")
        .args_from_usage("--threshold=[INT] 'Number of confirmations required (default: 1)'")
        .args_from_usage("--duration=[INT] 'How long to send transactions before shutting down (seconds, default: 20)'")
        .args_from_usage("--transaction-timeout=[INT] 'Timeout in ms for early ACKs before retry (default: 150)'")
        .args_from_usage("--metrics-file=[FILE] 'Path where the client writes latency metrics'")
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

    let ack_addr = matches
        .value_of("ack-addr")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid ACK address format")?;

    let committee_file = matches.value_of("committee").unwrap();
    let committee =
        Committee::import(committee_file).context("Failed to load committee information")?;

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

    let duration_secs = matches
        .value_of("duration")
        .unwrap_or("20")
        .parse::<u64>()
        .context("Duration must be a non-negative integer")?;
    let run_duration = Duration::from_secs(duration_secs);

    let transaction_timeout = matches
        .value_of("transaction-timeout")
        .unwrap_or("150")
        .parse::<u64>()
        .context("Transaction timeout must be a positive integer")?;

    let default_metrics_path = format!("logs/client-{}-metrics", client_id);
    let metrics_path = matches
        .value_of("metrics-file")
        .map(|p| p.to_owned())
        .unwrap_or(default_metrics_path);
    let metrics_path = PathBuf::from(metrics_path);

    // NOTE: These log entries are used to compute performance.
    info!("Client {} starting", client_id);
    info!("Reply address: {}", reply_addr);
    info!("ACK address: {}", ack_addr);
    info!("Transactions size: {} B", transaction_size);
    info!("Transactions rate: {} tx/s", transaction_rate);
    info!("Worker count: {}", worker_count);
    info!("Confirmation threshold: {}", threshold);
    info!("Run duration: {} s", duration_secs);
    info!("Transaction timeout: {} ms", transaction_timeout);

    let client_parameters = ClientParameters {
        transaction_size,
        transaction_rate,
        worker_count,
        threshold,
        duration: run_duration,
        transaction_timeout,
    };

    // Spawn the client process
    let metrics_handle = Client::spawn(
        client_id,
        committee,
        client_parameters,
        reply_addr,
        ack_addr,
        metrics_path,
    );

    wait_for_shutdown(metrics_handle, run_duration).await?;
    Ok(())
}

async fn wait_for_shutdown(metrics: MetricsCollector, run_duration: Duration) -> Result<()> {
    enum ShutdownReason {
        DurationElapsed,
        Signal,
    }

    let reason = if run_duration.is_zero() {
        wait_for_shutdown_signal().await?;
        ShutdownReason::Signal
    } else {
        tokio::select! {
            _ = tokio::time::sleep(run_duration) => ShutdownReason::DurationElapsed,
            res = wait_for_shutdown_signal() => {
                res?;
                ShutdownReason::Signal
            }
        }
    };

    if matches!(reason, ShutdownReason::DurationElapsed) {
        info!(
            "Client runtime limit of {:?} reached; allowing {:?} for replies to drain",
            run_duration, CLIENT_DRAIN_GRACE
        );
        tokio::time::sleep(CLIENT_DRAIN_GRACE).await;
    }

    metrics.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sighup = signal(SignalKind::hangup())?;

    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res?;
        }
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
        _ = sighup.recv() => {}
    };

    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}
