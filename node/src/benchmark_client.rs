use anyhow::Ok;
// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::{Buf, BufMut as _};
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--clients=<INT> 'Number of clients'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let clients = matches
        .value_of("clients")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    let rate = 200_000;
    info!("Transactions rate: {} tx/s", rate);

    info!("Number of clients: {}", clients);

    
    let mut futs = FuturesUnordered::new();
    for _ in 0..clients {
        let _nodes = nodes.iter().map(|e| e.clone()).collect::<Vec<_>>();
        futs.push(async move {
            let client = Arc::new(Box::pin(Client {
                target,
                size,
                rate,
                nodes: _nodes,
            }));
            
            // Wait for all nodes to be online and synchronized.
            client.wait().await;
            // Start the benchmark.
            client.send().await;
        });
    }

    for _ in 0..clients {
        futs.next().await;
    }

    Ok(())
}

struct Client {
    target: SocketAddr,  //specifies the worker to connect to
    size: usize,         //specifies the bit size of transactions
    rate: u64,
    nodes: Vec<SocketAddr>,  //specifies the addresses of all nodes. Currently only used to wait for them to be alive, but also necessary if we wanted to receive result replies (from any node).
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;
        const MAX_CONCURRENT_TXS: usize = 16;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let _burst = burst;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let (mut transport_sender, mut transport_receiver) = transport.split();
        let (sema_tx, mut sema_rx) = tokio::sync::mpsc::channel(MAX_CONCURRENT_TXS);
        let (sema_tx2, mut sema_rx2) = tokio::sync::mpsc::channel(MAX_CONCURRENT_TXS);
        for _ in 0..MAX_CONCURRENT_TXS {
            sema_tx.send(true).await;
        }

        tokio::spawn(async move {
            let mut request_store = HashMap::new();
            let mut response_store = HashSet::new();
            'main2: loop {
                tokio::select! {
                    req = sema_rx2.recv() => {
                        if let Some((x, counter, r, start_time)) = req {
                            if x == counter % _burst {
                                println!("Inserting sample transaction {}", (counter as u64) | (r << 32));
                                request_store.insert((x, counter, r), start_time);
                            }
                        }
                    },

                    resp = transport_receiver.next() => {
                        if let Some(Result::Ok(mut resp)) = resp {
                            let tag = resp.get_u8();
                            let id = resp.get_u64();
                            let x = resp.get_u64();

                            if tag == 0u8 {
                                let counter = id & ((1 << 32) - 1);
                                let r = id >> 32;

                                assert!(x == counter % _burst);
                                println!("Received sample transaction {}", id);

                                response_store.insert((x, counter, r));
                            }
                            // if x == counter % burst {
                            //     assert!(resp.get_u8() == 0u8);
                            //     assert!(resp.get_u64() == ((counter as u64) | (r << 32)));
                            // } else {
                            //     assert!(resp.get_u8() == 1u8);
                            //     assert!(resp.get_u64() == r);
                            // }
                            assert!(resp.get_u64() == 0xdeadbeef);

                            sema_tx.send(true).await;
                        } else {
                            warn!("Failed to receive transaction ack");
                            break 'main2;
                        }
                    }

                }

                let mut to_remove = vec![];
                for (x, counter, r) in request_store.keys() {
                    if response_store.contains(&(*x, *counter, *r)) {
                        to_remove.push((*x, *counter, *r));
                    }
                }

                for (x, counter, r) in to_remove {
                    let start_time: Instant = request_store.remove(&(x, counter, r)).unwrap();
                    let duration: Duration = start_time.elapsed();
                    info!("Client latency: {} ms", duration.as_millis());
                    response_store.remove(&(x, counter, r));
                }
            }
        });
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            let now = Instant::now();
            for x in 0..burst {
                let _ = sema_rx.recv().await;
                let start_time = Instant::now();
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", (counter as u64) | (r << 32));

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64((counter as u64) | (r << 32)); // This counter identifies the tx.
                    tx.put_u64(x);
                } else {
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                    tx.put_u64(x);
                };
                // while self.size > tx.len() {
                //     tx.put_u8(rand::random());
                // }
                tx.resize(self.size, 0u8); //Truncate any bits past size
                let bytes = tx.split().freeze(); //split() moves byte content from tx to bytes (i.e. avoids copy). freeze() makes it const so it can be shared. (bytes can now be used/sent async)
                //Note: Does not sign transactions. Transaction id-s are not unique w.r.t to content.
                if let Err(e) = transport_sender.send(bytes).await { //Uses TCP connection to send request to assigned worker. Note: Optimistically only sending to one worker.
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }

                sema_tx2.send((x, counter, r, start_time)).await;

                // match transport_receiver.next().await {
                //     Some(Result::Ok(mut resp)) => {
                //         if x == counter % burst {
                //             assert!(resp.get_u8() == 0u8);
                //             assert!(resp.get_u64() == ((counter as u64) | (r << 32)));
                //         } else {
                //             assert!(resp.get_u8() == 1u8);
                //             assert!(resp.get_u64() == r);
                //         }
                //         assert!(resp.get_u64() == 0xdeadbeef);
                //     },
                //     _ => {
                //         warn!("Failed to receive transaction ack");
                //         break 'main;
                //     }
                // }
                // if x == counter % burst {
                //     let duration = start_time.elapsed();
                //     info!("Client latency: {} ms", duration.as_millis());
                // }


                r += 1;
            }

            // for x in 0..burst {
            // }

            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
