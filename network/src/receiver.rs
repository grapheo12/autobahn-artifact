// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            Self { address, handler }.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
        //println!("receiver address {}", self.address.clone().to_string());
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            Self::spawn_runner(socket, peer, self.handler.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Err(e) = handler.dispatch(&mut writer, message.freeze()).await {
                            warn!("{}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}


pub type AsyncMessageResponse = (Bytes, oneshot::Receiver<()>);

#[async_trait]
pub trait AsyncMessageHandler: Clone + Send + Sync + 'static {
    async fn dispatch(&self, message: Bytes, resp_tx: Sender<AsyncMessageResponse>) -> Result<(), Box<dyn Error>>;
}

pub struct AsyncReceiver<Handler: AsyncMessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,   
}

impl<Handler: AsyncMessageHandler> AsyncReceiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            Self { address, handler }.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
        //println!("receiver address {}", self.address.clone().to_string());
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            let (tx, rx) = tokio::sync::mpsc::channel(1000);
            info!("Incoming connection established with {}", peer);
            Self::spawn_message_handler(reader, peer, self.handler.clone(), tx).await;
            Self::spawn_reply_handler(writer, peer, rx).await;
        }
    }

    async fn spawn_message_handler(mut reader: futures::stream::SplitStream<Framed<TcpStream, LengthDelimitedCodec>>, peer: SocketAddr, handler: Handler, tx: Sender<AsyncMessageResponse>) {
        tokio::spawn(async move {
            while let Some(frame) = reader.next().await {
                match frame {
                    Ok(message) => {
                        if let Err(e) = handler.dispatch(message.freeze(), tx.clone()).await {
                            warn!("{}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }

    async fn spawn_reply_handler(mut writer: futures::stream::SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>, peer: SocketAddr, mut rx: tokio::sync::mpsc::Receiver<AsyncMessageResponse>) {
        tokio::spawn(async move {
            let mut writer = writer;
            while let Some((msg, fut)) = rx.recv().await {
                let _ = fut.await.unwrap();
                if let Err(e) = writer.send(msg).await {
                    warn!("{}", e);
                    return;
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}