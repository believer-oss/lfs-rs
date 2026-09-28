// Copyright (c) 2021 Jason White
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#![deny(clippy::all)]

mod app;
mod auth;
mod error;
mod hyperext;
mod lfs;
mod locks;
mod logger;
mod lru;
mod sha256;
pub mod stats;
#[doc(hidden)]
pub mod storage;
mod util;

use anyhow::Context as _;
use futures::future::{BoxFuture, Either, Future, TryFutureExt};
use parking_lot::RwLock;
use std::{
    net::SocketAddr, path::PathBuf, pin::pin, sync::Arc, time::Duration,
};

use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tower::ServiceBuilder;

use auth::Auth;
use linked_hash_map::LinkedHashMap;

use crate::app::App;
use crate::error::Error;
pub use crate::lfs::Oid;
pub use crate::locks::{
    CreateLockBatchRequest, ListLocksResponse, LocalLs, LockBatch,
    LockBatchOuter, LockFailure, LockStorage, LockStoreError, NoneLs,
    ReleaseLockBatchRequest,
};
use crate::logger::Logger;
use crate::storage::{Cached, Disk, Encrypted, S3, Storage, Verify};
pub use crate::util::{empty, from_json, full, into_json};

#[cfg(feature = "dynamodb")]
pub use crate::locks::DynamoLs;

#[cfg(feature = "redis")]
pub use crate::locks::RedisLs;

#[cfg(feature = "faulty")]
use crate::storage::Faulty;

#[derive(Debug)]
pub struct Cache {
    /// Path to the cache.
    dir: PathBuf,

    /// Maximum size of the cache, in bytes.
    max_size: u64,
}

impl Cache {
    pub fn new(dir: PathBuf, max_size: u64) -> Self {
        Self { dir, max_size }
    }
}

#[derive(Debug)]
pub struct S3ServerBuilder {
    bucket: String,
    key: Option<[u8; 32]>,
    prefix: Option<String>,
    cdn: Option<String>,
    s3_accelerate: bool,
    cache: Option<Cache>,
    authenticated: bool,
    authentication_server: Option<String>,
    size_cache_entries: usize,
    credential_refresh_buffer: Duration,
    sdk_config: Option<aws_config::SdkConfig>,
}

impl S3ServerBuilder {
    pub fn new(bucket: String, key: Option<[u8; 32]>) -> Self {
        Self {
            bucket,
            prefix: None,
            cdn: None,
            s3_accelerate: false,
            key,
            cache: None,
            authenticated: false,
            authentication_server: None,
            size_cache_entries: 256000, // Default to 256k entries
            credential_refresh_buffer:
                storage::DEFAULT_CREDENTIAL_REFRESH_BUFFER,
            sdk_config: None,
        }
    }

    /// Sets the bucket to use.
    pub fn bucket(&mut self, bucket: String) -> &mut Self {
        self.bucket = bucket;
        self
    }

    /// Sets the encryption key to use. It is ignored with a CDN or S3 Transfer
    /// Acceleration, which transfer objects directly between clients and S3.
    pub fn key(&mut self, key: Option<[u8; 32]>) -> &mut Self {
        self.key = key;
        self
    }

    /// Sets the prefix to use.
    pub fn prefix(&mut self, prefix: String) -> &mut Self {
        self.prefix = Some(prefix);
        self
    }

    /// Sets the base URL of the CDN to use. Objects are then not encrypted,
    /// even with a key, since they are transferred directly to and from S3.
    pub fn cdn(&mut self, url: String) -> &mut Self {
        self.cdn = Some(url);
        self
    }

    /// Sets the flag to use S3 accelerate endpoints.
    pub fn s3_accelerate(&mut self, s3_accelerate: bool) -> &mut Self {
        self.s3_accelerate = s3_accelerate;
        self
    }

    /// Sets the cache to use. If not specified, then no local disk cache is
    /// used. All objects will get sent directly to S3.
    pub fn cache(&mut self, cache: Cache) -> &mut Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the flag to perform authentication on endpoints
    pub fn authenticated(&mut self, authenticated: bool) -> &mut Self {
        self.authenticated = authenticated;
        self
    }

    /// This is used only by tests.
    pub fn authentication_server(
        &mut self,
        authentication_server: String,
    ) -> &mut Self {
        self.authentication_server = Some(authentication_server);
        self
    }

    /// Sets the maximum number of entries in the S3 size cache.
    pub fn size_cache_entries(&mut self, entries: usize) -> &mut Self {
        self.size_cache_entries = entries;
        self
    }

    /// Sets how long before they expire the S3 credentials are refreshed.
    /// Zero keeps the SDK's default. See
    /// [`storage::DEFAULT_CREDENTIAL_REFRESH_BUFFER`].
    pub fn credential_refresh_buffer(&mut self, buffer: Duration) -> &mut Self {
        self.credential_refresh_buffer = buffer;
        self
    }

    /// Sets the AWS configuration to use instead of loading it from the
    /// environment. A custom endpoint in this configuration selects
    /// path-style addressing, as `$AWS_S3_ENDPOINT` does.
    pub fn sdk_config(&mut self, config: aws_config::SdkConfig) -> &mut Self {
        self.sdk_config = Some(config);
        self
    }

    /// Spawns the server. The server must be awaited on in order to accept
    /// incoming client connections and run.
    pub async fn spawn(
        mut self,
        addr: SocketAddr,
        locks: impl LockStorage + Send + Sync + 'static,
    ) -> Result<
        (BoxFuture<'static, Result<(), Error>>, SocketAddr),
        Box<dyn std::error::Error>,
    > {
        let prefix = self.prefix.unwrap_or_else(|| String::from("lfs"));

        // With a CDN or S3TA, transfers go straight between clients and S3,
        // where the server can't encrypt them. Some objects still go through
        // the server (uploads over 5 GiB, which can't be presigned), and those
        // must not be encrypted either: they would be served as ciphertext.
        if self.key.is_some() && (self.cdn.is_some() || self.s3_accelerate) {
            tracing::warn!(
                "--key is ignored with --cdn or --s3ta: objects are \
                 transferred directly between clients and S3, so they are \
                 stored unencrypted."
            );
            self.key = None;
        }

        if self.cdn.is_some() && self.cache.take().is_some() {
            tracing::warn!(
                "A local disk cache does not work with a CDN and will be \
                 disabled."
            );
        }

        let sdk_config = match self.sdk_config {
            Some(config) => config,
            None => S3::load_config().await?,
        };
        let s3 = S3::from_config(
            &sdk_config,
            self.bucket,
            prefix,
            self.cdn,
            self.s3_accelerate,
            self.size_cache_entries,
            self.credential_refresh_buffer,
        );
        s3.check().await?;

        // Retries are left to the AWS SDK, which retries transient failures
        // (5xx, throttling, timeouts, connection errors) up to 3 times.

        // Add a little instability for testing purposes.
        #[cfg(feature = "faulty")]
        let s3 = Faulty::new(s3);

        match self.cache {
            Some(cache) => {
                // Use disk storage as a cache.
                let disk = Disk::new(cache.dir).map_err(Error::from).await?;

                #[cfg(feature = "faulty")]
                let disk = Faulty::new(disk);

                let cache = Cached::new(cache.max_size, disk, s3).await?;
                let storage = Verify::new(match self.key {
                    Some(key) => {
                        Either::Left(Verify::new(Encrypted::new(key, cache)))
                    }
                    None => {
                        tracing::warn!("Not encrypting cache");
                        Either::Right(Verify::new(cache))
                    }
                });
                let (fut, addr) = spawn_server(
                    storage,
                    locks,
                    addr,
                    self.authenticated,
                    self.authentication_server,
                )
                .await?;

                Ok((Box::pin(fut), addr))
            }
            None => {
                let storage = Verify::new(match self.key {
                    Some(key) => {
                        Either::Left(Verify::new(Encrypted::new(key, s3)))
                    }
                    None => Either::Right(Verify::new(s3)),
                });
                let (fut, addr) = spawn_server(
                    storage,
                    locks,
                    addr,
                    self.authenticated,
                    self.authentication_server,
                )
                .await?;

                Ok((Box::pin(fut), addr))
            }
        }
    }

    /// Spawns the server and runs it to completion. This will run forever
    /// unless there is an error or the server shuts down gracefully.
    pub async fn run(
        self,
        addr: SocketAddr,
        lock: impl LockStorage + Send + Sync + 'static,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (server, addr) = self.spawn(addr, lock).await?;

        tracing::info!("Listening on {}", addr);

        server.await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LocalServerBuilder {
    path: PathBuf,
    key: Option<[u8; 32]>,
    authenticated: bool,
    authentication_server: Option<String>,
}

impl LocalServerBuilder {
    /// Creates a local server builder. `path` is the path to the folder where
    /// all of the LFS data will be stored.
    pub fn new(path: PathBuf, key: Option<[u8; 32]>) -> Self {
        Self {
            path,
            key,
            authenticated: false,
            authentication_server: None,
        }
    }

    /// Sets the encryption key to use.
    pub fn key(&mut self, key: Option<[u8; 32]>) -> &mut Self {
        self.key = key;
        self
    }

    /// Sets the flag to perform authentication on endpoints
    pub fn authenticated(&mut self, authenticated: bool) -> &mut Self {
        self.authenticated = authenticated;
        self
    }

    /// This is used only by tests.
    pub fn authentication_server(
        &mut self,
        authentication_server: String,
    ) -> &mut Self {
        self.authentication_server = Some(authentication_server);
        self
    }

    /// Spawns the server. The server must be awaited on in order to accept
    /// incoming client connections and run.
    pub async fn spawn(
        self,
        addr: SocketAddr,
        locks: impl LockStorage + Send + Sync + 'static,
    ) -> Result<
        (BoxFuture<'static, Result<(), Error>>, SocketAddr),
        Box<dyn std::error::Error>,
    > {
        let storage = Disk::new(self.path).map_err(Error::from).await?;
        let storage = Verify::new(match self.key {
            Some(key) => {
                Either::Left(Verify::new(Encrypted::new(key, storage)))
            }
            None => {
                tracing::warn!("Not encrypting cache");
                Either::Right(Verify::new(storage))
            }
        });

        tracing::info!("Local disk storage initialized.");

        let (fut, addr) = spawn_server(
            storage,
            locks,
            addr,
            self.authenticated,
            self.authentication_server,
        )
        .await?;

        Ok((Box::pin(fut), addr))
    }

    /// Spawns the server and runs it to completion. This will run forever
    /// unless there is an error or the server shuts down gracefully.
    pub async fn run(
        self,
        addr: SocketAddr,
        locks: impl LockStorage + Send + Sync + 'static,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (server, addr) = self.spawn(addr, locks).await?;

        tracing::info!("Listening on {}", addr);

        server.await?;
        Ok(())
    }
}

async fn spawn_server<S, L>(
    storage: S,
    locks: L,
    addr: SocketAddr,
    authenticated: bool,
    authentication_server: Option<String>,
) -> Result<(impl Future<Output = Result<(), Error>>, SocketAddr), Error>
where
    S: Storage + Send + Sync + 'static,
    S::Error: Into<Error>,
    L: LockStorage + Send + Sync + 'static,
    Error: From<S::Error>,
{
    let storage = Arc::new(storage);
    let locks = Arc::new(locks);
    let cache = Arc::new(RwLock::new(LinkedHashMap::new()));

    // One client for every connection, so they share its connection pool.
    // Created only when needed, so that without GitHub auth the system root
    // certificates aren't required.
    let github = if authenticated {
        Some(auth::github_client().context(
            "failed to create the GitHub API client (loading root \
             certificates)",
        )?)
    } else {
        None
    };

    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let server = hyper_util::server::conn::auto::Builder::new(
        hyper_util::rt::TokioExecutor::new(),
    );

    Ok((
        async move {
            let graceful =
                hyper_util::server::graceful::GracefulShutdown::new();
            let mut ctrl_c = pin!(tokio::signal::ctrl_c());
            loop {
                tokio::select! {
                    conn = listener.accept() => {
                        let (stream, peer_addr) = match conn {
                            Ok(conn) => conn,
                            Err(e) => {
                                tracing::error!("accept error: {}", e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };
                        // Create our app.
                        let app = App::new(storage.clone(), locks.clone());
                        let cache = Arc::clone(&cache);
                        stream.set_nodelay(true)?;

                        // Wrap the app in an auth middleware. If not authenticated, wrapper
                        // acts as a passthrough service.
                        let auth = Auth::new(
                            app,
                            cache,
                            github.clone(),
                            authentication_server.clone(),
                        );

                        // Add logging middleware
                        let logger = Logger::new(peer_addr, auth);
                        let svc = TowerToHyperService::new(logger);

                        let service = ServiceBuilder::new()
                            .service(svc);

                        let stream = hyper_util::rt::TokioIo::new(Box::pin(stream));

                        let conn = server.serve_connection(stream, service);

                        let conn = graceful.watch(conn.into_owned());

                        let handler = async move {
                            if let Err(err) = conn.await {
                                tracing::error!("connection error: {}", err);
                            }
                            tracing::debug!("connection dropped: {}", peer_addr);
                        };
                        tokio::spawn(handler);
                    },

                    _ = ctrl_c.as_mut() => {
                        drop(listener);
                        tracing::info!("Ctrl-C received, starting shutdown");
                            break;
                    }
                }
            }

            tokio::select! {
                _ = graceful.shutdown() => {
                    tracing::info!("Gracefully shutdown!");
                    Ok(())
                },
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    tracing::info!("Waited 10 seconds for graceful shutdown, aborting...");
                    Ok(())
                }
            }
        },
        addr,
    ))
}
