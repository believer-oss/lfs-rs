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
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use clap::Parser;
use hex::FromHex;

use lfs_rs::{Cache, LocalServerBuilder, S3ServerBuilder};
use lfs_rs::{LocalLs, NoneLs};

#[cfg(feature = "dynamodb")]
use lfs_rs::DynamoLs;
#[cfg(feature = "redis")]
use lfs_rs::RedisLs;

#[cfg(feature = "otel")]
mod init_tracing;
#[cfg(feature = "otel")]
use init_tracing::setup_tracing;
#[cfg(feature = "otel")]
use tracing::{field, instrument, span};

// Additional help to append to the end when `--help` is specified.
static AFTER_HELP: &str = include_str!("help.md");

#[derive(Parser)]
#[clap(after_help = AFTER_HELP)]
struct Args {
    #[clap(flatten)]
    global: GlobalArgs,

    #[clap(flatten)]
    lock_args: LockArgs,

    #[clap(subcommand)]
    backend: Backend,
}

#[derive(Parser)]
enum Backend {
    /// Starts the server with S3 as the storage backend.
    #[clap(name = "s3")]
    S3(S3Args),

    /// Starts the server with the local disk as the storage backend.
    #[clap(name = "local")]
    Local(LocalArgs),
}

#[derive(Parser, Debug)]
struct GlobalArgs {
    /// The host or address to listen on. If this is not specified, then
    /// `0.0.0.0` is used where the port can be specified with `--port`
    /// (port 8080 is used by default if that is also not specified).
    #[clap(long = "host", env = "RUDOLFS_HOST")]
    host: Option<String>,

    /// The port to bind to. This is only used if `--host` is not specified.
    #[clap(long = "port", default_value = "8080", env = "PORT")]
    port: u16,

    /// Encryption key to use. If not specified, then objects are *not*
    /// encrypted.
    #[clap(long = "key", value_parser = from_hex, env = "RUDOLFS_KEY")]
    key: Option<[u8; 32]>,

    /// Root directory of the object cache. If not specified or if the local
    /// disk is the storage backend, then no local disk cache will be used.
    #[clap(long = "cache-dir", env = "RUDOLFS_CACHE_DIR")]
    cache_dir: Option<PathBuf>,

    /// Maximum size of the cache, in bytes. Set to 0 for an unlimited cache
    /// size.
    #[clap(
        long = "max-cache-size",
        default_value = "50 GiB",
        env = "RUDOLFS_MAX_CACHE_SIZE"
    )]
    max_cache_size: human_size::Size,

    /// Logging level to use.
    #[clap(long = "log-level", default_value = "info", env = "RUDOLFS_LOG")]
    log_level: log::LevelFilter,

    /// Pass authorization header to GitHub to check permissions
    #[clap(long)]
    github_auth: bool,
}

fn from_hex(s: &str) -> Result<[u8; 32], hex::FromHexError> {
    FromHex::from_hex(s)
}

#[derive(Parser, Clone, Debug)]
enum LockBackend {
    /// Starts the server with DynamoDB as the lock backend.
    #[cfg(feature = "dynamodb")]
    #[clap(name = "dynamodb")]
    DynamoDB,

    /// Starts the server with DynamoDB as the lock backend.
    #[cfg(feature = "redis")]
    #[clap(name = "redis")]
    Redis,

    /// Starts the server with the local disk as the lock backend.
    #[clap(name = "local")]
    Local,

    /// Default to not supporting locking endpoints
    #[clap(name = "no-locks")]
    None,
}

impl From<&str> for LockBackend {
    fn from(value: &str) -> Self {
        match value {
            "local" | "localfs" => LockBackend::Local,
            #[cfg(feature = "dynamodb")]
            "dynamo" | "dynamodb" => LockBackend::DynamoDB,
            #[cfg(feature = "redis")]
            "redis" => LockBackend::Redis,
            "none" | "no-locks" | "false" => LockBackend::None,
            _ => panic!("Could not parse lock-backend!"),
        }
    }
}

#[derive(Parser, Debug)]
pub struct LockArgs {
    /// Locking backend to use
    #[clap(
        long = "lock-backend",
        default_value = "no-locks",
        value_parser = clap::value_parser!(LockBackend),
        env = "RUDOLFS_LOCK_BACKEND"
    )]
    lock_backend: LockBackend,

    /// If the --lock-backend is set to local, the root directory of the lock
    /// storage.
    #[clap(
        long = "lock-path",
        env = "RUDOLFS_LOCK_PATH",
        required_if_eq_any([
            ("lock_backend", "local"),
            ("lock_backend", "localfs"),
        ])
    )]
    local_lock_path: Option<PathBuf>,

    /// If the --lock-backend is set to redis, the uri to use for lock storage.
    #[cfg(feature = "redis")]
    #[clap(
        long = "lock-redis-uri",
        env = "RUDOLFS_LOCK_REDIS_URI",
        required_if_eq("lock_backend", "redis")
    )]
    redis_uri: Option<String>,

    /// If the --lock-backend is set to redis, the default ttl to use for
    /// locks. FIXME: Not implemented
    #[cfg(feature = "redis")]
    #[clap(long = "lock-redis-ttl", env = "RUDOLFS_LOCK_REDIS_TTL")]
    redis_ttl: Option<usize>,

    /// If the --lock-backend is set to dynamodb, the table name
    #[cfg(feature = "dynamodb")]
    #[clap(
        long = "lock-dynamodb-table",
        env = "RUDOLFS_LOCK_DYNAMODB_TABLE",
        required_if_eq("lock_backend", "dynamodb")
    )]
    dynamodb_table: Option<String>,
}

#[derive(Parser, Debug)]
struct S3Args {
    /// Amazon S3 bucket to use.
    #[clap(long, env = "RUDOLFS_S3_BUCKET")]
    bucket: String,

    /// Amazon S3 path prefix to use.
    #[clap(long, default_value = "lfs", env = "RUDOLFS_S3_PREFIX")]
    prefix: String,

    /// The base URL of your CDN. If specified, then all download URLs will be
    /// prefixed with this URL.
    #[clap(long = "cdn", env = "RUDOLFS_S3_CDN")]
    cdn: Option<String>,
}

#[derive(Parser)]
struct LocalArgs {
    /// Directory where the LFS files should be stored. This directory will be
    /// created if it does not exist.
    #[clap(long, env = "RUDOLFS_LOCAL_PATH")]
    path: PathBuf,
}

impl Args {
    async fn main(self) -> Result<(), Box<dyn std::error::Error>> {
        // Initialize logging.
        let mut logger_builder = pretty_env_logger::formatted_timed_builder();
        logger_builder.filter_module("rudolfs", self.global.log_level);

        if let Ok(env) = std::env::var("RUST_LOG") {
            // Support the addition of RUST_LOG to help with debugging
            // dependencies, such as Hyper.
            logger_builder.parse_filters(&env);
        }

        #[cfg(not(feature = "otel"))]
        logger_builder.init();
        #[cfg(feature = "otel")]
        let _guard = setup_tracing(self.global.log_level);

        #[cfg(feature = "otel")]
        let server_span =
            span!(tracing::Level::INFO, "server", local_addr = field::Empty);

        log::info!("Starting server...");

        // Find a socket address to bind to. This will resolve domain names.
        let addr = match self.global.host {
            Some(ref host) => host
                .to_socket_addrs()?
                .next()
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 8080))),
            None => SocketAddr::from(([0, 0, 0, 0], self.global.port)),
        };

        #[cfg(feature = "otel")]
        server_span.record("local_addr", addr.to_string());

        log::info!("Initializing storage...");

        match self.backend {
            Backend::S3(s3) => {
                s3.run(addr, self.global, self.lock_args).await?
            }
            Backend::Local(local) => {
                local.run(addr, self.global, self.lock_args).await?
            }
        }

        Ok(())
    }
}

impl S3Args {
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", name = "s3args.run")
    )]
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
        lock: LockArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = S3ServerBuilder::new(self.bucket, global_args.key);
        builder.prefix(self.prefix);
        builder.authenticated(global_args.github_auth);

        if let Some(cdn) = self.cdn {
            builder.cdn(cdn);
        }

        if let Some(cache_dir) = global_args.cache_dir {
            let max_cache_size = global_args
                .max_cache_size
                .into::<human_size::Byte>()
                .value() as u64;
            builder.cache(Cache::new(cache_dir, max_cache_size));
        }

        match lock.lock_backend {
            #[cfg(feature = "dynamodb")]
            LockBackend::DynamoDB => {
                let table_name = lock.dynamodb_table.clone().unwrap();
                builder
                    .run(addr, DynamoLs::new(table_name, None).await)
                    .await
            }
            #[cfg(feature = "redis")]
            LockBackend::Redis => {
                let uri = lock.redis_uri.clone().unwrap();
                let ttl = lock.redis_ttl.unwrap_or(0);
                let locks = RedisLs::new(&uri, ttl).await?;
                builder.run(addr, locks).await
            }
            LockBackend::Local => {
                let path = lock.local_lock_path.clone().unwrap();
                let locks = LocalLs::new(path).await?;
                builder.run(addr, locks).await
            }
            LockBackend::None => builder.run(addr, NoneLs::new()).await,
        }
    }
}

impl LocalArgs {
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(self), name = "http.request")
    )]
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
        lock: LockArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = LocalServerBuilder::new(self.path, global_args.key);

        builder.authenticated(global_args.github_auth);

        if let Some(cache_dir) = global_args.cache_dir {
            let max_cache_size = global_args
                .max_cache_size
                .into::<human_size::Byte>()
                .value() as u64;
            builder.cache(Cache::new(cache_dir, max_cache_size));
        }

        match lock.lock_backend {
            #[cfg(feature = "dynamodb")]
            LockBackend::DynamoDB => {
                let table_name = lock.dynamodb_table.clone().unwrap();
                builder
                    .run(addr, DynamoLs::new(table_name, None).await)
                    .await
            }
            #[cfg(feature = "redis")]
            LockBackend::Redis => {
                let uri = lock.redis_uri.clone().unwrap();
                let ttl = lock.redis_ttl.unwrap_or(0);
                let locks = RedisLs::new(&uri, ttl).await?;
                builder.run(addr, locks).await
            }
            LockBackend::Local => {
                let path = lock.local_lock_path.clone().unwrap();
                let locks = LocalLs::new(path).await?;
                builder.run(addr, locks).await
            }
            LockBackend::None => builder.run(addr, NoneLs::new()).await,
        }
    }
}

#[tokio::main]
async fn main() {
    let exit_code = if let Err(err) = Args::parse().main().await {
        log::error!("{}", err);
        1
    } else {
        0
    };

    std::process::exit(exit_code);
}
