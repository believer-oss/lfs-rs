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
use std::time::Duration;

use clap::Parser;
use hex::FromHex;

use tracing_subscriber::filter::LevelFilter;

use lfs_rs::{Cache, LocalServerBuilder, S3ServerBuilder};
use lfs_rs::{LocalLs, NoneLs};

#[cfg(feature = "dynamodb")]
use lfs_rs::DynamoLs;
#[cfg(feature = "redis")]
use lfs_rs::RedisLs;

mod init_tracing;

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
    /// encrypted. Ignored with --cdn or --s3ta, where objects are transferred
    /// directly between clients and S3.
    #[clap(long = "key", value_parser = from_hex, env = "RUDOLFS_KEY")]
    key: Option<[u8; 32]>,

    /// Root directory of the object cache, for the s3 backend. If not
    /// specified, no local disk cache is used. The local backend refuses it.
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

    /// Logging level for lfs-rs itself, e.g. "debug". Without it, lfs-rs logs
    /// at the level RUST_LOG sets, or `info`. A RUST_LOG directive naming
    /// `lfs_rs` takes precedence over this.
    #[clap(long = "log-level", env = "RUDOLFS_LOG")]
    log_level: Option<LevelFilter>,

    /// Pass authorization header to GitHub to check permissions
    #[clap(long)]
    github_auth: bool,

    /// How often to log a summary of the server's activity at info level
    /// (requests, bytes transferred, cache hit rates). 0 turns it off.
    #[clap(
        long = "stats-interval",
        default_value = "1h",
        value_parser = humantime::parse_duration,
        env = "RUDOLFS_STATS_INTERVAL"
    )]
    stats_interval: Duration,

    /// How long to wait, on SIGTERM or SIGINT, for requests in flight to
    /// finish before exiting. An upload through the server can take many
    /// minutes, so under Kubernetes set this, and the pod's
    /// terminationGracePeriodSeconds, to allow for one.
    #[clap(
        long = "shutdown-timeout",
        default_value = "25s",
        value_parser = humantime::parse_duration,
        env = "RUDOLFS_SHUTDOWN_TIMEOUT"
    )]
    shutdown_timeout: Duration,
}

fn from_hex(s: &str) -> Result<[u8; 32], hex::FromHexError> {
    FromHex::from_hex(s)
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum LockBackend {
    /// Locks in DynamoDB.
    #[cfg(feature = "dynamodb")]
    #[value(name = "dynamodb", alias = "dynamo")]
    DynamoDB,

    /// Locks in Redis.
    #[cfg(feature = "redis")]
    #[value(name = "redis")]
    Redis,

    /// Locks in a file on the local disk.
    #[value(name = "local", alias = "localfs")]
    Local,

    /// No locking: the locking endpoints are not supported.
    #[value(name = "no-locks", aliases = ["none", "false"])]
    None,
}

#[derive(Parser, Debug)]
pub struct LockArgs {
    /// Locking backend to use
    #[clap(
        long = "lock-backend",
        default_value = "no-locks",
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
    /// locks. Not implemented yet: locks never expire.
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

    /// Amazon S3 path prefix to use: `<prefix>/<namespace>/<oid>`. Passing an
    /// empty string omits the prefix: `<namespace>/<oid>`.
    #[clap(long, default_value = "lfs", env = "RUDOLFS_S3_PREFIX")]
    prefix: String,

    /// The base URL of your CDN. If specified, then all download URLs will be
    /// prefixed with this URL.
    #[clap(long = "cdn", env = "RUDOLFS_S3_CDN")]
    cdn: Option<String>,

    /// Use AWS S3 Transfer Acceleration endpoints. The endpoint must have
    /// transfer acceleration enabled.
    #[clap(long = "s3ta", env = "RUDOLFS_S3TA")]
    s3_accelerate: bool,

    /// Maximum number of entries in the S3 size cache. Set to 0 to disable
    /// size caching. The cache stores object sizes to avoid repeated HEAD
    /// requests.
    #[clap(
        long = "s3-size-cache-entries",
        default_value = "256000",
        env = "RUDOLFS_S3_SIZE_CACHE_ENTRIES"
    )]
    size_cache_entries: usize,

    /// How long before they expire to refresh the AWS credentials used for
    /// S3, so that presigned URLs (with --s3ta) are valid for their full 15
    /// minutes. The AWS SDK refreshes somewhere between half this and all of
    /// it, so it should be at least 30 minutes. Set to 0 to keep the SDK's
    /// default (10s) where the credential source serves the same credentials
    /// until they nearly expire, like EC2 instance and ECS task roles.
    #[clap(
        long = "s3-credential-refresh-buffer",
        default_value = "35m",
        value_parser = humantime::parse_duration,
        env = "RUDOLFS_S3_CREDENTIAL_REFRESH_BUFFER"
    )]
    credential_refresh_buffer: Duration,
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
        tracing::info!("Starting server...");

        if !self.global.stats_interval.is_zero() {
            tokio::spawn(lfs_rs::stats::log_every(self.global.stats_interval));
        }

        // Find a socket address to bind to. This will resolve domain names.
        let addr = match self.global.host {
            Some(ref host) => host
                .to_socket_addrs()?
                .next()
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 8080))),
            None => SocketAddr::from(([0, 0, 0, 0], self.global.port)),
        };

        tracing::info!("Initializing storage...");

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
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
        lock: LockArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = S3ServerBuilder::new(self.bucket, global_args.key);
        builder.prefix(self.prefix);
        builder.authenticated(global_args.github_auth);
        builder.shutdown_timeout(global_args.shutdown_timeout);
        builder.size_cache_entries(self.size_cache_entries);
        builder.credential_refresh_buffer(self.credential_refresh_buffer);

        if let Some(cdn) = self.cdn {
            builder.cdn(cdn);
        }

        if self.s3_accelerate {
            builder.s3_accelerate(self.s3_accelerate);
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
    async fn run(
        self,
        addr: SocketAddr,
        global_args: GlobalArgs,
        lock: LockArgs,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Local storage has no disk cache in front of it; say so, rather than
        // run without the cache that was asked for.
        if global_args.cache_dir.is_some() {
            return Err(
                "--cache-dir is only supported with the s3 backend".into()
            );
        }

        let mut builder = LocalServerBuilder::new(self.path, global_args.key);

        builder.authenticated(global_args.github_auth);
        builder.shutdown_timeout(global_args.shutdown_timeout);

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
    let args = Args::parse();

    // Set up here rather than in `Args::main` so that the guard outlives the
    // error logged below, and flushes it to the exporter.
    let guard = match init_tracing::setup_tracing(args.global.log_level) {
        Ok(guard) => guard,
        Err(err) => {
            eprintln!("Failed to set up logging: {err}");
            std::process::exit(1);
        }
    };

    let exit_code = if let Err(err) = args.main().await {
        // Include the causes: the outermost error is usually just context,
        // such as which bucket we failed to reach, and not why.
        let mut message = err.to_string();
        let mut source = err.source();
        while let Some(cause) = source {
            message.push_str(&format!(": {cause}"));
            source = cause.source();
        }
        tracing::error!("{message}");
        1
    } else {
        0
    };

    // `exit` skips destructors, so flush explicitly.
    drop(guard);
    std::process::exit(exit_code);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `--lock-backend value`, with every backend's required argument.
    fn lock_backend(value: &str) -> Result<LockBackend, clap::Error> {
        let mut args = vec!["lfs-rs", "--lock-backend", value];
        args.extend(["--lock-path", "/tmp/locks.json"]);
        #[cfg(feature = "dynamodb")]
        args.extend(["--lock-dynamodb-table", "locks"]);
        #[cfg(feature = "redis")]
        args.extend(["--lock-redis-uri", "redis://localhost"]);
        args.extend(["local", "--path", "/tmp/lfs"]);

        let args = Args::try_parse_from(args)?;
        Ok(args.lock_args.lock_backend)
    }

    #[test]
    fn unknown_lock_backends_are_usage_errors() {
        let err = lock_backend("bogus").expect_err("bogus was accepted");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn lock_backend_names_and_aliases() {
        for value in ["local", "localfs"] {
            assert!(matches!(lock_backend(value).unwrap(), LockBackend::Local));
        }
        for value in ["no-locks", "none", "false"] {
            assert!(matches!(lock_backend(value).unwrap(), LockBackend::None));
        }
        #[cfg(feature = "dynamodb")]
        for value in ["dynamodb", "dynamo"] {
            assert!(matches!(
                lock_backend(value).unwrap(),
                LockBackend::DynamoDB
            ));
        }
        #[cfg(feature = "redis")]
        assert!(matches!(lock_backend("redis").unwrap(), LockBackend::Redis));
    }
}
