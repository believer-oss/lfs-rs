// Some code in here is used in tests that aren't always built/run
#![allow(dead_code)]

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use base64::Engine;
use bytes::Bytes;
use futures::future::{Either, Future};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use lfs_rs::{
    CreateLockBatchRequest, LocalServerBuilder, LockBatchOuter, LockStorage,
    ReleaseLockBatchRequest, into_json,
};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::{Rng, RngExt};
use std::fs::{self, File};
use std::io;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::span::EnteredSpan;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[cfg(feature = "dynamodb")]
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, LocalSecondaryIndex, Projection,
    TableStatus,
};

#[cfg(feature = "otel")]
use opentelemetry_sdk::runtime;
#[cfg(feature = "otel")]
use tracing_subscriber::{Registry, prelude::*};

/// Bind test server to localhost port 0. We don't want this server to be
/// externally visible.
pub const SERVER_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>;

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn empty() -> BoxBody {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Runs `git` with a global config of our own, so that the developer's
/// `~/.gitconfig` (e.g. `lfs.storage` or `lfs.url`) can't change what the
/// tests do.
macro_rules! git {
    ($($arg:expr),* $(,)?) => {
        duct::cmd!("git", $($arg),*)
            .env("GIT_CONFIG_GLOBAL", git_config_global())
            .env("GIT_CONFIG_NOSYSTEM", "1")
    };
}

/// Returns the path of the global git config used by the tests. It only
/// registers the LFS filters, which `git clone` needs before any repo-local
/// config exists.
fn git_config_global() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();

    PATH.get_or_init(|| {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR"));
        let path = dir.join("gitconfig");

        // Written to a unique file and renamed into place, since several test
        // processes may do this at the same time.
        let tmp = dir.join(format!("gitconfig.{}", std::process::id()));
        fs::write(
            &tmp,
            concat!(
                "[filter \"lfs\"]\n",
                "\tclean = git-lfs clean -- %f\n",
                "\tsmudge = git-lfs smudge -- %f\n",
                "\tprocess = git-lfs filter-process\n",
                "\trequired = true\n",
                "[user]\n",
                "\tname = Foo Bar\n",
                "\temail = foobar@example.com\n",
            ),
        )
        .expect("failed to write test gitconfig");
        fs::rename(&tmp, &path).expect("failed to write test gitconfig");

        path
    })
}

/// Integration tests against S3 and DynamoDB are configured with these
/// environment variables, and skip when they are absent:
///
/// - `LFS_TEST_S3_BUCKET`, and `LFS_TEST_S3_ENDPOINT` for an S3-compatible
///   server. The bucket name must start with [`SCRATCH_PREFIX`]. The bucket is
///   created if an endpoint is given.
/// - `LFS_TEST_DYNAMODB_TABLE`, and `LFS_TEST_DYNAMODB_ENDPOINT` for DynamoDB
///   Local. This is the base of the table names; see [`dynamodb_target`].
///   Tables are deleted and recreated by the tests.
/// - `LFS_TEST_AWS_ACCESS_KEY_ID`, `LFS_TEST_AWS_SECRET_ACCESS_KEY`, and
///   optionally `LFS_TEST_AWS_SESSION_TOKEN` and `LFS_TEST_AWS_REGION` (default
///   `us-east-1`), used for both.
///
/// Credentials are deliberately not read from the usual `AWS_*` variables or
/// profiles, so that a test can't touch a production bucket or table by
/// accident.
///
/// Set `LFS_TEST_REQUIRED=1` to turn a skip into a failure. CI sets it, so the
/// tests can't pass by not running.
///
/// Every bucket and table the tests touch is named with [`SCRATCH_PREFIX`], so
/// a mistyped name can't reach a real one, and test credentials can be limited
/// to `lfs-rs-scratch-*` resources.
pub const SCRATCH_PREFIX: &str = "lfs-rs-scratch-";

fn test_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn skip<T>(test: &str, var: &str) -> Option<T> {
    assert!(
        std::env::var_os("LFS_TEST_REQUIRED").is_none(),
        "LFS_TEST_REQUIRED is set but {var} is not; the {test} test would \
         have skipped and reported success"
    );
    eprintln!("Skipping {test} test: {var} is not set");
    None
}

/// An AWS configuration built only from the `LFS_TEST_AWS_*` variables.
fn aws_config(endpoint: Option<String>) -> SdkConfig {
    let var = |name: &str| {
        test_var(name).unwrap_or_else(|| {
            panic!("{name} must be set to run the S3 and DynamoDB tests")
        })
    };
    let credentials = Credentials::new(
        var("LFS_TEST_AWS_ACCESS_KEY_ID"),
        var("LFS_TEST_AWS_SECRET_ACCESS_KEY"),
        test_var("LFS_TEST_AWS_SESSION_TOKEN"),
        None,
        "lfs-rs-tests",
    );
    let region =
        test_var("LFS_TEST_AWS_REGION").unwrap_or_else(|| "us-east-1".into());

    let mut config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new(region))
        .credentials_provider(SharedCredentialsProvider::new(credentials));
    if let Some(endpoint) = endpoint {
        config = config.endpoint_url(endpoint);
    }
    config.build()
}

pub struct S3Target {
    pub bucket: String,
    pub config: SdkConfig,
}

/// Returns the S3 bucket to test against, or `None` if the test should skip.
pub async fn s3_target(test: &str) -> Option<S3Target> {
    let Some(bucket) = test_var("LFS_TEST_S3_BUCKET") else {
        return skip(test, "LFS_TEST_S3_BUCKET");
    };
    // Bucket names are global and a real bucket must already exist, so this
    // can't add the prefix itself; it refuses anything else instead.
    assert!(
        bucket.starts_with(SCRATCH_PREFIX),
        "LFS_TEST_S3_BUCKET is '{bucket}', but test buckets must be named \
         '{SCRATCH_PREFIX}*' so that they can't be mistaken for real ones"
    );
    let endpoint = test_var("LFS_TEST_S3_ENDPOINT");
    let config = aws_config(endpoint.clone());

    // An S3-compatible server started for the tests starts out empty. Several
    // tests may get here at once, so a failed create is fine as long as the
    // bucket exists afterwards.
    if endpoint.is_some() {
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&config)
                .force_path_style(true)
                .build(),
        );
        if client.head_bucket().bucket(&bucket).send().await.is_err() {
            let _ = client.create_bucket().bucket(&bucket).send().await;
            client
                .head_bucket()
                .bucket(&bucket)
                .send()
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to create bucket '{bucket}': {err:?}")
                });
        }
    }

    Some(S3Target { bucket, config })
}

pub struct DynamoTarget {
    pub table: String,
    pub config: SdkConfig,
}

/// Returns the DynamoDB table to test against, or `None` if the test should
/// skip. Each test uses its own table, since the tables are recreated:
/// `lfs-rs-scratch-<LFS_TEST_DYNAMODB_TABLE>-<suffix>`. The prefix is always
/// applied, so the delete can't reach a table that isn't a scratch table.
pub fn dynamodb_target(test: &str, suffix: &str) -> Option<DynamoTarget> {
    let Some(table) = test_var("LFS_TEST_DYNAMODB_TABLE") else {
        return skip(test, "LFS_TEST_DYNAMODB_TABLE");
    };
    let base = table.strip_prefix(SCRATCH_PREFIX).unwrap_or(&table);
    let config = aws_config(test_var("LFS_TEST_DYNAMODB_ENDPOINT"));

    Some(DynamoTarget {
        table: format!("{SCRATCH_PREFIX}{base}-{suffix}"),
        config,
    })
}

/// A temporary git repository.
pub struct GitRepo {
    repo: tempfile::TempDir,
    lfs_server: Option<SocketAddr>,
    lfs_url: Option<String>,
}

impl GitRepo {
    /// Initialize a temporary synthetic git repository. It is set up to be
    /// connected to our LFS server.
    pub fn init(lfs_server: SocketAddr) -> io::Result<Self> {
        let repo = tempfile::TempDir::new()?;
        let path = repo.path();
        let lfs_url = format!("http://{}/api/test/test", lfs_server);

        git!("init", "--initial-branch=main", ".").dir(path).run()?;

        // Installs the hooks. The filters are in the test global config.
        git!("lfs", "install", "--local").dir(path).run()?;

        git!("remote", "add", "origin", "fake_remote")
            .dir(path)
            .run()?;
        git!("config", "lfs.url", &lfs_url,).dir(path).run()?;
        git!(
            "config",
            "lfs.storage",
            path.join(".git/lfs").to_str().unwrap()
        )
        .dir(path)
        .run()?;
        git!("config", "user.name", "Foo Bar").dir(path).run()?;
        git!("config", "user.email", "foobar@example.com")
            .dir(path)
            .run()?;
        git!("lfs", "track", "*.bin", "--lockable")
            .dir(path)
            .run()?;
        git!("add", ".gitattributes").dir(path).run()?;
        git!("commit", "-m", "Initial commit").dir(path).run()?;

        Ok(Self {
            repo,
            lfs_server: Some(lfs_server),
            lfs_url: Some(lfs_url),
        })
    }

    pub fn clone_repo(
        &self,
        lfs_server: Option<SocketAddr>,
    ) -> io::Result<Self> {
        let repo = tempfile::TempDir::new()?;
        let src_dir_str = self
            .repo
            .path()
            .to_str()
            .expect("could not convert src repo path to str");
        let dst_dir_str = repo
            .path()
            .to_str()
            .expect("could not convert src repo path to str");
        git!("clone", src_dir_str, dst_dir_str).run()?;

        let lfs_url = match lfs_server {
            Some(lfs_server) => {
                let url = format!("http://{}/api/test/test", lfs_server);
                git!("config", "lfs.url", url.clone())
                    .dir(dst_dir_str)
                    .run()?;
                Some(url)
            }
            None => None,
        };

        Ok(Self {
            repo,
            lfs_server,
            lfs_url,
        })
    }

    /// Adds a random file with the given size and random number generator. The
    /// file is also staged with `git add`.
    pub fn add_random<R: Rng>(
        &self,
        path: &Path,
        size: usize,
        rng: &mut R,
    ) -> io::Result<()> {
        let mut file = File::create(self.repo.path().join(path))?;
        gen_file(&mut file, size, rng)?;
        git!("add", path).dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Commits the currently staged files.
    pub fn commit(&self, message: &str) -> io::Result<()> {
        git!("commit", "-m", message).dir(self.repo.path()).run()?;
        Ok(())
    }

    pub fn lfs_push(&self) -> io::Result<()> {
        git!("lfs", "push", "origin", "main")
            .dir(self.repo.path())
            .run()?;
        Ok(())
    }

    pub fn lfs_pull(&self) -> io::Result<()> {
        git!("lfs", "pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    pub fn pull(&self) -> io::Result<()> {
        git!("pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Deletes all cached LFS files in `.git/lfs/`. This will force a
    /// re-download from the server.
    pub fn clean_lfs(&self) -> io::Result<()> {
        fs::remove_dir_all(self.repo.path().join(".git/lfs"))
    }

    /// Try to lock a file
    pub fn lock_file(&self, path: &Path) -> anyhow::Result<Output> {
        Ok(git!("lfs", "lock", path).dir(self.repo.path()).run()?)
    }

    /// Try to unlock a file
    pub fn unlock_file(
        &self,
        path: &Path,
        force: bool,
    ) -> anyhow::Result<Output> {
        match force {
            true => Ok(git!("lfs", "unlock", path, "--force")
                .dir(self.repo.path())
                .run()?),
            false => {
                Ok(git!("lfs", "unlock", path).dir(self.repo.path()).run()?)
            }
        }
    }

    async fn send_lock_request(
        uri: String,
        json: Bytes,
        user: u32,
        expected_failures: Vec<&str>,
    ) -> anyhow::Result<()> {
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("testuser{}:pass", user).as_bytes());

        let request = hyper::Request::post(uri)
            .header("authorization", format!("Basic {}", auth))
            .body(Full::new(json))
            .unwrap();

        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build_http();

        let resp = client.request(request).await?;
        let status = resp.status();

        let body_bytes = resp.into_body().collect().await?.to_bytes();

        assert!(
            status.is_success(),
            "response code {}: {}",
            status,
            String::from_utf8(body_bytes.to_vec()).unwrap_or_default()
        );

        let response: LockBatchOuter =
            serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(
            expected_failures.len(),
            response.batch.failures.len(),
            "Expected failures {:?} but got {:?}",
            expected_failures,
            response.batch.failures,
        );

        for expected in expected_failures.iter() {
            assert!(
                response.batch.failures.iter().any(|v| v.path.eq(expected)),
                "Failed to find failure {:?}",
                expected
            );
        }

        Ok(())
    }

    /// Lock a set of files in a batch operation
    pub async fn lock_files(
        &self,
        paths: &[String],
        user: u32,
        expected_failures: Vec<&str>,
    ) -> anyhow::Result<()> {
        let paths = paths.iter().map(|v| v.to_string()).collect();
        let json = into_json(&CreateLockBatchRequest { paths })?;
        let uri = format!("{}/locks/batch/lock", self.lfs_url.clone().unwrap());
        Self::send_lock_request(uri, json, user, expected_failures).await
    }

    /// Unlock a set of files in a batch operation
    pub async fn unlock_files(
        &self,
        paths: &[String],
        user: u32,
        force: bool,
        expected_failures: Vec<&str>,
    ) -> anyhow::Result<()> {
        let paths = paths.iter().map(|v| v.to_string()).collect();
        let json = into_json(&ReleaseLockBatchRequest {
            paths,
            force: Some(force),
        })?;
        let uri =
            format!("{}/locks/batch/unlock", self.lfs_url.clone().unwrap());
        Self::send_lock_request(uri, json, user, expected_failures).await
    }

    /// Try to list all locks
    pub fn get_locks(&self) -> io::Result<()> {
        git!("lfs", "locks").dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Try to verify all locks
    pub fn verify_locks(&self) -> io::Result<String> {
        let s = git!("lfs", "locks", "--verify")
            .dir(self.repo.path())
            .read()?;
        Ok(s)
    }

    pub fn add_auth_rewrite(&self, user: u32) -> io::Result<()> {
        if let Some(lfs_server) = self.lfs_server {
            git!(
                "config",
                format!(
                    "url.http://testuser{}:pass@{}/.insteadOf",
                    user, lfs_server
                ),
                format!("http://{}/", lfs_server)
            )
            .dir(self.repo.path())
            .run()?;
        }

        Ok(())
    }

    pub async fn setup_mock_gh_auth() -> MockServer {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/repos/test/test"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    r#"{"id": 1, "permissions":"#,
                    r#"{"admin":true,"maintain":true,"#,
                    r#""push":true,"triage":true,"pull":true}}"#,
                ),
                "application/json",
            ))
            .expect(1..)
            .named("setup_mock_gh_auth GET /repos/test/test")
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/user"))
            .and(header("Authorization", "Basic dGVzdHVzZXIxOnBhc3M="))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(
                    r#"{"login":"testuser1"}"#,
                    "application/json",
                ),
            )
            .expect(1..)
            .named("setup_mock_gh_auth GET /user auth user1")
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/user"))
            .and(header("Authorization", "Basic dGVzdHVzZXIyOnBhc3M="))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(
                    r#"{"login":"testuser2"}"#,
                    "application/json",
                ),
            )
            .expect(1..)
            .named("setup_mock_gh_auth GET /user auth user2")
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[cfg(feature = "dynamodb")]
    pub async fn setup_dynamodb_table(
        target: &DynamoTarget,
    ) -> anyhow::Result<()> {
        let table = target.table.as_str();
        // This deletes the table, so check again here however the target was
        // built.
        assert!(
            table.starts_with(SCRATCH_PREFIX),
            "refusing to recreate '{table}', which is not a scratch table"
        );
        let client = aws_sdk_dynamodb::Client::new(&target.config);
        match client.describe_table().table_name(table).send().await {
            Ok(_) => {
                tracing::debug!("Table {} already exists", table);
                client.delete_table().table_name(table).send().await?;

                // wait until the table has been deleted
                loop {
                    if (client.describe_table().table_name(table).send().await)
                        .is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
            Err(e) => {
                tracing::debug!("Table {} does not exist", table);
                tracing::debug!("Error: {:?}", e.into_source());
            }
        }
        let resp = client
            .create_table()
            .table_name(table)
            .set_attribute_definitions(Some(vec![
                AttributeDefinition::builder()
                    .attribute_name("repo")
                    .attribute_type("S".into())
                    .build()?,
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type("S".into())
                    .build()?,
                AttributeDefinition::builder()
                    .attribute_name("path")
                    .attribute_type("S".into())
                    .build()?,
                AttributeDefinition::builder()
                    .attribute_name("locked_at")
                    .attribute_type("S".into())
                    .build()?,
            ]))
            .set_key_schema(Some(vec![
                KeySchemaElement::builder()
                    .attribute_name("repo")
                    .key_type("HASH".into())
                    .build()?,
                KeySchemaElement::builder()
                    .attribute_name("path")
                    .key_type("RANGE".into())
                    .build()?,
            ]))
            .set_local_secondary_indexes(Some(vec![
                LocalSecondaryIndex::builder()
                    .index_name("id-index")
                    .set_key_schema(Some(vec![
                        KeySchemaElement::builder()
                            .attribute_name("repo")
                            .key_type("HASH".into())
                            .build()?,
                        KeySchemaElement::builder()
                            .attribute_name("id")
                            .key_type("RANGE".into())
                            .build()?,
                    ]))
                    .projection(
                        Projection::builder()
                            .projection_type("INCLUDE".into())
                            .non_key_attributes("path")
                            .non_key_attributes("locked_at")
                            .build(),
                    )
                    .build()?,
                LocalSecondaryIndex::builder()
                    .index_name("creation-index")
                    .set_key_schema(Some(vec![
                        KeySchemaElement::builder()
                            .attribute_name("repo")
                            .key_type("HASH".into())
                            .build()?,
                        KeySchemaElement::builder()
                            .attribute_name("locked_at")
                            .key_type("RANGE".into())
                            .build()?,
                    ]))
                    .projection(
                        Projection::builder()
                            .projection_type("INCLUDE".into())
                            .non_key_attributes("path")
                            .non_key_attributes("locked_at")
                            .non_key_attributes("owner")
                            .build(),
                    )
                    .build()?,
            ]))
            .billing_mode("PAY_PER_REQUEST".into())
            .send()
            .await;

        if let Err(e) = resp {
            return Err(anyhow::anyhow!(e));
        };

        // Wait until the table can be used. DynamoDB Local creates tables
        // immediately, but real DynamoDB takes a while.
        loop {
            let status = client
                .describe_table()
                .table_name(table)
                .send()
                .await
                .ok()
                .and_then(|resp| resp.table?.table_status);
            if status == Some(TableStatus::Active) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        Ok(())
    }
}

fn gen_file<W, R>(
    writer: &mut W,
    mut size: usize,
    rng: &mut R,
) -> io::Result<()>
where
    W: io::Write,
    R: Rng,
{
    let mut buf = [0u8; 4096];

    while size > 0 {
        let to_write = buf.len().min(size);

        let buf = &mut buf[..to_write];
        rng.fill(buf);
        writer.write_all(buf)?;

        size -= to_write;
    }

    Ok(())
}

#[cfg(not(feature = "otel"))]
/// Sets the default subscriber for the current thread until the guard is
/// dropped.
///
/// NOTE: Use `cargo test -- --nocapture` to see server logs.
pub fn init_logger() -> tracing::subscriber::DefaultGuard {
    let subscriber = tracing_subscriber::fmt().with_test_writer().finish();
    tracing::subscriber::set_default(subscriber)
}

#[cfg(feature = "otel")]
pub fn init_logger() -> tracing::subscriber::DefaultGuard {
    use opentelemetry::trace::TracerProvider as _;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .unwrap();

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_id_generator(
            opentelemetry_sdk::trace::RandomIdGenerator::default(),
        )
        .with_batch_exporter(exporter, runtime::Tokio)
        .build();

    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));

    let telemetry = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());
    let subscriber = Registry::default().with(telemetry);
    // ignore any errors if we fail to set the global default
    tracing::subscriber::set_default(subscriber)
}

pub fn startup() -> EnteredSpan {
    tracing::span!(tracing::Level::INFO, "test startup").entered()
}

/// Runs [`lock_smoke_test`] against local disk storage with the given locks.
pub async fn smoke_test(
    locks: impl LockStorage + Send + Sync + 'static,
    startup_span: Option<EnteredSpan>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = tempfile::TempDir::new()?;
    let key = Some(StdRng::seed_from_u64(42).random());

    let mock = GitRepo::setup_mock_gh_auth().await;

    let mut server = LocalServerBuilder::new(data.path().into(), key);
    server.authenticated(true);
    server.authentication_server(mock.uri());
    let (server, addr) = server.spawn(SERVER_ADDR, locks).await?;

    lock_smoke_test(server, addr, startup_span).await
}

/// Pushes, pulls and clones LFS objects through the running `server`, and
/// exercises locking as two users. The server must authenticate against
/// [`GitRepo::setup_mock_gh_auth`], which must outlive this call.
pub async fn lock_smoke_test<F, E>(
    server: F,
    addr: SocketAddr,
    startup_span: Option<EnteredSpan>,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error>> + Send + 'static,
{
    // Make sure our seed is deterministic. This makes it easier to reproduce
    // the same repo every time.
    let mut rng = StdRng::seed_from_u64(42);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(futures::future::select(shutdown_rx, server));

    if let Some(span) = startup_span {
        span.exit();
    }

    let repo = GitRepo::init(addr)?;
    {
        // We're interacting with locks, so fake authentication
        repo.add_auth_rewrite(1)?;

        repo.add_random(Path::new("4mb.bin"), 4 * 1024 * 1024, &mut rng)?;
        repo.add_random(Path::new("8mb.bin"), 8 * 1024 * 1024, &mut rng)?;
        repo.add_random(Path::new("16mb.bin"), 16 * 1024 * 1024, &mut rng)?;
        repo.commit("Add LFS objects")?;

        // Make sure we can push LFS objects to the server.
        repo.lfs_push()?;

        // Lock one of the new files
        repo.lock_file(Path::new("4mb.bin"))?;

        // This should be fast since we already have the data
        repo.lfs_pull()?;
    }

    // Make sure we can re-download the same objects in another repo
    let repo_clone = repo.clone_repo(repo.lfs_server).expect("unable to clone");
    {
        // We're interacting with locks, so fake authentication
        repo_clone.add_auth_rewrite(2)?;

        // This should be fast since the lfs data should come along properly
        // with the clone
        repo_clone.lfs_pull()?;

        // Try to take another lock, without forcing first. This should fail.
        let res = repo_clone.lock_file(Path::new("4mb.bin"));
        let error = res.unwrap_err().downcast::<std::io::Error>().unwrap();
        assert_eq!(error.kind(), ErrorKind::Other);

        repo_clone.get_locks()?;
        let verify_response = repo_clone.verify_locks()?;
        assert!(verify_response.contains("4mb.bin"));
        assert!(verify_response.contains("testuser1"));
        assert!(verify_response.contains("ID:"));

        // force-unlocking should work
        repo_clone.unlock_file(Path::new("4mb.bin"), true)?;

        repo_clone.lock_file(Path::new("4mb.bin"))?;

        repo_clone.pull()?;

        repo_clone.unlock_file(Path::new("4mb.bin"), true)?;

        // useful for testing auth caching
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            repo_clone.get_locks()?;
        }
    }

    // test batch API
    {
        let all_locks = vec![
            "4mb.bin".to_string(),
            "8mb.bin".to_string(),
            "16mb.bin".to_string(),
        ];

        repo.lock_files(&all_locks, 1, vec![]).await?;
        repo.unlock_files(&all_locks, 1, false, vec![]).await?;

        repo.lock_files(&all_locks, 1, vec![]).await?;
        repo.unlock_files(&all_locks, 1, false, vec![]).await?;

        repo.lock_file(Path::new("4mb.bin"))?;
        repo_clone.lock_file(Path::new("8mb.bin"))?;
        repo.lock_files(&all_locks, 1, vec!["4mb.bin", "8mb.bin"])
            .await?;
        repo.unlock_files(
            &[
                "4mb.bin".to_string(),
                "does".to_string(),
                "not".to_string(),
                "exist".to_string(),
            ],
            1,
            false,
            vec!["does", "not", "exist"],
        )
        .await?;

        // Should fail to unlock this since it was locked by testuser2
        repo.unlock_files(&["8mb.bin".to_string()], 1, false, vec!["8mb.bin"])
            .await?;

        // Force unlock should work
        repo.unlock_files(&["8mb.bin".to_string()], 1, true, vec![])
            .await?;

        // the 4mb file should have been unlocked in an earlier call
        repo.unlock_files(&all_locks, 1, false, vec!["4mb.bin", "8mb.bin"])
            .await?;

        // stresstest
        let mut stress_locks = vec![];
        for i in 0..500 {
            stress_locks.push(format!("stress_lock_{}", i));
        }

        let now = std::time::Instant::now();

        repo.lock_files(&stress_locks, 1, vec![]).await?;
        repo.unlock_files(&stress_locks, 1, false, vec![]).await?;

        let elapsed_secs = now.elapsed().as_secs_f32();
        if elapsed_secs > 10.0 {
            return Err(format!(
                "Batch stress test took too long. Max allowed is 10 seconds, \
                 but test finished in {} seconds.",
                elapsed_secs
            )
            .into());
        }
    }

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await? {
        // If the server exited first, then propagate the error.
        result.map_err(Into::into)?;
    }

    #[cfg(feature = "otel")]
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
