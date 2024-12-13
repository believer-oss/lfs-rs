// Some code in here is used in tests that aren't always built/run
#![allow(dead_code)]

// mod common;
use base64::Engine;
use duct::cmd;
use futures::future::Either;
use lfs_rs::{
    into_json, CreateLockBatchRequest, LocalServerBuilder, LockBatchOuter,
    LockStorage, ReleaseLockBatchRequest, Server,
};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::fs::{self, File};
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Output;
use tokio::sync::oneshot;
use tracing::span::EnteredSpan;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[cfg(feature = "dynamodb")]
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, LocalSecondaryIndex, Projection,
};

#[cfg(feature = "otel")]
use opentelemetry_sdk::runtime;
#[cfg(feature = "otel")]
use tracing_subscriber::{prelude::*, Registry};

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

        cmd!("git", "init", "--initial-branch=main", ".")
            .dir(path)
            .run()?;
        cmd!("git", "lfs", "install").dir(path).run()?;
        cmd!("git", "remote", "add", "origin", "fake_remote")
            .dir(path)
            .run()?;
        cmd!("git", "config", "lfs.url", &lfs_url,)
            .dir(path)
            .run()?;
        cmd!("git", "config", "user.name", "Foo Bar")
            .dir(path)
            .run()?;
        cmd!("git", "config", "user.email", "foobar@example.com")
            .dir(path)
            .run()?;
        cmd!("git", "lfs", "track", "*.bin", "--lockable")
            .dir(path)
            .run()?;
        cmd!("git", "add", ".gitattributes").dir(path).run()?;
        cmd!("git", "commit", "-m", "Initial commit")
            .dir(path)
            .run()?;

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
        cmd!("git", "clone", src_dir_str, dst_dir_str).run()?;

        let lfs_url = match lfs_server {
            Some(lfs_server) => {
                let url = format!("http://{}/api/test/test", lfs_server);
                cmd!("git", "config", "lfs.url", url.clone())
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
        cmd!("git", "add", path).dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Commits the currently staged files.
    pub fn commit(&self, message: &str) -> io::Result<()> {
        cmd!("git", "commit", "-m", message)
            .dir(self.repo.path())
            .run()?;
        Ok(())
    }

    pub fn lfs_push(&self) -> io::Result<()> {
        cmd!("git", "lfs", "push", "origin", "main")
            .dir(self.repo.path())
            .run()?;
        Ok(())
    }

    pub fn lfs_pull(&self) -> io::Result<()> {
        cmd!("git", "lfs", "pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    pub fn pull(&self) -> io::Result<()> {
        cmd!("git", "pull").dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Deletes all cached LFS files in `.git/lfs/`. This will force a
    /// re-download from the server.
    pub fn clean_lfs(&self) -> io::Result<()> {
        fs::remove_dir_all(self.repo.path().join(".git/lfs"))
    }

    /// Try to lock a file
    pub fn lock_file(&self, path: &Path) -> anyhow::Result<Output> {
        Ok(cmd!("git", "lfs", "lock", path)
            .dir(self.repo.path())
            .run()?)
    }

    /// Try to unlock a file
    pub fn unlock_file(
        &self,
        path: &Path,
        force: bool,
    ) -> anyhow::Result<Output> {
        match force {
            true => Ok(cmd!("git", "lfs", "unlock", path, "--force")
                .dir(self.repo.path())
                .run()?),
            false => Ok(cmd!("git", "lfs", "unlock", path)
                .dir(self.repo.path())
                .run()?),
        }
    }

    async fn send_lock_request(
        uri: String,
        json: hyper::body::Body,
        user: u32,
        expected_failures: Vec<&str>,
    ) -> anyhow::Result<()> {
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("testuser{}:pass", user).as_bytes());

        let request = hyper::Request::post(uri)
            .header("authorization", format!("Basic {}", auth))
            .body(json)
            .unwrap();

        let client = hyper::Client::new();
        let resp = client.request(request).await?;
        let status = resp.status();

        let body_bytes = hyper::body::to_bytes(resp.into_body()).await?;

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
        cmd!("git", "lfs", "locks").dir(self.repo.path()).run()?;
        Ok(())
    }

    /// Try to verify all locks
    pub fn verify_locks(&self) -> io::Result<String> {
        let s = cmd!("git", "lfs", "locks", "--verify")
            .dir(self.repo.path())
            .read()?;
        Ok(s)
    }

    pub fn add_auth_rewrite(&self, user: u32) -> io::Result<()> {
        if let Some(lfs_server) = self.lfs_server {
            cmd!(
                "git",
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
        table: &str,
        endpoint_url: &str,
    ) -> anyhow::Result<()> {
        let shared_config =
            aws_config::defaults(aws_config::BehaviorVersion::v2024_03_28());
        let shared_config = shared_config.endpoint_url(endpoint_url);
        let sdk_config = shared_config.load().await;

        let client = aws_sdk_dynamodb::Client::new(&sdk_config);
        match client.describe_table().table_name(table).send().await {
            Ok(_) => {
                log::debug!("Table {} already exists", table);
                client.delete_table().table_name(table).send().await?;

                // wait until the table has been deleted
                loop {
                    if (client.describe_table().table_name(table).send().await)
                        .is_err()
                    {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }
            Err(e) => {
                log::debug!("Table {} does not exist", table);
                log::debug!("Error: {:?}", e.into_source());
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

        // wait until the table is finished being created
        loop {
            if (client.describe_table().table_name(table).send().await).is_ok()
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }

        std::thread::sleep(std::time::Duration::from_millis(5000));

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
pub fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_module("rudolfs", log::LevelFilter::max())
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[cfg(feature = "otel")]
pub fn init_logger() {
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
    Registry::default().with(telemetry).init();
}

pub fn startup() -> EnteredSpan {
    init_logger();
    tracing::span!(tracing::Level::INFO, "test startup").entered()
}

pub async fn smoke_test(
    locks: impl LockStorage + Send + Sync + 'static,
    startup_span: Option<EnteredSpan>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Make sure our seed is deterministic. This makes it easier to reproduce
    // the same repo every time.
    let mut rng = StdRng::seed_from_u64(42);

    let data = tempfile::TempDir::new()?;
    let key = rng.gen();

    let mock = GitRepo::setup_mock_gh_auth().await;

    let mut server = LocalServerBuilder::new(data.path().into(), key);
    server.authenticated(true);
    server.authentication_server(mock.uri());
    let server = server
        .spawn(SocketAddr::from(([127, 0, 0, 1], 0)), locks)
        .await?;
    let addr = server.addr();

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
        if elapsed_secs > 5.0 {
            return Err(format!(
                "Batch stress test took too long. Max allowed is 5 seconds, \
                 but test finished in {} seconds.",
                elapsed_secs
            )
            .into());
        }
    }

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await? {
        // If the server exited first, then propagate the error.
        result?;
    }

    #[cfg(feature = "otel")]
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
