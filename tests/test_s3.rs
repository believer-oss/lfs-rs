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

//! This integration test only runs if the file `.test_credentials.toml` is in
//! the same directory. Otherwise, it succeeds and does nothing.
//!
//! To run this test, create `tests/.test_credentials.toml` with the following
//! contents:
//!
//! ```toml
//! access_key_id = "XXXXXXXXXXXXXXXXXXXX"
//! secret_access_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
//! default_region = "us-east-1"
//! bucket = "my-test-bucket"
//! ```
//!
//! Be sure to *only* use non-production credentials for testing purposes. We
//! intentionally do not load the credentials from the environment to avoid
//! clobbering any existing S3 bucket.

mod common;

use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use futures::future::Either;
use lfs_rs::S3ServerBuilder;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use common::{init_logger, GitRepo};

#[derive(Debug, Serialize, Deserialize)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    default_region: String,
    bucket: String,
    #[serde(default)]
    s3ta_enabled: bool,
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();

    let config = match fs::read("tests/.test_credentials.toml") {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Skipping test. No S3 credentials available: {}", err);
            return Ok(());
        }
    };

    // Try to load S3 credentials `.test_credentials.toml`. If they don't exist,
    // then we can't really run this test. Note that these should be completely
    // separate credentials than what is used in production.
    let creds: Credentials =
        toml::from_str(std::str::from_utf8(config.as_slice())?)?;

    std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key);
    std::env::set_var(
        "AWS_SESSION_TOKEN",
        creds.session_token.unwrap_or_default(),
    );
    std::env::set_var("AWS_DEFAULT_REGION", creds.default_region);

    // Make sure our seed is deterministic. This prevents us from filling up our
    // S3 bucket with a bunch of random files if this test gets ran a bunch of
    // times.
    let mut rng = StdRng::seed_from_u64(42);

    let key = rng.gen();

    let mut server = S3ServerBuilder::new(creds.bucket, key);
    server.prefix("test_lfs".into());

    let locks = lfs_rs::NoneLs::new();

    let (server, addr) = server
        .spawn(SocketAddr::from(([0, 0, 0, 0], 0)), locks)
        .await?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(futures::future::select(shutdown_rx, server));

    let repo = GitRepo::init(addr)?;
    repo.add_random(Path::new("4mb.bin"), 4 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("8mb.bin"), 8 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("16mb.bin"), 16 * 1024 * 1024, &mut rng)?;
    repo.commit("Add LFS objects")?;

    // Make sure we can push LFS objects to the server.
    repo.lfs_push().unwrap();

    // Make sure we can re-download the same objects.
    repo.clean_lfs().unwrap();
    repo.lfs_pull().unwrap();

    // Push again. This should be super fast.
    repo.lfs_push().unwrap();

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await.unwrap() {
        // If the server exited first, then propagate the error.
        result.expect("server failed unexpectedly");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn s3ta_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();

    let config = match fs::read("tests/.test_credentials.toml") {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Skipping test. No S3 credentials available: {}", err);
            return Ok(());
        }
    };

    // Try to load S3 credentials `.test_credentials.toml`. If they don't exist,
    // then we can't really run this test. Note that these should be completely
    // separate credentials than what is used in production.
    let creds: Credentials =
        toml::from_str(std::str::from_utf8(config.as_slice())?)?;

    if creds.s3ta_enabled {
        eprintln!(
            "Skipping test. S3 Transfer Acceleration is required and not set \
             in test credentials config."
        );
        return Ok(());
    }

    std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key);
    std::env::set_var(
        "AWS_SESSION_TOKEN",
        creds.session_token.unwrap_or_default(),
    );
    std::env::set_var("AWS_DEFAULT_REGION", creds.default_region);

    // Make sure our seed is deterministic. This prevents us from filling up our
    // S3 bucket with a bunch of random files if this test gets ran a bunch of
    // times.
    let mut rng = StdRng::seed_from_u64(42);

    let key = rng.gen();

    let mut server = S3ServerBuilder::new(creds.bucket, key);
    server.s3_accelerate(true);
    server.prefix("test_lfs".into());

    let locks = lfs_rs::NoneLs::new();

    let (server, addr) = server
        .spawn(SocketAddr::from(([0, 0, 0, 0], 0)), locks)
        .await?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(futures::future::select(shutdown_rx, server));

    let repo = GitRepo::init(addr)?;
    repo.add_random(Path::new("4mb.bin"), 4 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("8mb.bin"), 8 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("16mb.bin"), 16 * 1024 * 1024, &mut rng)?;
    repo.commit("Add LFS objects")?;

    // Make sure we can push LFS objects to the server.
    repo.lfs_push().unwrap();

    // Make sure we can re-download the same objects.
    repo.clean_lfs().unwrap();
    repo.lfs_pull().unwrap();

    // Push again. This should be super fast.
    repo.lfs_push().unwrap();

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await.unwrap() {
        // If the server exited first, then propagate the error.
        result.expect("server failed unexpectedly");
    }

    Ok(())
}
