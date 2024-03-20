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
//! table = "my-test-table"
//! endpoint_url = "http://localhost:8000"
//! ```
//!
//! Be sure to *only* use non-production credentials for testing purposes. We
//! intentionally do not load the credentials from the environment to avoid
//! clobbering any existing S3 bucket.
//!
//! Run this test with:
//!   cargo test --test test_locks_dynamols --no-default-features
//!     --features dynamodb -- --show-output

mod common;

use common::GitRepo;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    default_region: String,
    table: String,
    endpoint_url: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn local_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let startup_span = common::startup();

    let config = match fs::read("tests/.test_credentials.toml") {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!(
                "Skipping test. No DynamoDB credentials available: {}",
                err
            );
            return Ok(());
        }
    };

    // Try to load dynamo credentials `.test_credentials.toml`. If they don't
    // exist, then we can't really run this test. Note that these should be
    // completely separate credentials than what is used in production.
    let creds: Credentials =
        toml::from_str(std::str::from_utf8(config.as_slice())?)?;

    std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key);
    std::env::set_var("AWS_DEFAULT_REGION", creds.default_region);

    let locks =
        lfs_rs::DynamoLs::new(creds.table.clone(), Some(&creds.endpoint_url))
            .await;
    GitRepo::setup_dynamodb_table(&creds.table, &creds.endpoint_url).await?;

    common::smoke_test(locks, Some(startup_span)).await
}
