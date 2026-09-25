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

//! Locking against DynamoDB or DynamoDB Local. This skips unless
//! `LFS_TEST_DYNAMODB_TABLE` is set; see `tests/common.rs` for the
//! configuration.
//!
//! Be sure to *only* use non-production credentials and tables for testing
//! purposes: the table is deleted and recreated.
#![cfg(feature = "dynamodb")]

mod common;

use common::GitRepo;

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = common::init_logger();
    let startup_span = common::startup();

    let Some(target) = common::dynamodb_target("DynamoDB locks", "locks")
    else {
        return Ok(());
    };

    GitRepo::setup_dynamodb_table(&target).await?;
    let locks = lfs_rs::DynamoLs::from_config(&target.config, target.table);

    common::smoke_test(locks, Some(startup_span)).await
}
