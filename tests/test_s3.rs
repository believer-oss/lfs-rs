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
use std::path::Path;

use futures::future::Either;
use lfs_rs::S3ServerBuilder;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use common::{init_logger, GitRepo, SERVER_ADDR};

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
    let _guard = init_logger();

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

    let (server, addr) = server.spawn(SERVER_ADDR, locks).await?;

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
    let _guard = init_logger();

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

    let (server, addr) = server.spawn(SERVER_ADDR, locks).await?;

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
async fn s3_size_cache_test() -> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::storage::{Storage, StorageKey};
    use lfs_rs::Oid;

    let _guard = init_logger();

    let config = match fs::read("tests/.test_credentials.toml") {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Skipping test. No S3 credentials available: {}", err);
            return Ok(());
        }
    };

    let creds: Credentials =
        toml::from_str(std::str::from_utf8(config.as_slice())?)?;

    std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key);
    std::env::set_var(
        "AWS_SESSION_TOKEN",
        creds.session_token.unwrap_or_default(),
    );
    std::env::set_var("AWS_DEFAULT_REGION", creds.default_region);

    // Create S3 backend with a very small cache (3 entries) to test eviction
    let backend = lfs_rs::storage::S3::new(
        creds.bucket,
        "test_lfs_cache".to_string(),
        None,
        false,
        3, // Small cache size for testing
    )
    .await?;

    let mut rng = StdRng::seed_from_u64(123);
    let namespace = lfs_rs::storage::Namespace::new(
        "test".to_string(),
        "cache-test".to_string(),
    );

    // Upload 5 test objects to S3
    let mut keys = Vec::new();
    let mut sizes = Vec::new();
    for i in 0..5 {
        let size = (i + 1) * 1024; // 1KB, 2KB, 3KB, 4KB, 5KB
        let mut data = vec![0u8; size];
        rng.fill(&mut data[..]);

        // Hash the data to create an Oid
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&data);
        let hash_result = hasher.finalize();
        let oid = Oid::from(hash_result);

        let key = StorageKey::new(namespace.clone(), oid);

        // Upload the object
        let stream = Box::pin(futures::stream::once(async move {
            Ok(bytes::Bytes::from(data))
        }));
        let obj = lfs_rs::storage::LFSObject::new(size as u64, stream);
        backend.put(key.clone(), obj).await?;

        keys.push(key);
        sizes.push(size as u64);
    }

    eprintln!("✓ Uploaded 5 test objects");

    // Test 1: First access should populate cache (3 misses)
    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (0, 0, 0),
        "Cache should start empty"
    );

    for (key, expected_size) in keys.iter().zip(sizes.iter()).take(3) {
        let size = backend.size(key).await?;
        assert_eq!(
            size,
            Some(*expected_size),
            "Size mismatch for {}",
            key.oid()
        );
    }

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (0, 3, 3),
        "Should have 3 misses, cache full"
    );
    eprintln!("✓ Test 1: Cache populated with 3 entries (3 misses)");

    // Test 2: Access the same 3 keys again - should hit cache (3 hits)
    for (key, expected_size) in keys.iter().zip(sizes.iter()).take(3) {
        let size = backend.size(key).await?;
        assert_eq!(
            size,
            Some(*expected_size),
            "Cached size mismatch for {}",
            key.oid()
        );
    }

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (3, 3, 3),
        "Should have 3 hits total"
    );
    eprintln!("✓ Test 2: Cache hits work correctly (3 hits, 3 misses total)");

    // Test 3: Access keys 4 and 5, which should evict keys 1 and 2 (LRU)
    // This adds 2 new entries (2 more misses)
    for (key, expected_size) in keys.iter().zip(sizes.iter()).skip(3).take(2) {
        let size = backend.size(key).await?;
        assert_eq!(
            size,
            Some(*expected_size),
            "Size mismatch for {}",
            key.oid()
        );
    }

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (3, 5, 3),
        "Should have 2 more misses, cache still at capacity"
    );
    eprintln!(
        "✓ Test 3: Added 2 new entries, evicting LRU items (3 hits, 5 misses)"
    );

    // Test 4: Access key 3 again (should still be in cache since we only added
    // 2 new entries) Cache should contain: key3, key4, key5
    let size = backend.size(&keys[2]).await?;
    assert_eq!(size, Some(sizes[2]), "Key 3 should still be cached");

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (4, 5, 3),
        "Key 3 should be a cache hit"
    );
    eprintln!("✓ Test 4: Key 3 still cached (4 hits, 5 misses)");

    // Test 5: Verify key 1 is still accessible but will be a cache miss (was
    // evicted)
    let size = backend.size(&keys[0]).await?;
    assert_eq!(
        size,
        Some(sizes[0]),
        "Key 1 should still be accessible from S3"
    );

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (4, 6, 3),
        "Key 1 should be a cache miss"
    );
    eprintln!(
        "✓ Test 5: Key 1 evicted but accessible from S3 (4 hits, 6 misses)"
    );

    // Test 6: Verify key 2 is also a cache miss (was evicted)
    let size = backend.size(&keys[1]).await?;
    assert_eq!(
        size,
        Some(sizes[1]),
        "Key 2 should still be accessible from S3"
    );

    let (hits, misses, entries) = backend.cache_stats();
    assert_eq!(
        (hits, misses, entries),
        (4, 7, 3),
        "Key 2 should be a cache miss"
    );
    eprintln!(
        "✓ Test 6: Key 2 evicted but accessible from S3 (4 hits, 7 misses)"
    );

    // Clean up - delete test objects
    for key in &keys {
        let _ = backend.delete(key).await;
    }

    eprintln!(
        "✓ All cache tests passed! Final stats: {} hits, {} misses, {} entries",
        hits, misses, entries
    );
    Ok(())
}
