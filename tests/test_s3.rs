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

//! Integration tests against S3 or an S3-compatible server. These skip unless
//! `LFS_TEST_S3_BUCKET` is set; see `tests/common.rs` for the configuration.
//!
//! Be sure to *only* use non-production credentials and buckets for testing
//! purposes.

mod common;

use std::path::Path;

use futures::future::Either;
use lfs_rs::S3ServerBuilder;
use lfs_rs::stats::{RequestClass, STATS};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::oneshot;

use common::{GitRepo, SERVER_ADDR, init_logger};

/// Pushes and pulls LFS objects through S3 without a local cache. Each caller
/// needs its own prefix: the objects are deterministic, so encrypted and
/// unencrypted runs would otherwise find each other's objects.
async fn s3_smoke_test(
    test: &str,
    prefix: &str,
    key: Option<[u8; 32]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _guard = init_logger();

    let Some(target) = common::s3_target(test).await else {
        return Ok(());
    };

    // Make sure our seed is deterministic. This prevents us from filling up our
    // S3 bucket with a bunch of random files if this test gets ran a bunch of
    // times.
    let mut rng = StdRng::seed_from_u64(42);

    let mut server = S3ServerBuilder::new(target.bucket, key);
    server.prefix(prefix.into());
    server.sdk_config(target.config);

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
    repo.lfs_push()?;

    // Make sure we can re-download the same objects.
    let before_pull = STATS.snapshot();
    repo.clean_lfs()?;
    repo.lfs_pull()?;

    // The counters are shared by every test in this binary, so only check
    // for at least what this pull did.
    let pulled = STATS.snapshot().since(&before_pull);
    assert!(pulled.requests(RequestClass::Batch) >= 1, "{pulled:?}");
    assert!(pulled.requests(RequestClass::Download) >= 3, "{pulled:?}");
    assert!(pulled.bytes_downloaded >= 28 * 1024 * 1024, "{pulled:?}");
    assert!(
        pulled.s3_size_cache_hits + pulled.s3_size_cache_misses >= 3,
        "{pulled:?}"
    );

    // Push again. This should be super fast.
    repo.lfs_push()?;

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await? {
        // If the server exited first, then propagate the error.
        result?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_smoke_test_unencrypted() -> Result<(), Box<dyn std::error::Error>> {
    s3_smoke_test("S3 unencrypted", "test_lfs_unencrypted", None).await
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_smoke_test_encrypted() -> Result<(), Box<dyn std::error::Error>> {
    let key = StdRng::seed_from_u64(42).random();
    s3_smoke_test("S3 encrypted", "test_lfs_encrypted", Some(key)).await
}

/// The configuration production runs, less transfer acceleration: S3 behind a
/// local disk cache, unencrypted, with DynamoDB locks and GitHub
/// authentication (mocked).
#[cfg(feature = "dynamodb")]
#[tokio::test(flavor = "multi_thread")]
async fn s3_production_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = init_logger();
    let startup_span = common::startup();

    let test = "S3 production";
    let Some(s3) = common::s3_target(test).await else {
        return Ok(());
    };
    let Some(dynamodb) = common::dynamodb_target(test, "s3") else {
        return Ok(());
    };

    GitRepo::setup_dynamodb_table(&dynamodb).await?;
    let locks = lfs_rs::DynamoLs::from_config(&dynamodb.config, dynamodb.table);

    let cache = tempfile::TempDir::new()?;
    let mock = GitRepo::setup_mock_gh_auth().await;

    let mut server = S3ServerBuilder::new(s3.bucket, None);
    server.prefix("test_lfs_production".into());
    server.sdk_config(s3.config);
    server.cache(lfs_rs::Cache::new(cache.path().into(), 1024 * 1024 * 1024));
    server.authenticated(true);
    server.authentication_server(mock.uri());
    let (server, addr) = server.spawn(SERVER_ADDR, locks).await?;

    let before = STATS.snapshot();
    common::lock_smoke_test(server, addr, Some(startup_span)).await?;

    // Uploads go through the disk cache, and every API request is
    // authenticated through the (mocked) GitHub API or its cache.
    let after = STATS.snapshot();
    let delta = after.since(&before);
    assert!(after.disk_cache_bytes >= 28 * 1024 * 1024, "{after:?}");
    assert_eq!(after.disk_cache_limit, 1024 * 1024 * 1024);
    assert!(delta.github_api_calls >= 1, "{delta:?}");
    assert!(delta.github_auth_cache_hits >= 1, "{delta:?}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_size_cache_test() -> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::Oid;
    use lfs_rs::storage::{Storage, StorageKey};

    let _guard = init_logger();

    let Some(target) = common::s3_target("S3 size cache").await else {
        return Ok(());
    };

    // Create S3 backend with a very small cache (3 entries) to test eviction
    let backend = lfs_rs::storage::S3::from_config(
        &target.config,
        target.bucket,
        "test_lfs_cache".to_string(),
        None,
        false,
        3, // Small cache size for testing
        std::time::Duration::ZERO,
    );
    backend.check().await?;

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

/// Objects are uploaded to S3 in parts, which is what lets objects over 5 GiB
/// be stored. With the smallest part size S3 allows, small objects exercise
/// the same code.
mod multipart {
    use super::*;
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt};
    use lfs_rs::Oid;
    use lfs_rs::storage::{
        LFSObject, MIN_PART_SIZE, Namespace, S3, Storage, StorageKey,
    };
    use sha2::Digest;

    const PREFIX: &str = "test_lfs_multipart";

    fn backend(target: &common::S3Target) -> S3 {
        S3::from_config(
            &target.config,
            target.bucket.clone(),
            PREFIX.into(),
            None,
            false,
            0,
            std::time::Duration::ZERO,
        )
        .with_part_size(MIN_PART_SIZE)
    }

    fn key(data: &[u8]) -> StorageKey {
        let oid = Oid::from(sha2::Sha256::digest(data));
        StorageKey::new(Namespace::new("test".into(), "multipart".into()), oid)
    }

    /// A key no earlier run used, so that uploads a failed run left open can't
    /// affect this one.
    fn unique_key(name: &str) -> StorageKey {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        key(format!("{name} {nanos}").as_bytes())
    }

    /// `data` as a stream of chunks that don't line up with the parts.
    fn object(data: &[u8]) -> LFSObject {
        let chunks: Vec<Result<Bytes, std::io::Error>> = data
            .chunks(1024 * 1024 + 3)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        LFSObject::new(
            data.len() as u64,
            Box::pin(futures::stream::iter(chunks)),
        )
    }

    async fn round_trip(len: usize) -> Result<(), Box<dyn std::error::Error>> {
        let Some(target) = common::s3_target("S3 multipart").await else {
            return Ok(());
        };
        let backend = backend(&target);

        let mut data = vec![0u8; len];
        StdRng::seed_from_u64(len as u64).fill(&mut data[..]);
        let key = key(&data);

        backend.put(key.clone(), object(&data)).await?;

        assert_eq!(backend.size(&key).await?, Some(len as u64));
        let stored = backend.get(&key).await?.expect("object was not stored");
        let stored: Vec<Bytes> = stored.stream().try_collect().await?;
        assert!(stored.concat() == data, "stored object differs");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ends_with_a_short_part() -> Result<(), Box<dyn std::error::Error>>
    {
        round_trip(2 * MIN_PART_SIZE + MIN_PART_SIZE * 2 / 5 + 7).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ends_on_a_part_boundary() -> Result<(), Box<dyn std::error::Error>>
    {
        round_trip(2 * MIN_PART_SIZE).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_object() -> Result<(), Box<dyn std::error::Error>> {
        round_trip(0).await
    }

    /// Uploads of `key` still open. Only this key's: tests run in parallel, and
    /// an earlier failed run may have left others open.
    async fn open_uploads(
        target: &common::S3Target,
        key: &StorageKey,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let path = format!("{PREFIX}/{}/{}", key.namespace(), key.oid().path());
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&target.config)
                .force_path_style(true)
                .build(),
        );
        let uploads = client
            .list_multipart_uploads()
            .bucket(&target.bucket)
            .prefix(&path)
            .send()
            .await?;
        Ok(uploads.uploads().len())
    }

    /// When a client disconnects, hyper drops the request, `put` included,
    /// so it never sees an error. The upload must still be aborted.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_upload_is_aborted()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(target) = common::s3_target("S3 multipart drop").await else {
            return Ok(());
        };
        let backend = backend(&target);

        // A part and a bit, then the client stalls.
        let first: Vec<Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from(vec![9u8; MIN_PART_SIZE + 1024 * 1024]))];
        let stream =
            futures::stream::iter(first).chain(futures::stream::pending());
        let object =
            LFSObject::new((MIN_PART_SIZE * 3) as u64, Box::pin(stream));

        let key = unique_key("dropped upload");
        let put = backend.put(key.clone(), object);
        let dropped =
            tokio::time::timeout(std::time::Duration::from_secs(3), put).await;
        assert!(
            dropped.is_err(),
            "the upload should still have been waiting"
        );

        // The abort runs in the background once the part in flight is done.
        for _ in 0..50 {
            if open_uploads(&target, &key).await? == 0 {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        panic!("the dropped upload was left open");
    }

    /// A failed upload is aborted, so its parts aren't left in the bucket.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_upload_is_aborted() -> Result<(), Box<dyn std::error::Error>>
    {
        let Some(target) = common::s3_target("S3 multipart abort").await else {
            return Ok(());
        };
        let backend = backend(&target);

        // One full part, then the client goes away.
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from(vec![7u8; MIN_PART_SIZE + 1024 * 1024])),
            Err(std::io::Error::other("client went away")),
        ];
        let key = unique_key("failed upload");
        let object = LFSObject::new(
            (MIN_PART_SIZE * 3) as u64,
            Box::pin(futures::stream::iter(chunks)),
        );
        assert!(backend.put(key.clone(), object).await.is_err());
        assert_eq!(open_uploads(&target, &key).await?, 0, "upload left open");
        assert_eq!(backend.size(&key).await?, None);
        Ok(())
    }
}
