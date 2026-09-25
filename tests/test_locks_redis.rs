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

//! Locking against Redis. This skips unless `LFS_TEST_REDIS_URI` is set; see
//! `tests/common.rs` for the configuration.
#![cfg(feature = "redis")]

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn redis_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = common::init_logger();
    let startup_span = common::startup();

    let Some(uri) = common::redis_target("Redis locks", "test/test").await
    else {
        return Ok(());
    };
    let locks = lfs_rs::RedisLs::new(&uri, 0).await?;

    common::smoke_test(locks, Some(startup_span)).await
}

/// A lock can only be read or released through its own repo. Lock ids are
/// predictable, so otherwise anyone with push access to one repo could
/// release another repo's locks.
#[tokio::test(flavor = "multi_thread")]
async fn locks_are_scoped_to_their_repo()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::LockStorage;

    let Some(uri) =
        common::redis_target("Redis lock scoping", "test/scoping").await
    else {
        return Ok(());
    };
    let locks = lfs_rs::RedisLs::new(&uri, 0).await?;
    let (repo, other) = ("test/scoping".to_string(), "test/other".to_string());

    let lock = locks
        .create_lock(repo.clone(), "scoped.bin".into(), "alice".into())
        .await?;

    let listed = locks
        .list_locks(other.clone(), None, Some(lock.id.clone()), None, None)
        .await;
    assert!(listed.is_err(), "listed another repo's lock by id");

    let released = locks
        .release_lock(
            other.clone(),
            "mallory".into(),
            lock.id.clone(),
            Some(true),
        )
        .await;
    assert!(released.is_err(), "released another repo's lock");

    let still_there = locks
        .list_locks(repo.clone(), Some("scoped.bin".into()), None, None, None)
        .await?;
    assert_eq!(still_there.locks.len(), 1);

    locks
        .release_lock(repo, "alice".into(), lock.id, None)
        .await?;
    Ok(())
}

/// If a lock's key is gone but its path's index key is left behind, the path
/// can still be locked.
#[tokio::test(flavor = "multi_thread")]
async fn a_dangling_index_does_not_block_the_path()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::LockStorage;
    use redis::AsyncCommands;

    let Some(uri) =
        common::redis_target("Redis dangling index", "test/dangling").await
    else {
        return Ok(());
    };
    let locks = lfs_rs::RedisLs::new(&uri, 0).await?;
    let repo = "test/dangling".to_string();

    let lock = locks
        .create_lock(repo.clone(), "dangling.bin".into(), "alice".into())
        .await?;

    // Leave only the index key.
    let mut con = redis::Client::open(uri.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let _: u64 = con.del(&lock.id).await?;

    let relocked = locks
        .create_lock(repo.clone(), "dangling.bin".into(), "bob".into())
        .await?;
    assert_eq!(relocked.owner.map(|o| o.name).as_deref(), Some("bob"));

    locks
        .release_lock(repo, "bob".into(), relocked.id, None)
        .await?;
    Ok(())
}

/// Several clients repairing the same dangling index at once must leave the
/// winner's lock findable by path.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_repairs_keep_the_winners_index()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::{LockStorage, LockStoreError};
    use redis::AsyncCommands;
    use std::sync::Arc;

    let Some(uri) =
        common::redis_target("Redis concurrent repairs", "test/repair").await
    else {
        return Ok(());
    };
    let locks = Arc::new(lfs_rs::RedisLs::new(&uri, 0).await?);
    let mut con = redis::Client::open(uri.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let repo = "test/repair".to_string();

    // Paths unique to this run, so that keys a failed run orphaned can't
    // matter.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    for round in 0..50 {
        let path = format!("repair-{run}-{round}.bin");

        // Leave a dangling index: the lock's key is gone, its index isn't.
        let lock = locks
            .create_lock(repo.clone(), path.clone(), "setup".into())
            .await?;
        let _: u64 = con.del(&lock.id).await?;

        let attempts: Vec<_> = (0..8)
            .map(|n| {
                let (locks, repo, path) =
                    (locks.clone(), repo.clone(), path.clone());
                tokio::spawn(async move {
                    locks.create_lock(repo, path, format!("user{n}")).await
                })
            })
            .collect();
        let mut winners = Vec::new();
        for attempt in attempts {
            match attempt.await? {
                Ok(lock) => winners.push(lock),
                Err(err) => assert!(
                    matches!(
                        err.downcast_ref(),
                        Some(LockStoreError::CreateConflict(_))
                    ),
                    "round {round}: {err}"
                ),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}");

        let listed = locks
            .list_locks(repo.clone(), Some(path.clone()), None, None, None)
            .await
            .map_err(|err| {
                format!("round {round}: the winner's index is gone: {err}")
            })?;
        assert_eq!(listed.locks[0].id, winners[0].id, "round {round}");

        let owner = winners[0].owner.clone().unwrap().name;
        locks
            .release_lock(repo.clone(), owner, winners[0].id.clone(), None)
            .await?;
    }
    Ok(())
}
