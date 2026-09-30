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

/// A lock store on a fresh table of its own.
async fn dynamo(
    test: &str,
    suffix: &str,
) -> anyhow::Result<Option<lfs_rs::DynamoLs>> {
    let Some(target) = common::dynamodb_target(test, suffix) else {
        return Ok(None);
    };
    GitRepo::setup_dynamodb_table(&target).await?;
    Ok(Some(lfs_rs::DynamoLs::from_config(
        &target.config,
        target.table,
    )))
}

fn owner_of(response: &lfs_rs::ListLocksResponse) -> Option<&str> {
    response
        .locks
        .first()?
        .owner
        .as_ref()
        .map(|o| o.name.as_str())
}

/// Locking a batch in which some paths are already locked locks only the
/// others, and leaves the existing locks alone.
#[tokio::test(flavor = "multi_thread")]
async fn batch_lock_leaves_existing_locks_alone()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::LockStorage;

    let Some(locks) = dynamo("DynamoDB batch lock", "batch").await? else {
        return Ok(());
    };
    let repo = "test/test".to_string();

    // The locked path comes second, so it isn't the first one requested.
    locks
        .create_lock(repo.clone(), "b.bin".into(), "alice".into())
        .await?;
    let batch = locks
        .create_locks(
            repo.clone(),
            vec!["a.bin".into(), "b.bin".into()],
            "bob".into(),
        )
        .await?;

    assert_eq!(batch.paths, ["a.bin"]);
    assert_eq!(batch.failures.len(), 1, "{:?}", batch.failures);
    assert_eq!(batch.failures[0].path, "b.bin");
    assert_eq!(batch.failures[0].reason, "lock held by user alice");

    let a = locks
        .list_locks(repo.clone(), Some("a.bin".into()), None, None, None)
        .await?;
    let b = locks
        .list_locks(repo.clone(), Some("b.bin".into()), None, None, None)
        .await?;
    assert_eq!(owner_of(&a), Some("bob"));
    assert_eq!(
        owner_of(&b),
        Some("alice"),
        "the existing lock was overwritten"
    );
    Ok(())
}

/// Of several clients locking the same path at once, exactly one gets the
/// lock, and the others are told who holds it.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_locks_have_one_winner()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::{LockStorage, LockStoreError};
    use std::sync::Arc;

    let Some(locks) = dynamo("DynamoDB concurrent locks", "race").await? else {
        return Ok(());
    };
    let locks = Arc::new(locks);
    let repo = "test/test".to_string();

    let attempts: Vec<_> = (0..8)
        .map(|n| {
            let (locks, repo) = (locks.clone(), repo.clone());
            tokio::spawn(async move {
                locks
                    .create_lock(repo, "race.bin".into(), format!("user{n}"))
                    .await
            })
        })
        .collect();

    let mut winners = Vec::new();
    let mut holders = Vec::new();
    for attempt in attempts {
        match attempt.await? {
            Ok(lock) => winners.push(lock.owner.unwrap().name),
            Err(err) => match err.downcast_ref::<LockStoreError>() {
                Some(LockStoreError::CreateConflict(held)) => {
                    holders.push(held.owner.clone().unwrap().name)
                }
                _ => return Err(err.into()),
            },
        }
    }

    assert_eq!(
        winners.len(),
        1,
        "more than one client got the lock: {winners:?}"
    );
    let winner = &winners[0];
    assert!(
        holders.iter().all(|h| h == winner),
        "{holders:?} vs {winner}"
    );

    let listed = locks
        .list_locks(repo, Some("race.bin".into()), None, None, None)
        .await?;
    assert_eq!(owner_of(&listed), Some(winner.as_str()));
    Ok(())
}

/// Cursors and limits come from clients: bad ones are errors, not panics.
#[tokio::test(flavor = "multi_thread")]
async fn bad_cursors_and_limits_are_handled()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::{LockStorage, LockStoreError};

    let Some(locks) = dynamo("DynamoDB cursors", "cursor").await? else {
        return Ok(());
    };
    let repo = "test/test".to_string();

    for cursor in ["not base64!", "bm90IGpzb24=" /* "not json" */] {
        let listed = locks
            .list_locks(repo.clone(), None, None, Some(cursor.into()), None)
            .await;
        let err = listed.expect_err("a bad cursor was accepted");
        assert!(
            matches!(err.downcast_ref(), Some(LockStoreError::BadRequest(_))),
            "{err}"
        );
        let verified = locks
            .verify_locks(
                repo.clone(),
                "alice".into(),
                Some(cursor.into()),
                None,
            )
            .await;
        assert!(verified.is_err());
    }

    locks
        .list_locks(repo.clone(), None, None, None, Some(u64::MAX))
        .await?;
    locks
        .verify_locks(repo, "alice".into(), None, Some(u64::MAX))
        .await?;
    Ok(())
}

/// Releasing another user's lock without forcing it is refused, and leaves
/// the lock. A lock can be listed by its id, and only it is.
#[tokio::test(flavor = "multi_thread")]
async fn locks_are_released_by_their_owner_and_listed_by_id()
-> Result<(), Box<dyn std::error::Error>> {
    use lfs_rs::{LockStorage, LockStoreError};

    let Some(locks) = dynamo("DynamoDB release", "release").await? else {
        return Ok(());
    };
    let repo = "test/release".to_string();

    let lock = locks
        .create_lock(repo.clone(), "a.bin".into(), "alice".into())
        .await?;
    locks
        .create_lock(repo.clone(), "b.bin".into(), "alice".into())
        .await?;

    let listed = locks
        .list_locks(repo.clone(), None, Some(lock.id.clone()), None, None)
        .await?;
    assert_eq!(listed.locks.len(), 1);
    assert_eq!(listed.locks[0].id, lock.id);

    let err = locks
        .release_lock(repo.clone(), "bob".into(), lock.id.clone(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.downcast_ref(), Some(LockStoreError::Forbidden(_))),
        "{err}"
    );
    let listed = locks
        .list_locks(repo.clone(), Some("a.bin".into()), None, None, None)
        .await?;
    assert_eq!(owner_of(&listed), Some("alice"), "the lock was released");

    locks
        .release_lock(repo.clone(), "bob".into(), lock.id, Some(true))
        .await?;
    Ok(())
}
