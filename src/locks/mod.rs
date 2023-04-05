#[cfg(feature = "dynamodb")]
pub mod dynamo;
pub mod localfs;
pub mod none;
#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "redis")]
pub use self::redis::RedisLockStore as RedisLs;
#[cfg(feature = "dynamodb")]
pub use dynamo::DynamoLockStore as DynamoLs;
pub use localfs::LocalFsLockStore as LocalLs;
pub use none::NoneLockStore as NoneLs;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LockStoreError {
    #[error("already created lock")]
    CreateConflict(Lock),
    #[error("not implemented")]
    NotImplemented,
    #[error("internal server error")]
    InternalServerError,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateLockRequest {
    pub path: String,

    #[serde(rename = "ref")]
    pub ref_info: RefInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateLockBatchRequest {
    pub paths: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListLocksResponse {
    pub locks: Vec<Lock>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VerifyLocksRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub limit: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ref")]
    pub ref_info: Option<RefInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VerifyLocksResponse {
    pub ours: Vec<Lock>,
    pub theirs: Vec<Lock>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReleaseLockRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ref")]
    pub ref_info: Option<RefInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReleaseLockBatchRequest {
    pub paths: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockOuter {
    pub lock: Lock,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Lock {
    pub id: String,
    pub path: String,
    pub locked_at: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<OwnerInfo>,
}

impl Lock {
    fn new(id: String, path: String, owner: String) -> Self {
        Lock {
            id,
            path,
            locked_at: chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
            owner: Some(OwnerInfo { name: owner }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockFailure {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockBatch {
    pub paths: Vec<String>,
    pub failures: Vec<LockFailure>,

    pub timestamp: String,
    pub owner: OwnerInfo,
}

impl LockBatch {
    fn new(
        paths: Vec<String>,
        failures: Vec<LockFailure>,
        owner: String,
    ) -> Self {
        LockBatch {
            paths,
            failures,
            timestamp: chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
            owner: OwnerInfo { name: owner },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockBatchOuter {
    pub batch: LockBatch,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OwnerInfo {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefInfo {
    pub name: String,
}

#[async_trait]
pub trait LockStorage {
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock>;

    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch>;

    async fn list_locks(
        &self,
        repo: String,
        path: Option<String>,
        id: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<ListLocksResponse>;

    async fn verify_locks(
        &self,
        repo: String,
        owner: String,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<VerifyLocksResponse>;

    async fn release_lock(
        &self,
        repo: String,
        owner: String,
        id: String,
        force: Option<bool>,
    ) -> Result<Lock>;

    async fn release_locks(
        &self,
        repo: String,
        owner: String,
        ids: Vec<String>,
        force: Option<bool>,
    ) -> Result<LockBatch>;
}

#[async_trait]
impl<L> LockStorage for Arc<L>
where
    L: LockStorage + Send + Sync,
{
    #[inline]
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock> {
        self.as_ref().create_lock(repo, path, owner).await
    }

    #[inline]
    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch> {
        self.as_ref().create_locks(repo, paths, owner).await
    }

    #[inline]
    async fn list_locks(
        &self,
        repo: String,
        path: Option<String>,
        id: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<ListLocksResponse> {
        self.as_ref()
            .list_locks(repo, path, id, cursor, limit)
            .await
    }

    #[inline]
    async fn verify_locks(
        &self,
        repo: String,
        owner: String,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<VerifyLocksResponse> {
        self.as_ref().verify_locks(repo, owner, cursor, limit).await
    }

    #[inline]
    async fn release_lock(
        &self,
        repo: String,
        owner: String,
        id: String,
        force: Option<bool>,
    ) -> Result<Lock> {
        self.as_ref().release_lock(repo, owner, id, force).await
    }

    #[inline]
    async fn release_locks(
        &self,
        repo: String,
        owner: String,
        ids: Vec<String>,
        force: Option<bool>,
    ) -> Result<LockBatch> {
        self.as_ref().release_locks(repo, owner, ids, force).await
    }
}
