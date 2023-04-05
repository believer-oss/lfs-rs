use super::{
    ListLocksResponse, Lock, LockBatch, LockStorage, VerifyLocksResponse,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;

#[derive(Debug)]
pub struct NoneLockStore;

impl NoneLockStore {
    pub fn new() -> Self {
        NoneLockStore {}
    }
}

impl Default for NoneLockStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LockStorage for NoneLockStore {
    #[inline]
    async fn create_lock(
        &self,
        _repo: String,
        _path: String,
        _owner: String,
    ) -> Result<Lock> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }

    #[inline]
    async fn create_locks(
        &self,
        _repo: String,
        _path: Vec<String>,
        _owner: String,
    ) -> Result<LockBatch> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }

    #[inline]
    async fn list_locks(
        &self,
        _repo: String,
        _path: Option<String>,
        _id: Option<String>,
        _cursor: Option<String>,
        _limit: Option<u64>,
    ) -> Result<ListLocksResponse> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }

    #[inline]
    async fn verify_locks(
        &self,
        _repo: String,
        _owner: String,
        _cursor: Option<String>,
        _limit: Option<u64>,
    ) -> Result<VerifyLocksResponse> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }

    #[inline]
    async fn release_lock(
        &self,
        _repo: String,
        _owner: String,
        _id: String,
        _force: Option<bool>,
    ) -> Result<Lock> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }

    #[inline]
    async fn release_locks(
        &self,
        _repo: String,
        _owner: String,
        _paths: Vec<String>,
        _force: Option<bool>,
    ) -> Result<LockBatch> {
        Err(anyhow!(super::LockStoreError::NotImplemented))
    }
}
