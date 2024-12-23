use crate::lfs::Oid;
use futures::StreamExt;
use hex::FromHex;
use redis::{from_redis_value, AsyncCommands, AsyncIter, FromRedisValue};

use super::{
    ListLocksResponse, Lock, LockBatch, LockStorage, VerifyLocksResponse,
};
use anyhow::{anyhow, bail, Error, Result};
use async_trait::async_trait;
use sha2::Digest;

pub struct RedisLockStore {
    client: redis::Client,
    // ttl: usize,
}

impl RedisLockStore {
    pub async fn new(uri: &str, _lock_ttl: usize) -> Result<Self, Error> {
        let client = redis::Client::open(uri)?;
        Ok(RedisLockStore {
            client,
            // ttl: lock_ttl,
        })
    }

    async fn get_lock_from_oid(&self, oid: &Oid) -> Result<Vec<Lock>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        match con.get::<String, Lock>(oid.to_string()).await {
            Ok(v) => Ok(vec![v]),
            Err(_) => Err(anyhow!(super::LockStoreError::InternalServerError)),
        }
    }
}

impl FromRedisValue for Lock {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let v: String = from_redis_value(v)?;
        match serde_json::from_slice::<Lock>(v.as_bytes()) {
            Ok(l) => Ok(l),
            Err(e) => Err((
                redis::ErrorKind::TypeError,
                "couldn't deserialize json from redis",
                e.to_string(),
            )
                .into()),
        }
    }
}

#[async_trait]
impl LockStorage for RedisLockStore {
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock> {
        let mut hasher = sha2::Sha256::new();
        hasher.update(format!("{}:{}", repo, path));
        let path_oid = Oid::from(hasher.finalize()).to_string();

        let key = format!("{}:{}", repo, path);
        // We could store this as an HSET of the elements, or as a string JSON
        // of the lock and a separate lookup from repo/path to the
        // hashed key. This implements the latter.
        let lock = Lock::new(key.to_string(), path, owner);
        let mut con = self.client.get_multiplexed_async_connection().await?;

        let json_lock = serde_json::to_string(&lock)?;

        // Try to set the value, if it errors try to get the existing lock
        if con
            .mset_nx::<String, String, bool>(&[
                (key, path_oid.clone()),
                (path_oid.clone(), json_lock),
            ])
            .await?
        {
            Ok(lock)
        } else if let Ok(v) = con.get::<String, String>(path_oid).await {
            Err(anyhow!(super::LockStoreError::CreateConflict(
                serde_json::from_str(v.as_str())?
            )))
        } else {
            Err(anyhow!(super::LockStoreError::InternalServerError))
        }
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

    async fn list_locks(
        &self,
        repo: String,
        path: Option<String>,
        id: Option<String>,
        _cursor: Option<String>,
        _limit: Option<u64>,
    ) -> Result<ListLocksResponse> {
        let locks: Vec<Lock>;
        if let Some(id) = id {
            // if we're passed an id, look it up and return the single lock.
            // Path shouldn't matter?
            let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
            locks = self.get_lock_from_oid(&oid).await?;
        } else if let Some(path) = path {
            // If we have a path, we need to find it and return the matching
            // lock
            let mut con =
                self.client.get_multiplexed_async_connection().await?;
            let key = format!("{}:{}", repo, path);
            match con.get::<String, String>(key).await {
                Ok(id) => {
                    let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
                    locks = self.get_lock_from_oid(&oid).await?;
                }
                Err(_) => {
                    return Err(anyhow!(
                        super::LockStoreError::InternalServerError
                    ))
                }
            }
        } else {
            // If we don't get an id or path, we return them all...
            let mut con =
                self.client.get_multiplexed_async_connection().await?;
            let iter: AsyncIter<String> =
                con.scan_match(format!("{}:*", repo)).await?;
            let keys: Vec<String> = iter.collect().await;
            locks = con.mget::<Vec<String>, Vec<Lock>>(keys).await?;
        }
        Ok(ListLocksResponse {
            locks,
            next_cursor: None,
        })
    }

    async fn verify_locks(
        &self,
        repo: String,
        owner: String,
        _cursor: Option<String>,
        _limit: Option<u64>,
    ) -> Result<VerifyLocksResponse> {
        // let lockfile = self
        //     .lockfile
        //     .lock()
        //     .expect("couldnt acquire lock, poisoned");
        //
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let iter: AsyncIter<String> =
            con.scan_match(format!("{}:*", repo)).await?;
        let keys: Vec<String> = iter.collect().await;
        let locks = con.mget::<Vec<String>, Vec<Lock>>(keys).await?;
        let (ours, theirs) = locks.iter().cloned().partition(|v| {
            if let Some(o) = v.owner.clone() {
                o.name == owner
            } else {
                false
            }
        });

        Ok(VerifyLocksResponse {
            ours,
            theirs,
            next_cursor: None,
        })
    }

    async fn release_lock(
        &self,
        repo: String,
        owner: String,
        id: String,
        force: Option<bool>,
    ) -> Result<Lock> {
        // let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
        // let key = LocalFsKey::new_from_oid(&repo, &oid);
        //
        // let mut lockfile = self
        //     .lockfile
        //     .lock()
        //     .expect("couldnt acquire lock, poisoned");
        // lockfile.del_entry(&key, owner, force)
        let force = force.unwrap_or(false);
        let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let lock = con.get::<String, Lock>(oid.to_string()).await?;
        if let Some(o) = lock.owner.clone() {
            if owner == o.name || force {
                let key = format!("{}:{}", repo, lock.path.clone());
                let keys = vec![oid.to_string(), key];
                // FIXME This should be handling the case where one key deletes
                // but the other does not...
                match con.del::<Vec<String>, u8>(keys).await? {
                    2 => Ok(lock),
                    _ => {
                        bail!("failed to release lock")
                    }
                }
            } else {
                bail!("lock owner incorrect and not forced")
            }
        } else {
            bail!("lock with no owner!")
        }
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
