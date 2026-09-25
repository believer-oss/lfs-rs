use futures::TryStreamExt;
use hex::FromHex;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, FromRedisValue, ParsingError, from_redis_value};
use std::sync::LazyLock;

use crate::lfs::Oid;

use anyhow::{Result, anyhow, bail};

use super::{
    ListLocksResponse, Lock, LockBatch, LockFailure, LockStorage,
    LockStoreError as Error, VerifyLocksResponse,
};
use async_trait::async_trait;
use sha2::Digest;

/// Stores locks in Redis as two keys, like `LocalLs`'s index:
///
/// - `{repo}:{path}` holds the lock's id, the hex SHA256 of `{repo}:{path}`.
/// - `{id}` holds the lock, as JSON.
///
/// Both are written with one MSETNX, so a lock is created whole or not at all,
/// and deleted with one DEL.
pub struct RedisLockStore {
    client: redis::Client,
    // FIXME: --lock-redis-ttl is accepted but not applied. Expiring both keys
    // while keeping MSETNX's all-or-nothing creation needs a Lua script.
    // ttl: usize,
}

/// The id of the lock on `path` in `repo`. Ids must be the same for the same
/// lock across backends, since clients hold on to them.
fn lock_id(repo: &str, path: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(format!("{repo}:{path}"));
    Oid::from(hasher.finalize()).to_string()
}

/// Deletes a path's index key (`KEYS[1]`) if it points to the lock id
/// (`ARGV[1]`, whose key is `KEYS[2]`) and that lock doesn't exist.
///
/// Atomically: checking and deleting in separate commands let two clients
/// repairing the same index at once delete the one a third had just created
/// along with its lock, hiding that lock from listings.
static REPAIR_DANGLING_INDEX: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r"
        if redis.call('EXISTS', KEYS[2]) == 0
            and redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        ",
    )
});

fn path_key(repo: &str, path: &str) -> String {
    format!("{repo}:{path}")
}

/// Escapes `s` for use in a SCAN MATCH pattern.
fn escape_glob(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

impl RedisLockStore {
    pub async fn new(uri: &str, _lock_ttl: usize) -> Result<Self, Error> {
        let client = redis::Client::open(uri)?;
        Ok(RedisLockStore { client })
    }

    async fn connection(&self) -> Result<MultiplexedConnection, Error> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }

    async fn get_lock(
        con: &mut MultiplexedConnection,
        id: &str,
    ) -> Result<Option<Lock>> {
        Ok(con.get::<_, Option<Lock>>(id).await?)
    }

    /// The lock with `id`, if it is one of `repo`'s. Ids are hashes of repo and
    /// path, and predictable, so a lock found under another repo's request is
    /// treated as not found.
    async fn get_repo_lock(
        con: &mut MultiplexedConnection,
        repo: &str,
        id: &str,
    ) -> Result<Option<Lock>> {
        Ok(Self::get_lock(con, id)
            .await?
            .filter(|lock| lock_id(repo, &lock.path) == id))
    }

    /// All the locks in `repo`.
    async fn repo_locks(
        con: &mut MultiplexedConnection,
        repo: &str,
    ) -> Result<Vec<Lock>> {
        let pattern = format!("{}:*", escape_glob(repo));
        let keys: Vec<String> = {
            let iter = con.scan_match::<_, String>(pattern).await?;
            iter.try_collect().await?
        };
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Locks released since the scan come back as nil and are skipped.
        let ids: Vec<Option<String>> = con.mget(keys).await?;
        let ids: Vec<String> = ids.into_iter().flatten().collect();
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let locks: Vec<Option<Lock>> = con.mget(ids).await?;
        Ok(locks.into_iter().flatten().collect())
    }

    /// Creates the lock, or returns the lock that already exists on `path`.
    async fn try_create(
        con: &mut MultiplexedConnection,
        repo: &str,
        path: &str,
        owner: &str,
    ) -> Result<Result<Lock, Lock>> {
        let id = lock_id(repo, path);
        let lock = Lock::new(id.clone(), path.to_string(), owner.to_string());
        let json = serde_json::to_string(&lock)?;

        let index = path_key(repo, path);

        // At most twice: the second time after removing a dangling index key.
        for _ in 0..2 {
            let created: bool = con
                .mset_nx(&[
                    (index.clone(), id.clone()),
                    (id.clone(), json.clone()),
                ])
                .await?;
            if created {
                return Ok(Ok(lock));
            }

            if let Some(existing) = Self::get_lock(con, &id).await? {
                return Ok(Err(existing));
            }

            // The path's index key exists but its lock doesn't, e.g. because
            // the lock was deleted without its index. It would block the path
            // for good, so remove it, and then try again.
            let _: u64 = REPAIR_DANGLING_INDEX
                .key(&index)
                .key(&id)
                .arg(&id)
                .invoke_async(con)
                .await?;
        }

        Err(anyhow!(Error::InternalServerError(format!(
            "could not create lock on '{path}'"
        ))))
    }

    async fn release(
        con: &mut MultiplexedConnection,
        repo: &str,
        owner: &str,
        id: &str,
        force: bool,
    ) -> Result<Lock> {
        let Some(lock) = Self::get_repo_lock(con, repo, id).await? else {
            bail!(Error::DeleteNotFound(id.to_string()));
        };

        match &lock.owner {
            Some(o) if o.name == owner || force => {}
            Some(o) => {
                bail!("lock held by {}, not {owner}, and not forced", o.name)
            }
            None => bail!("lock with no owner!"),
        }

        // Another client could release and retake the lock between the read
        // above and this delete. Locks are advisory and git-lfs checks them
        // again before pushing, so this is not guarded with WATCH.
        let _: u64 = con
            .del(&[id.to_string(), path_key(repo, &lock.path)])
            .await?;
        Ok(lock)
    }
}

impl FromRedisValue for Lock {
    fn from_redis_value(v: redis::Value) -> Result<Self, ParsingError> {
        let v: String = from_redis_value(v)?;
        serde_json::from_str::<Lock>(&v).map_err(|e| {
            format!("couldn't deserialize json from redis: {e}").into()
        })
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
        let mut con = self.connection().await?;
        match Self::try_create(&mut con, &repo, &path, &owner).await? {
            Ok(lock) => Ok(lock),
            Err(existing) => Err(anyhow!(Error::CreateConflict(existing))),
        }
    }

    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch> {
        let mut con = self.connection().await?;
        let mut paths_ok = Vec::with_capacity(paths.len());
        let mut failures = Vec::new();

        for path in paths {
            let reason =
                match Self::try_create(&mut con, &repo, &path, &owner).await {
                    Ok(Ok(_)) => {
                        paths_ok.push(path);
                        continue;
                    }
                    // The same reasons as `LocalLs` gives.
                    Ok(Err(existing)) => match existing.owner {
                        Some(o) if o.name == owner => {
                            "lock already held".to_string()
                        }
                        Some(o) => format!("lock held by user {}", o.name),
                        None => "lock held".to_string(),
                    },
                    Err(err) => err.to_string(),
                };
            failures.push(LockFailure { path, reason });
        }

        Ok(LockBatch::new(paths_ok, failures, owner))
    }

    async fn list_locks(
        &self,
        repo: String,
        path: Option<String>,
        id: Option<String>,
        _cursor: Option<String>,
        _limit: Option<u64>,
    ) -> Result<ListLocksResponse> {
        let mut con = self.connection().await?;

        let locks = if let Some(id) = id {
            // Reject anything that isn't an id before using it as a key.
            let id = Oid::from(<[u8; 32]>::from_hex(id)?).to_string();
            match Self::get_repo_lock(&mut con, &repo, &id).await? {
                Some(lock) => vec![lock],
                None => bail!(Error::LockNotFound(id)),
            }
        } else if let Some(path) = path {
            let id: Option<String> = con.get(path_key(&repo, &path)).await?;
            let lock = match id {
                Some(id) => Self::get_lock(&mut con, &id).await?,
                None => None,
            };
            match lock {
                Some(lock) => vec![lock],
                None => bail!(Error::LockNotFound(path)),
            }
        } else {
            Self::repo_locks(&mut con, &repo).await?
        };

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
        let mut con = self.connection().await?;
        let (ours, theirs) = Self::repo_locks(&mut con, &repo)
            .await?
            .into_iter()
            .partition(|lock| {
                lock.owner.as_ref().is_some_and(|o| o.name == owner)
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
        let id = Oid::from(<[u8; 32]>::from_hex(id)?).to_string();
        let mut con = self.connection().await?;
        Self::release(&mut con, &repo, &owner, &id, force.unwrap_or(false))
            .await
    }

    async fn release_locks(
        &self,
        repo: String,
        owner: String,
        paths: Vec<String>,
        force: Option<bool>,
    ) -> Result<LockBatch> {
        let mut con = self.connection().await?;
        let force = force.unwrap_or(false);
        let mut paths_ok = Vec::with_capacity(paths.len());
        let mut failures = Vec::new();

        for path in paths {
            let id = lock_id(&repo, &path);
            match Self::release(&mut con, &repo, &owner, &id, force).await {
                Ok(_) => paths_ok.push(path),
                Err(err) => failures.push(LockFailure {
                    path,
                    reason: err.to_string(),
                }),
            }
        }

        Ok(LockBatch::new(paths_ok, failures, owner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_match_local_ls() {
        // The hex SHA256 of "org/repo:a.bin", as LocalLs computes it.
        let mut hasher = sha2::Sha256::new();
        hasher.update("org/repo:a.bin");
        let expected = hex::encode(hasher.finalize());

        assert_eq!(lock_id("org/repo", "a.bin"), expected);
    }

    #[test]
    fn escapes_scan_patterns() {
        assert_eq!(escape_glob("org/repo"), "org/repo");
        assert_eq!(escape_glob(r"a*b?c[d]e\f"), r"a\*b\?c\[d\]e\\f");
    }
}
