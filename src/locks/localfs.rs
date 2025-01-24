use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};

use crate::lfs::Oid;
use hex::FromHex;

use super::{
    ListLocksResponse, Lock, LockBatch, LockFailure, LockStorage,
    VerifyLocksResponse,
};
use anyhow::{anyhow, bail, Context, Error, Result};
use async_trait::async_trait;
use sha2::Digest;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct LocalFsKey {
    pub repo: String,
    pub path_oid: Oid,
}

impl LocalFsKey {
    fn new(repo: &String, path: &String) -> Self {
        let mut hasher = sha2::Sha256::new();
        hasher.update(format!("{}:{}", repo, path));
        let path_oid = Oid::from(hasher.finalize());

        LocalFsKey {
            repo: repo.clone(),
            path_oid,
        }
    }

    fn new_from_oid(repo: &str, path_oid: &Oid) -> Self {
        Self {
            repo: repo.to_owned(),
            path_oid: *path_oid,
        }
    }
}

impl std::fmt::Display for LocalFsKey {
    // Since identical paths in different repos are still unique locks, we don't
    // factor that into the string representation.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.path_oid.fmt(f)
    }
}

impl Serialize for LocalFsKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.repo, self.path_oid))
    }
}

impl<'de> Deserialize<'de> for LocalFsKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = <&str>::deserialize(deserializer)?;
        let mut parts = data.splitn(2, ':');
        let repo: String =
            parts.next().unwrap().parse().map_err(D::Error::custom)?;
        let path_oid: Oid = parts
            .next()
            .ok_or_else(|| D::Error::custom("LocalFsKey must contain :"))?
            .parse()
            .map_err(D::Error::custom)?;
        Ok(Self { repo, path_oid })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct LocalFsPathKey {
    pub repo: String,
    pub path: String,
}

impl LocalFsPathKey {
    fn new(repo: &str, path: &str) -> Self {
        Self {
            repo: repo.to_owned(),
            path: path.to_owned(),
        }
    }
}

impl Serialize for LocalFsPathKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.repo, self.path))
    }
}

impl<'de> Deserialize<'de> for LocalFsPathKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = <&str>::deserialize(deserializer)?;
        let mut parts = data.splitn(2, ':');
        let repo: String =
            parts.next().unwrap().parse().map_err(D::Error::custom)?;
        let path: String = parts
            .next()
            .ok_or_else(|| D::Error::custom("LocalFsPathKey must contain :"))?
            .parse()
            .map_err(D::Error::custom)?;
        Ok(Self { repo, path })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalFsLockFile {
    locks: HashMap<LocalFsKey, Lock>,
    oid_lookup: HashMap<LocalFsPathKey, LocalFsKey>,
}

impl LocalFsLockFile {
    fn add_entry(
        &mut self,
        key: LocalFsKey,
        path_key: LocalFsPathKey,
        lock: Lock,
    ) -> Result<(), Error> {
        match self.locks.get(&key) {
            Some(v) => {
                Err(anyhow!(super::LockStoreError::CreateConflict(v.clone())))
            }
            None => {
                // We've already checked that the result of locks.insert is
                // None, and if there was an entry in oid_lookup
                // it was wrong...
                self.locks.insert(key.clone(), lock);
                self.oid_lookup.insert(path_key, key);
                Ok(())
            }
        }
    }

    fn del_entry(
        &mut self,
        key: &LocalFsKey,
        owner: String,
        force: Option<bool>,
    ) -> Result<Lock, Error> {
        let force = force.unwrap_or(false);
        let lock_ref = self.locks.get(key).ok_or_else(|| {
            super::LockStoreError::DeleteNotFound(key.to_string())
        })?;

        let lock: Lock;
        if let Some(o) = lock_ref.owner.clone() {
            if owner == o.name || force {
                lock = self
                    .locks
                    .remove(key)
                    .context("could not find lock to delete")?;

                let repo = key.repo.as_ref();
                let path = lock.path.as_ref();
                let path_key = LocalFsPathKey::new(repo, path);
                self.oid_lookup
                    .remove(&path_key)
                    .context("could not find the oid to delete")?;
            } else {
                bail!(
                    "lock owner {}, but unlocking with {} and not forced",
                    owner,
                    o.name
                )
            }
        } else {
            bail!("lock with no owner!")
        }
        Ok(lock)
    }
}

/// This is a local implementation of a Lock Store using a single JSON file to
/// store locks. Nothing about this is performant, and should only be used for
/// development testing.
pub struct LocalFsLockStore {
    path: std::path::PathBuf,
    // We don't want a tokio mutex, since we're not awaiting with it held.
    lockfile: Arc<Mutex<LocalFsLockFile>>,
}

impl LocalFsLockStore {
    pub async fn new(path: std::path::PathBuf) -> Result<Self, Error> {
        let lockfile =
            Arc::new(Mutex::new(Self::load_store(path.clone(), true).await?));
        Ok(LocalFsLockStore { path, lockfile })
    }

    async fn load_store(
        path: PathBuf,
        create: bool,
    ) -> Result<LocalFsLockFile, Error> {
        let lockfile: LocalFsLockFile = match File::open(path.clone()).await {
            Ok(f) => {
                let mut reader = BufReader::new(f);
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await?;
                serde_json::from_str(
                    std::str::from_utf8(&buffer)
                        .context("could not read file as utf8")?,
                )?
            }
            Err(e) => {
                if create {
                    File::create(path.clone()).await?;
                    LocalFsLockFile {
                        locks: HashMap::new(),
                        oid_lookup: HashMap::new(),
                    }
                } else {
                    return Err(e.into());
                }
            }
        };

        Ok(lockfile)
    }

    async fn save_store(&self) -> Result<(), Error> {
        let s: String;
        {
            let lockfile =
                self.lockfile.lock().expect("couldnt aquire lock, poisoned");

            s = serde_json::to_string(&*lockfile)?;
        }
        let mut f = File::create(self.path.clone()).await?;
        f.write_all(s.as_bytes()).await?;
        f.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl LockStorage for LocalFsLockStore {
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock> {
        let key = LocalFsKey::new(&repo, &path);
        let path_key = LocalFsPathKey::new(&repo, &path);
        let lock = Lock::new(key.to_string(), path, owner);
        {
            let mut lockfile =
                self.lockfile.lock().expect("couldnt aquire lock, poisoned");
            match lockfile.locks.contains_key(&key) {
                true => bail!("lock exists"),
                false => lockfile.add_entry(key, path_key, lock.clone())?,
            }
        }
        self.save_store().await?;
        Ok(lock)
    }

    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch> {
        let mut paths_ok: Vec<String> = vec![];
        let mut failures: Vec<LockFailure> = vec![];

        paths_ok.reserve_exact(paths.len());
        failures.reserve(paths.len());

        {
            let mut lockfile =
                self.lockfile.lock().expect("couldnt aquire lock, poisoned");

            for path in paths.iter() {
                let key = LocalFsKey::new(&repo, path);
                let path_key = LocalFsPathKey::new(&repo, path);
                let id = key.to_string();

                if let Some(lock) = lockfile.locks.get(&key) {
                    let reason: String = if lock.owner.is_some()
                        && lock.owner.as_ref().unwrap().name == owner
                    {
                        "lock already held".to_string()
                    } else {
                        format!(
                            "lock held by user {}",
                            lock.owner.as_ref().unwrap().name
                        )
                    };
                    failures.push(LockFailure {
                        path: path.clone(),
                        reason,
                    })
                } else {
                    let lock =
                        Lock::new(id.clone(), path.clone(), owner.clone());
                    lockfile.add_entry(key, path_key, lock.clone())?;
                    paths_ok.push(path.to_string());
                }
            }
        }

        self.save_store().await?;

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
        let locks: Vec<Lock>;
        if let Some(id) = id {
            // if we're passed an id, look it up and return the single lock.
            // Path shouldn't matter?
            let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
            let key = LocalFsKey::new_from_oid(&repo, &oid);

            {
                let lockfile = self
                    .lockfile
                    .lock()
                    .expect("couldnt acquire lock, poisoned");
                match lockfile.locks.get(&key) {
                    Some(lock) => {
                        locks = vec![lock.clone()];

                        return Ok(ListLocksResponse {
                            locks,
                            next_cursor: None,
                        });
                    }
                    None => bail!("couldn't find key"),
                }
            }
        } else if let Some(path) = path {
            let path_key = LocalFsPathKey::new(&repo, &path);
            // If we have a path, we need to find it and return the matching
            // lock
            let lockfile = self
                .lockfile
                .lock()
                .expect("couldnt acquire lock, poisoned");
            let key = lockfile.oid_lookup.get(&path_key);
            match key {
                Some(k) => {
                    locks = vec![lockfile
                        .locks
                        .get(k)
                        .expect(
                            "could not find lock for path, invalid oid_lookup!",
                        )
                        .clone()]
                }
                None => bail!("could not find lock for path"),
            }
        } else {
            // If we don't get an id or path, we return them all...
            let lockfile = self
                .lockfile
                .lock()
                .expect("couldnt acquire lock, poisoned");
            locks = lockfile
                .locks
                .iter()
                .filter(|(k, _)| k.repo.eq_ignore_ascii_case(repo.as_str()))
                .map(|(_, v)| v.clone())
                .collect()
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
        let lockfile = self
            .lockfile
            .lock()
            .expect("couldnt acquire lock, poisoned");

        let (ours, theirs) = lockfile
            .locks
            .iter()
            .filter(|(k, _)| k.repo.eq_ignore_ascii_case(repo.as_str()))
            .map(|(_, v)| v.clone())
            .partition(|v| {
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
        let oid = Oid::from(<[u8; 32]>::from_hex(id)?);
        let key = LocalFsKey::new_from_oid(&repo, &oid);

        let mut lockfile = self
            .lockfile
            .lock()
            .expect("couldnt acquire lock, poisoned");
        lockfile.del_entry(&key, owner, force)
    }

    async fn release_locks(
        &self,
        repo: String,
        owner: String,
        paths: Vec<String>,
        force: Option<bool>,
    ) -> Result<LockBatch> {
        let mut lockfile = self
            .lockfile
            .lock()
            .expect("couldnt acquire lock, poisoned");

        let mut paths_ok: Vec<String> = vec![];
        let mut failures: Vec<LockFailure> = vec![];

        paths_ok.reserve_exact(paths.len());
        failures.reserve_exact(paths.len());

        for path in paths.iter() {
            let key = LocalFsKey::new(&repo, path);
            match lockfile.del_entry(&key, owner.clone(), force) {
                Ok(_) => {
                    paths_ok.push(path.clone());
                }
                Err(e) => failures.push(LockFailure {
                    path: path.clone(),
                    reason: e.to_string(),
                }),
            }
        }

        Ok(LockBatch::new(paths_ok, failures, owner))
    }
}
