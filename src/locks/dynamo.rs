// Remove this when implemented
#![allow(dead_code, unused_variables)]
use super::{
    ListLocksResponse, Lock, LockBatch, LockFailure, LockStorage,
    LockStoreError, OwnerInfo, VerifyLocksResponse,
};
use anyhow::bail;
use anyhow::{Result, anyhow};
use async_trait::async_trait;

use aws_sdk_dynamodb::{
    Client, types::AttributeValue, types::DeleteRequest,
    types::KeysAndAttributes, types::WriteRequest,
};
use base64::{Engine as _, engine::general_purpose};
use futures::{StreamExt, future, stream};

use uuid::Uuid;

use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::{
    BatchWriteItemError, BatchWriteItemOutput,
};
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use std::collections::{BTreeMap, HashMap};
use std::str;
use tracing::{info, instrument};

/// Reads a lock from its item. An item missing an attribute is an error
/// rather than a panic: one bad item would otherwise break every listing of
/// its repo.
fn lock_from_item(item: &HashMap<String, AttributeValue>) -> Result<Lock> {
    let attr = |name: &str| -> Result<String> {
        item.get(name)
            .and_then(|value| value.as_s().ok())
            .cloned()
            .ok_or_else(|| {
                anyhow!(LockStoreError::InternalServerError(format!(
                    "lock item without a string `{name}`"
                )))
            })
    };
    Ok(Lock {
        id: attr("id")?,
        path: attr("path")?,
        locked_at: attr("locked_at")?,
        owner: Some(OwnerInfo {
            name: attr("owner")?,
        }),
    })
}

/// A client's page size, as DynamoDB takes it: at least 1, and at most
/// `i32::MAX`.
fn page_limit(limit: u64) -> i32 {
    i32::try_from(limit.max(1)).unwrap_or(i32::MAX)
}

#[derive(Clone, Debug, PartialEq)]
struct DynamoCursor {
    last_evaluated_key: HashMap<String, AttributeValue>,
}

impl DynamoCursor {
    /// Parses a cursor that a client sent back. Clients can send anything, so
    /// a bad one is a [`LockStoreError::BadRequest`].
    fn from_cursor_string(cursor: String) -> Result<Self> {
        let bad = |what: &str| {
            anyhow!(LockStoreError::BadRequest(format!(
                "invalid cursor: {what}"
            )))
        };
        let json = general_purpose::STANDARD
            .decode(cursor.as_bytes())
            .map_err(|_| bad("not base64"))?;
        let cursor: BTreeMap<String, String> =
            serde_json::from_slice(&json).map_err(|_| bad("not a cursor"))?;

        Ok(DynamoCursor::from(cursor))
    }

    fn to_cursor_string(&self) -> Result<String> {
        Ok(general_purpose::STANDARD.encode(serde_json::to_string(
            &BTreeMap::<String, String>::from(&(*self).clone()),
        )?))
    }
}

// Rather than try to serialize the entire AttributeValue, we'll
// assume that all keys are strings for now.
impl From<BTreeMap<String, String>> for DynamoCursor {
    fn from(cursor: BTreeMap<String, String>) -> Self {
        let mut dynamo_cursor = DynamoCursor {
            last_evaluated_key: HashMap::new(),
        };
        for (k, v) in cursor {
            dynamo_cursor
                .last_evaluated_key
                .insert(k, AttributeValue::S(v));
        }

        dynamo_cursor
    }
}

impl From<&DynamoCursor> for BTreeMap<String, String> {
    fn from(dynamo_cursor: &DynamoCursor) -> Self {
        let last_evaluated_key = dynamo_cursor.last_evaluated_key.clone();

        let mut cursor = BTreeMap::new();
        for (k, v) in last_evaluated_key {
            // The table's keys are all strings.
            if let Ok(v) = v.as_s() {
                cursor.insert(k, v.clone());
            }
        }

        cursor
    }
}

#[derive(Debug)]
pub struct DynamoLockStore {
    client: Client,
    table_name: String,
}

impl DynamoLockStore {
    pub async fn new(table_name: String, endpoint_url: Option<&str>) -> Self {
        let mut shared_config =
            aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12());
        if let Some(endpoint_url) = endpoint_url {
            shared_config = shared_config.endpoint_url(endpoint_url)
        };
        let sdk_config = shared_config.load().await;

        Self::from_config(&sdk_config, table_name)
    }

    /// Creates the lock store from an already loaded AWS configuration.
    pub fn from_config(
        sdk_config: &aws_config::SdkConfig,
        table_name: String,
    ) -> Self {
        DynamoLockStore {
            client: Client::new(sdk_config),
            table_name,
        }
    }

    #[instrument(
        level = "info",
        err,
        skip_all,
        fields(paths = paths.len() as i64)
    )]
    async fn get_locks_for_paths(
        &self,
        repo: &str,
        paths: &[String],
    ) -> Result<Vec<Lock>> {
        // BatchGetItem can retrieve up to 100 items per request
        let mut keys_and_attributes: Vec<HashMap<String, KeysAndAttributes>> =
            paths
                .chunks(100)
                .map(|paths_chunk| {
                    let mut builder = KeysAndAttributes::builder();
                    for path in paths_chunk {
                        let mut map = HashMap::<String, AttributeValue>::new();
                        map.insert(
                            "path".to_string(),
                            AttributeValue::S(path.clone()),
                        );
                        map.insert(
                            "repo".to_string(),
                            AttributeValue::S(repo.to_string()),
                        );
                        builder = builder.keys(map);
                    }
                    let items = builder.build().expect("Failed to build keys");
                    HashMap::<String, KeysAndAttributes>::from([(
                        self.table_name.clone(),
                        items,
                    )])
                })
                .collect();

        let mut all_locks: Vec<Lock> = vec![];

        let mut iterations: u64 = 0;

        while !keys_and_attributes.is_empty() {
            let mut requests = vec![];
            requests.reserve_exact(keys_and_attributes.len());

            while let Some(items) = keys_and_attributes.pop() {
                let request = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(items))
                    .send();
                requests.push(request);
            }

            let mut results = future::join_all(requests).await;

            while let Some(res) = results.pop() {
                match res {
                    Err(e) => {
                        let unified_error: aws_sdk_dynamodb::Error = e.into();
                        bail!("SDK error fetching locks: {:?}", unified_error);
                    }
                    Ok(output) => {
                        if let Some(responses) = &output.responses
                            && let Some(existing_locks) =
                                responses.get(&self.table_name)
                        {
                            for data in existing_locks.iter() {
                                all_locks.push(lock_from_item(data)?);
                            }
                        }

                        if let Some(keys) = output.unprocessed_keys
                            && !keys.is_empty()
                        {
                            keys_and_attributes.push(keys);
                        }
                    }
                }
            }

            if !keys_and_attributes.is_empty() {
                iterations += 1;

                let base_delay_ms: u64 = 100;
                let total_delay_ms = base_delay_ms * iterations;
                let delay = std::time::Duration::from_millis(total_delay_ms);
                tracing::info!(
                    "get_locks_for_paths: Got {} unprocessed items, sleeping \
                     for {}ms",
                    keys_and_attributes.len(),
                    total_delay_ms
                );
                tokio::time::sleep(delay).await;
            }
        }

        Ok(all_locks)
    }

    #[instrument(
        level = "info",
        err,
        skip_all,
        fields(writes = writes.len() as i64)
    )]
    async fn write_batch(&self, mut writes: Vec<WriteRequest>) -> Result<()> {
        let mut iterations: u64 = 0;

        while !writes.is_empty() {
            let mut results: Vec<
                Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>>,
            > = vec![];

            // We send requests in batches of 200 items (8 x 25), to avoid
            // DynamoDB throttling issues.
            // https://stackoverflow.com/questions/66019805/dynamodb-throttlingexception-issues
            for chunk in writes.chunks_mut(200) {
                let mut requests = vec![];

                // BatchWriteItem can write up to 25 items per request
                for chunk in chunk.chunks_mut(25) {
                    let request = self
                        .client
                        .batch_write_item()
                        .request_items(self.table_name.clone(), chunk.to_vec())
                        .send();
                    requests.push(request);
                }

                let res = future::join_all(requests).await;
                results.extend(res);

                // sleep 1s
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            writes.clear();

            for res in results {
                if let Err(e) = res {
                    let unified_error: aws_sdk_dynamodb::Error = e.into();
                    bail!("SDK error creating locks: {:?}", unified_error);
                }
                if let Ok(output) = res
                    && let Some(unprocessed) = &output.unprocessed_items
                    && let Some(unprocessed_writes) =
                        unprocessed.get(&self.table_name)
                {
                    writes.append(&mut unprocessed_writes.clone());
                }
            }

            // Backoff to retry submitting unprocessed writes, as
            // recommended by the documentation:
            //
            // If DynamoDB returns any unprocessed items, you should retry the
            // batch operation on those items. However, we strongly recommend
            // that you use an exponential backoff algorithm. If you retry the
            // batch operation immediately, the underlying read or write
            // requests can still fail due to throttling on the individual
            // tables. If you delay the batch operation using exponential
            // backoff, the individual requests in the batch are much more
            // likely to succeed.
            if !writes.is_empty() {
                iterations += 1;

                let base_delay_ms: u64 = 100;
                let total_delay_ms = base_delay_ms * iterations;
                let delay = std::time::Duration::from_millis(total_delay_ms);
                info!(
                    "write_batch: Got {} unprocessed items, sleeping for {}ms",
                    writes.len(),
                    total_delay_ms
                );
                tokio::time::sleep(delay).await;
            }
        }

        Ok(())
    }
}

/// How many locks of a batch are written at once.
const CONCURRENT_LOCK_WRITES: usize = 16;

impl DynamoLockStore {
    /// The lock on `path`, read consistently, so that a lock just written is
    /// seen.
    async fn get_lock(&self, repo: &str, path: &str) -> Result<Option<Lock>> {
        let output = self
            .client
            .get_item()
            .table_name(self.table_name.clone())
            .key("repo", AttributeValue::S(repo.to_string()))
            .key("path", AttributeValue::S(path.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(aws_sdk_dynamodb::Error::from)?;
        output.item().map(lock_from_item).transpose()
    }

    /// Locks `path` for `owner`, or returns the lock that already holds it.
    ///
    /// The write is conditional on there being no lock on the path, so of
    /// several clients locking it at once exactly one succeeds. Checking first
    /// and writing afterwards let them all "succeed", the last write winning.
    async fn put_lock(
        &self,
        repo: &str,
        path: &str,
        owner: &str,
    ) -> Result<Result<Lock, Lock>> {
        // At most twice: again if the lock that blocked this one was released
        // before it could be read.
        for _ in 0..2 {
            let lock = Lock::new(
                Uuid::new_v4().to_string(),
                path.to_string(),
                owner.to_string(),
            );
            let written = self
                .client
                .put_item()
                .table_name(self.table_name.clone())
                .item("repo", AttributeValue::S(repo.to_string()))
                .item("path", AttributeValue::S(path.to_string()))
                .item("owner", AttributeValue::S(owner.to_string()))
                .item("id", AttributeValue::S(lock.id.clone()))
                .item("locked_at", AttributeValue::S(lock.locked_at.clone()))
                .condition_expression("attribute_not_exists(#path)")
                .expression_attribute_names("#path", "path")
                .send()
                .await;

            match written {
                Ok(_) => return Ok(Ok(lock)),
                Err(err)
                    if matches!(
                        err.as_service_error(),
                        Some(PutItemError::ConditionalCheckFailedException(_))
                    ) =>
                {
                    if let Some(existing) = self.get_lock(repo, path).await? {
                        return Ok(Err(existing));
                    }
                }
                Err(err) => {
                    let err = aws_sdk_dynamodb::Error::from(err);
                    tracing::error!("Error locking file {}: {}", path, err);
                    return Err(err.into());
                }
            }
        }

        Err(anyhow!(LockStoreError::InternalServerError(format!(
            "could not lock '{path}'"
        ))))
    }
}

#[async_trait]
impl LockStorage for DynamoLockStore {
    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip_all, fields(lfs.path = %path))
    )]
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock> {
        match self.put_lock(&repo, &path, &owner).await? {
            Ok(lock) => Ok(lock),
            Err(existing) => {
                Err(anyhow!(super::LockStoreError::CreateConflict(existing)))
            }
        }
    }

    /// Locks each path with its own conditional write, several at once.
    /// `BatchWriteItem` can't be conditional, so it could overwrite a lock
    /// taken after any check made first.
    #[cfg_attr(
        feature = "otel",
        tracing::instrument(
            level = "info",
            skip_all,
            fields(paths = paths.len() as i64)
        )
    )]
    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch> {
        let (repo, owner_ref) = (&repo, &owner);
        let results: Vec<(String, Result<Result<Lock, Lock>>)> =
            stream::iter(paths)
                .map(|path| async move {
                    let result = self.put_lock(repo, &path, owner_ref).await;
                    (path, result)
                })
                .buffered(CONCURRENT_LOCK_WRITES)
                .collect()
                .await;

        let mut locked = Vec::new();
        let mut failures = Vec::new();
        for (path, result) in results {
            let reason = match result {
                Ok(Ok(_)) => {
                    locked.push(path);
                    continue;
                }
                Ok(Err(existing)) => {
                    let holder =
                        existing.owner.as_ref().map_or("", |o| &o.name);
                    if holder == owner {
                        "lock already held".to_string()
                    } else {
                        format!("lock held by user {holder}")
                    }
                }
                Err(err) => err.to_string(),
            };
            failures.push(LockFailure { path, reason });
        }

        Ok(LockBatch::new(locked, failures, owner))
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(
            level = "info",
            skip_all,
            fields(lfs.path = path.as_deref(), lfs.lock_id = id.as_deref())
        )
    )]
    async fn list_locks(
        &self,
        repo: String,
        path: Option<String>,
        id: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<ListLocksResponse> {
        let mut request = self
            .client
            .query()
            .table_name(self.table_name.clone())
            .index_name("creation-index")
            .key_condition_expression("repo = :repo")
            .projection_expression("id, #path, locked_at, #owner")
            .expression_attribute_names("#path", "path")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_values(
                ":repo",
                AttributeValue::S(repo.clone()),
            );

        if let Some(path) = path.clone() {
            request = request.filter_expression("#path = :path");
            request = request.expression_attribute_values(
                ":path",
                AttributeValue::S(path.clone()),
            );
        }

        if let Some(limit) = limit {
            request = request.limit(page_limit(limit))
        }

        if let Some(cursor) = cursor {
            request = request.set_exclusive_start_key(Some(
                DynamoCursor::from_cursor_string(cursor)?.last_evaluated_key,
            ));
        }

        let results = request.send().await?;
        if let Some(items) = &results.items {
            let locks = items
                .iter()
                .map(lock_from_item)
                .collect::<Result<Vec<Lock>>>()?;

            let next_cursor = match results.last_evaluated_key() {
                Some(last_evaluated_key) => {
                    let dynamo_cursor = DynamoCursor {
                        last_evaluated_key: last_evaluated_key.clone(),
                    };
                    Some(dynamo_cursor.to_cursor_string()?)
                }
                None => None,
            };

            // if there are no locks, but there is a cursor, we need to run
            // another request using the cursor
            if locks.is_empty() && next_cursor.is_some() {
                return self
                    .list_locks(
                        repo.clone(),
                        path.clone(),
                        id,
                        next_cursor,
                        limit,
                    )
                    .await;
            }

            Ok(ListLocksResponse { locks, next_cursor })
        } else {
            Ok(ListLocksResponse {
                locks: vec![],
                next_cursor: None,
            })
        }
    }

    #[cfg_attr(feature = "otel", tracing::instrument(level = "info", skip_all))]
    async fn verify_locks(
        &self,
        repo: String,
        owner: String,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<VerifyLocksResponse> {
        let mut request = self
            .client
            .query()
            .table_name(self.table_name.clone())
            .index_name("creation-index")
            .key_condition_expression("repo = :repo")
            .projection_expression("id, #path, locked_at, #owner")
            .expression_attribute_names("#path", "path")
            .expression_attribute_names("#owner", "owner")
            .expression_attribute_values(":repo", AttributeValue::S(repo));

        if let Some(limit) = limit {
            request = request.limit(page_limit(limit))
        }

        if let Some(cursor) = cursor {
            request = request.set_exclusive_start_key(Some(
                DynamoCursor::from_cursor_string(cursor)?.last_evaluated_key,
            ));
        }

        let results = request.send().await?;
        if let Some(items) = &results.items {
            let mut ours: Vec<Lock> = Vec::new();
            let mut theirs: Vec<Lock> = Vec::new();

            for lock in items.iter() {
                let lock = lock_from_item(lock)?;

                if lock.owner.as_ref().is_some_and(|o| o.name == owner) {
                    ours.push(lock);
                } else {
                    theirs.push(lock);
                }
            }
            Ok(VerifyLocksResponse {
                ours,
                theirs,
                next_cursor: match results.last_evaluated_key() {
                    Some(last_evaluated_key) => {
                        let dynamo_cursor = DynamoCursor {
                            last_evaluated_key: last_evaluated_key.clone(),
                        };
                        Some(dynamo_cursor.to_cursor_string()?)
                    }
                    None => None,
                },
            })
        } else {
            Ok(VerifyLocksResponse {
                ours: vec![],
                theirs: vec![],
                next_cursor: None,
            })
        }
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(
            level = "info",
            skip_all,
            fields(lfs.lock_id = %id, force = force.unwrap_or_default())
        )
    )]
    async fn release_lock(
        &self,
        repo: String,
        owner: String,
        id: String,
        force: Option<bool>,
    ) -> Result<Lock> {
        let request = self
            .client
            .query()
            .table_name(self.table_name.clone())
            .index_name("id-index")
            .projection_expression("id, #path, locked_at, #owner")
            .expression_attribute_names("#path", "path")
            .expression_attribute_names("#owner", "owner")
            .key_condition_expression("repo = :repo and id = :id")
            .expression_attribute_values(
                ":repo",
                AttributeValue::S(repo.clone()),
            )
            .expression_attribute_values(":id", AttributeValue::S(id.clone()));

        let output = request.send().await?;

        if let Some(item) = output.items().first() {
            let lock = lock_from_item(item)?;

            if lock.owner.as_ref().is_some_and(|o| o.name == owner)
                || force.unwrap_or(false)
            {
                self.client
                    .delete_item()
                    .table_name(self.table_name.clone())
                    .key("repo", AttributeValue::S(repo))
                    .key("path", AttributeValue::S(lock.path.clone()))
                    .send()
                    .await?;
            }

            Ok(lock)
        } else {
            Err(anyhow!(super::LockStoreError::DeleteNotFound(id)))
        }
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(
            level = "info",
            skip_all,
            fields(
                paths = paths.len() as i64,
                force = force.unwrap_or_default()
            )
        )
    )]
    async fn release_locks(
        &self,
        repo: String,
        owner: String,
        paths: Vec<String>,
        force: Option<bool>,
    ) -> Result<LockBatch> {
        let existing_locks = self.get_locks_for_paths(&repo, &paths).await?;

        let mut filtered_paths: Vec<String> = vec![];
        let mut failures: Vec<LockFailure> = vec![];

        for path in paths.iter() {
            if !existing_locks.iter().any(|v| v.path.eq(path)) {
                failures.push(LockFailure {
                    path: path.clone(),
                    reason: "lock doesn't exist".to_string(),
                });
            }
        }

        let force = force.unwrap_or(false);
        for lock in existing_locks.iter() {
            let lock_owner = lock.owner.as_ref().map_or("", |v| &v.name);
            if owner.eq(lock_owner) || force {
                filtered_paths.push(lock.path.clone());
            } else {
                failures.push(LockFailure {
                    path: lock.path.clone(),
                    reason: format!("lock held by user {}", lock_owner),
                });
            }
        }

        // Remove the filtered locks from the db
        let batch = LockBatch::new(filtered_paths, failures, owner.clone());

        let mut writes: Vec<WriteRequest> = vec![];
        for path in batch.paths.iter() {
            writes.push(
                WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .key("repo", AttributeValue::S(repo.clone()))
                            .key("path", AttributeValue::S(path.clone()))
                            .build()
                            .expect("Failed to build delete request"),
                    )
                    .build(),
            );
        }

        self.write_batch(writes).await?;

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_from_string() {
        // echo -n '{"repo":"foo","path":"bar"}' | base64
        let cursor = DynamoCursor::from_cursor_string(
            "eyJyZXBvIjoiZm9vIiwicGF0aCI6ImJhciJ9".to_string(),
        )
        .unwrap();

        assert_eq!(cursor.last_evaluated_key["repo"].as_s().unwrap(), "foo");
        assert_eq!(cursor.last_evaluated_key["path"].as_s().unwrap(), "bar");
    }

    #[test]
    fn test_string_from_cursor() {
        let cursor = DynamoCursor {
            last_evaluated_key: {
                let mut last_evaluated_key = HashMap::new();
                last_evaluated_key.insert(
                    "repo".to_string(),
                    AttributeValue::S("foo".to_string()),
                );
                last_evaluated_key.insert(
                    "path".to_string(),
                    AttributeValue::S("bar".to_string()),
                );
                last_evaluated_key
            },
        };

        // echo -n '{"path":"bar","repo":"foo"}' | base64
        assert_eq!(
            cursor.to_cursor_string().unwrap(),
            "eyJwYXRoIjoiYmFyIiwicmVwbyI6ImZvbyJ9"
        );
    }
}
