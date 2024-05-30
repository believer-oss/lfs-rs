// Remove this when implemented
#![allow(dead_code, unused_variables)]
use super::{
    ListLocksResponse, Lock, LockBatch, LockFailure, LockStorage, OwnerInfo,
    VerifyLocksResponse,
};
use anyhow::bail;
use anyhow::{anyhow, Result};
use async_trait::async_trait;

use aws_sdk_dynamodb::{
    types::AttributeValue, types::DeleteRequest, types::KeysAndAttributes,
    types::PutRequest, types::WriteRequest, Client,
};
use base64::{engine::general_purpose, Engine as _};
use futures::future;

use uuid::Uuid;

use std::collections::{BTreeMap, HashMap};
use std::str;

#[derive(Clone, Debug, PartialEq)]
struct DynamoCursor {
    last_evaluated_key: HashMap<String, AttributeValue>,
}

impl DynamoCursor {
    fn from_cursor_string(cursor: String) -> Result<Self> {
        let cursor: BTreeMap<String, String> = serde_json::from_str(
            str::from_utf8(
                general_purpose::STANDARD
                    .decode(cursor.as_bytes())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        )?;

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
            cursor.insert(k, v.as_s().unwrap().to_string());
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
        let client: Client;
        match endpoint_url {
            Some(endpoint_url) => {
                let shared_config = aws_config::from_env()
                    .endpoint_url(endpoint_url)
                    .load()
                    .await;
                client = Client::new(&shared_config);
            }
            None => {
                let shared_config = aws_config::load_from_env().await;
                client = Client::new(&shared_config);
            }
        }

        DynamoLockStore { client, table_name }
    }

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
                    let items = builder.build();
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

                        log::error!(
                            "Failed to get locks {:?} due to SDK error: {:?}",
                            paths,
                            unified_error
                        );

                        bail!(
                            "SDK error fetching locks {:?}: {:?}",
                            paths,
                            unified_error
                        );
                    }
                    Ok(output) => {
                        if let Some(responses) = &output.responses {
                            if let Some(existing_locks) =
                                responses.get(&self.table_name)
                            {
                                for data in existing_locks.iter() {
                                    let extract = |v| {
                                        data.get(v)
                                            .unwrap()
                                            .as_s()
                                            .unwrap()
                                            .to_string()
                                    };

                                    all_locks.push(Lock {
                                        id: extract("id"),
                                        path: extract("path"),
                                        locked_at: extract("locked_at"),
                                        owner: Some(OwnerInfo {
                                            name: extract("owner"),
                                        }),
                                    });
                                }
                            }
                        }

                        if let Some(keys) = output.unprocessed_keys {
                            if !keys.is_empty() {
                                keys_and_attributes.push(keys);
                            }
                        }
                    }
                }
            }

            if !keys_and_attributes.is_empty() {
                iterations += 1;

                let base_delay_ms: u64 = 100;
                let total_delay_ms = base_delay_ms * iterations;
                let delay = std::time::Duration::from_millis(total_delay_ms);
                log::info!(
                    "get_locks_for_paths: Got {} unprocessed items, sleeping \
                     for {}ms",
                    keys_and_attributes.len(),
                    total_delay_ms
                );
                std::thread::sleep(delay);
            }
        }

        Ok(all_locks)
    }

    async fn write_batch(&self, mut writes: Vec<WriteRequest>) -> Result<()> {
        let mut iterations: u64 = 0;

        while !writes.is_empty() {
            let mut requests = vec![];

            // BatchWriteItem can write up to 25 items per request
            for chunk in writes.chunks_mut(25) {
                let request = self
                    .client
                    .batch_write_item()
                    .request_items(self.table_name.clone(), chunk.to_vec())
                    .send();
                requests.push(request);
            }
            writes.clear();

            let results = future::join_all(requests).await;
            for res in results.iter() {
                if let Err(e) = res {
                    log::error!(
                        "Failed to create locks due to SDK error: {:?}",
                        e
                    );
                    bail!("SDK error creating locks: {}", e);
                }
                if let Ok(output) = res {
                    if let Some(unprocessed) = &output.unprocessed_items {
                        if let Some(unprocessed_writes) =
                            unprocessed.get(&self.table_name)
                        {
                            writes.append(&mut unprocessed_writes.clone());
                        }
                    }
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
                log::info!(
                    "write_batch: Got {} unprocessed items, sleeping for {}ms",
                    writes.len(),
                    total_delay_ms
                );
                std::thread::sleep(delay);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl LockStorage for DynamoLockStore {
    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip(self), ret)
    )]
    async fn create_lock(
        &self,
        repo: String,
        path: String,
        owner: String,
    ) -> Result<Lock> {
        let id = Uuid::new_v4();
        let lock = Lock::new(id.to_string(), path.clone(), owner.clone());

        let request = self
            .client
            .get_item()
            .table_name(self.table_name.clone())
            .key("path", AttributeValue::S(path.clone()))
            .key("repo", AttributeValue::S(repo.clone()));

        let output = request.send().await?;
        if output.item().is_some() {
            return Err(anyhow!(super::LockStoreError::CreateConflict(lock)));
        }

        match self
            .client
            .put_item()
            .table_name(self.table_name.clone())
            .item("repo", AttributeValue::S(repo))
            .item("path", AttributeValue::S(path.clone()))
            .item("owner", AttributeValue::S(owner))
            .item("id", AttributeValue::S(lock.id.clone()))
            .item("locked_at", AttributeValue::S(lock.locked_at.clone()))
            .send()
            .await
        {
            Err(e) => {
                let unified_error: aws_sdk_dynamodb::Error = e.into();
                log::error!("Errror locking file {}: {}", path, unified_error);
                return Err(unified_error.into());
            }
            Ok(_) => Ok(lock),
        }
    }

    async fn create_locks(
        &self,
        repo: String,
        paths: Vec<String>,
        owner: String,
    ) -> Result<LockBatch> {
        // // Filter out any keys that already exist
        let existing_locks = self.get_locks_for_paths(&repo, &paths).await?;

        let mut filtered_paths: Vec<String> = paths;
        let failures: Vec<LockFailure> = existing_locks
            .iter()
            .map(|existing| {
                filtered_paths.remove(
                    filtered_paths.iter().position(|v| v.eq(v)).unwrap(),
                );
                let lock_owner: &str =
                    existing.owner.as_ref().map_or("", |v| &v.name);
                LockFailure {
                    path: existing.path.clone(),
                    reason: if owner.eq(lock_owner) {
                        "lock already held".to_string()
                    } else {
                        format!("lock held by user {}", lock_owner)
                    },
                }
            })
            .collect();

        // Write the locks to the db
        let batch = LockBatch::new(filtered_paths, failures, owner.clone());

        let ids: Vec<String> = batch
            .paths
            .iter()
            .map(|_| Uuid::new_v4().to_string())
            .collect();

        let mut writes: Vec<WriteRequest> = vec![];
        for (id, path) in std::iter::zip(ids, &batch.paths) {
            writes.push(
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .item("repo", AttributeValue::S(repo.clone()))
                            .item("path", AttributeValue::S(path.clone()))
                            .item("owner", AttributeValue::S(owner.clone()))
                            .item("id", AttributeValue::S(id.clone()))
                            .item(
                                "locked_at",
                                AttributeValue::S(batch.timestamp.clone()),
                            )
                            .build(),
                    )
                    .build(),
            );
        }

        self.write_batch(writes).await?;

        Ok(batch)
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip(self), ret)
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
            request = request.limit(limit.try_into().unwrap())
        }

        if let Some(cursor) = cursor {
            request = request.set_exclusive_start_key(Some(
                DynamoCursor::from_cursor_string(cursor)
                    .unwrap()
                    .last_evaluated_key,
            ));
        }

        let results = request.send().await?;
        if let Some(items) = &results.items {
            let locks: Vec<Lock> = items
                .iter()
                .map(|lock| Lock {
                    id: lock["id"].as_s().unwrap().to_string(),
                    path: lock["path"].as_s().unwrap().to_string(),
                    locked_at: lock["locked_at"].as_s().unwrap().to_string(),
                    owner: Some(OwnerInfo {
                        name: lock["owner"].as_s().unwrap().to_string(),
                    }),
                })
                .collect();

            let next_cursor = match results.last_evaluated_key() {
                Some(last_evaluated_key) => {
                    let dynamo_cursor = DynamoCursor {
                        last_evaluated_key: last_evaluated_key.clone(),
                    };
                    Some(dynamo_cursor.to_cursor_string().unwrap())
                }
                None => None,
            };

            // if there are no locks, but there is a cursor, we need to run another request using
            // the cursor
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

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip(self), ret)
    )]
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
            request = request.limit(limit.try_into().unwrap())
        }

        if let Some(cursor) = cursor {
            request = request.set_exclusive_start_key(Some(
                DynamoCursor::from_cursor_string(cursor)
                    .unwrap()
                    .last_evaluated_key,
            ));
        }

        let results = request.send().await?;
        if let Some(items) = &results.items {
            let mut ours: Vec<Lock> = Vec::new();
            let mut theirs: Vec<Lock> = Vec::new();

            for lock in items.iter() {
                let lock = Lock {
                    id: lock["id"].as_s().unwrap().to_string(),
                    path: lock["path"].as_s().unwrap().to_string(),
                    locked_at: lock["locked_at"].as_s().unwrap().to_string(),
                    owner: Some(OwnerInfo {
                        name: lock["owner"].as_s().unwrap().to_string(),
                    }),
                };

                if lock.owner.as_ref().unwrap().name == owner {
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
                        Some(dynamo_cursor.to_cursor_string().unwrap())
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
        tracing::instrument(level = "info", skip(self), ret)
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
            .expression_attribute_values(":id", AttributeValue::S(id));

        let output = request.send().await?;

        if let Some(item) = output.items().unwrap().first() {
            let lock = Lock {
                id: item["id"].as_s().unwrap().to_string(),
                path: item["path"].as_s().unwrap().to_string(),
                locked_at: item["locked_at"].as_s().unwrap().to_string(),
                owner: Some(OwnerInfo {
                    name: item["owner"].as_s().unwrap().to_string(),
                }),
            };

            if lock.owner.as_ref().unwrap().name == owner
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
            Err(anyhow!(super::LockStoreError::InternalServerError))
        }
    }

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
                            .build(),
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
