// Copyright (c) 2019 Jason White
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

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    future::{self, BoxFuture},
    TryStreamExt,
};

use http::{self, header, HeaderMap, StatusCode, Uri};
use http_body_util::{BodyDataStream, BodyExt, StreamBody};
use hyper::{
    self,
    body::{Frame, Incoming},
    Method, Request, Response,
};
use tower::Service;
use url::form_urlencoded;

use askama::Template;
use bytes::Bytes;

use crate::auth::UserRepoInfo;
use crate::error::Error;
use crate::hyperext::RequestExt;
use crate::lfs;
use crate::locks::{
    CreateLockBatchRequest, CreateLockRequest, ListLocksResponse, Lock,
    LockBatchOuter, LockOuter, LockStorage, LockStoreError, OwnerInfo,
    ReleaseLockBatchRequest, ReleaseLockRequest, VerifyLocksRequest,
    VerifyLocksResponse,
};
use crate::storage::{LFSObject, Namespace, Storage, StorageKey};
use crate::{empty, from_json, full, into_json};

#[cfg(feature = "otel")]
use crate::util::RedactedHeaders;
#[cfg(feature = "otel")]
use opentelemetry::trace::FutureExt;
#[cfg(feature = "otel")]
use tracing::instrument;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

const UPLOAD_EXPIRATION: Duration = Duration::from_secs(30 * 60);

fn handle_lock_error_response(err: anyhow::Error) -> (StatusCode, BoxBody) {
    match err.downcast_ref::<LockStoreError>() {
        Some(e @ LockStoreError::CreateConflict(l)) => (
            StatusCode::CONFLICT,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: Some(l.clone()),
                    message: e.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
        Some(LockStoreError::NotImplemented) => {
            (StatusCode::NOT_FOUND, empty())
        }
        #[cfg(feature = "redis")]
        Some(LockStoreError::RedisError(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: None,
                    message: e.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
        Some(LockStoreError::DeleteNotFound(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: None,
                    message: e.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
        Some(LockStoreError::LockNotFound(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: None,
                    message: e.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
        Some(LockStoreError::InternalServerError(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: None,
                    message: e.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            full(
                into_json(&lfs::BatchResponseError {
                    locks: None,
                    message: err.to_string(),
                    documentation_url: None,
                    request_id: None,
                })
                .unwrap_or_default(),
            ),
        ),
    }
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate<'a> {
    title: &'a str,
    api: Uri,
}

#[derive(Clone)]
pub struct App<S, L> {
    storage: S,
    locks: L,
}

impl<S, L> App<S, L> {
    pub fn new(storage: S, locks: L) -> Self {
        App { storage, locks }
    }
}

pub type Req = Request<Incoming>;
pub type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, Error>;

impl<S, L> App<S, L>
where
    S: Storage + Send + Sync,
    S::Error: Into<Error>,
    L: LockStorage + Send + Sync,
    Error: From<S::Error>,
{
    /// Handles the index route.
    #[cfg_attr(feature = "otel", instrument(level = "debug", skip(req)))]
    fn index(req: Req) -> Result<Response<BoxBody>, Error> {
        let template = IndexTemplate {
            title: "Rudolfs",
            api: req.base_uri().path_and_query("/api").build()?,
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(full(template.render()?))?)
    }

    /// Generates a "404 not found" response.
    #[cfg_attr(feature = "otel", instrument(level = "info", skip(_req)))]
    fn not_found(_req: Req) -> Result<Response<BoxBody>, Error> {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full("Not found"))?)
    }

    /// Generates a "403 forbidden" response.
    #[cfg_attr(feature = "otel", instrument(level = "info", skip(_req)))]
    fn forbidden(_req: Req) -> Result<Response<BoxBody>, Error> {
        Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("Lfs-Authenticate", "Basic realm=\"GitHub\"")
            .body(empty())?)
    }

    /// Handles `/api` routes.
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(storage, locks, req))
    )]
    async fn api(
        storage: S,
        locks: L,
        req: Request<Incoming>,
    ) -> Result<Response<BoxBody>, Error> {
        let mut parts = req.uri().path().split('/').filter(|s| !s.is_empty());

        // Skip over the '/api' part.
        assert_eq!(parts.next(), Some("api"));

        // Extract the namespace.
        let namespace = match (parts.next(), parts.next()) {
            (Some(org), Some(project)) => {
                Namespace::new(org.into(), project.into())
            }
            _ => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(full("Missing org/project in URL"))?)
            }
        };

        match parts.next() {
            Some("object") => {
                // Upload or download a single object.
                let oid = parts.next().and_then(|x| x.parse::<lfs::Oid>().ok());
                let oid = match oid {
                    Some(oid) => oid,
                    None => {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(full("Missing OID parameter."))?)
                    }
                };

                let key = StorageKey::new(namespace, oid);

                match *req.method() {
                    Method::GET => Self::download(storage, req, key).await,
                    Method::PUT => Self::upload(storage, req, key).await,
                    _ => Self::not_found(req),
                }
            }
            Some("objects") => match (req.method(), parts.next()) {
                (&Method::POST, Some("batch")) => {
                    Self::batch(storage, req, namespace).await
                }
                (&Method::POST, Some("verify")) => {
                    Self::verify(storage, req, namespace).await
                }
                _ => Self::not_found(req),
            },
            Some("locks") => {
                let user = req.extensions().get::<UserRepoInfo>().cloned();
                if let Some(user) = user {
                    match (req.method(), parts.next()) {
                        (&Method::GET, None) => {
                            if !user.permissions.unwrap_or_default().pull {
                                return Self::forbidden(req);
                            }

                            Self::list_locks(locks, req, namespace).await
                        }
                        (&Method::POST, None) => {
                            if !user.permissions.unwrap_or_default().push {
                                return Self::forbidden(req);
                            }

                            Self::create_lock(
                                locks,
                                req,
                                namespace,
                                user.username.unwrap(),
                            )
                            .await
                        }
                        (&Method::POST, Some("batch")) => {
                            if !user.permissions.unwrap_or_default().push {
                                return Self::forbidden(req);
                            }

                            match parts.next() {
                                Some("lock") => {
                                    Self::create_lock_batch(
                                        locks,
                                        req,
                                        namespace,
                                        user.username.unwrap(),
                                    )
                                    .await
                                }
                                Some("unlock") => {
                                    Self::release_lock_batch(
                                        locks,
                                        req,
                                        namespace,
                                        user.username.unwrap(),
                                    )
                                    .await
                                }
                                _ => Self::not_found(req),
                            }
                        }
                        (&Method::POST, Some("verify")) => {
                            if !user.permissions.unwrap_or_default().push {
                                return Self::forbidden(req);
                            }

                            Self::list_locks_for_verification(
                                locks,
                                req,
                                namespace,
                                user.username.unwrap(),
                            )
                            .await
                        }
                        (&Method::POST, Some(id)) => {
                            if !user.permissions.unwrap_or_default().push {
                                return Self::forbidden(req);
                            }

                            match parts.next() {
                                Some("unlock") => {
                                    let id = id.to_owned();
                                    Self::release_lock(
                                        locks,
                                        req,
                                        namespace,
                                        id,
                                        user.username.unwrap(),
                                    )
                                    .await
                                }
                                _ => Self::not_found(req),
                            }
                        }

                        _ => Self::not_found(req),
                    }
                } else {
                    Self::not_found(req)
                }
            }
            _ => Self::not_found(req),
        }
    }

    /// Downloads a single LFS object.
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(storage, _req))
    )]
    async fn download(
        storage: S,
        _req: Req,
        key: StorageKey,
    ) -> Result<Response<BoxBody>, Error> {
        if let Some(object) = storage.get(&key).await? {
            let len = &object.len().to_string();
            let body_stream = StreamBody::new(
                object
                    .stream()
                    .map_ok(Frame::data)
                    .map_err(|e: std::io::Error| e.into()),
            );
            let boxed_body = body_stream.boxed_unsync();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, len)
                .body(boxed_body)?)
        } else {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(empty())?)
        }
    }

    /// Uploads a single LFS object.
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(storage, req))
    )]
    async fn upload(
        storage: S,
        req: Request<Incoming>,
        key: StorageKey,
    ) -> Result<Response<BoxBody>, Error> {
        let len = req
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let len = match len {
            Some(len) => len,
            None => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(full("Invalid Content-Length header."))
                    .map_err(Into::into);
            }
        };

        // Verify the SHA256 of the uploaded object as it is being uploaded.
        let body = req.into_body();
        let stream = BodyDataStream::new(body)
            .try_filter_map(|chunk| async { Ok(Some(chunk)) })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        let object = LFSObject::new(len, Box::pin(stream));

        storage.put(key, object).await?;

        Ok(Response::builder().status(StatusCode::OK).body(empty())?)
    }

    /// Verifies that an LFS object exists on the server.
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(storage, req))
    )]
    async fn verify(
        storage: S,
        req: Request<Incoming>,
        namespace: Namespace,
    ) -> Result<Response<BoxBody>, Error> {
        let val: lfs::VerifyRequest = from_json(req.into_body()).await?;
        let key = StorageKey::new(namespace, val.oid);

        if let Some(size) = storage.size(&key).await? {
            if size == val.size {
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(empty())?);
            }
        }

        // Object doesn't exist or the size is incorrect.
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(empty())?)
    }

    /// Batch API endpoint for the Git LFS server spec.
    ///
    /// See also:
    /// https://github.com/git-lfs/git-lfs/blob/master/docs/api/batch.md
    #[cfg_attr(
        feature = "otel",
        instrument(level = "info", skip(storage, req))
    )]
    async fn batch(
        storage: S,
        req: Request<Incoming>,
        namespace: Namespace,
    ) -> Result<Response<BoxBody>, Error> {
        // Get the host name and scheme.
        let uri = req.base_uri().path_and_query("/").build()?;
        let headers = req.headers().clone();

        match from_json::<lfs::BatchRequest>(req.into_body()).await {
            Ok(val) => {
                let operation = val.operation;

                // For each object, check if it exists in the storage
                // backend.
                let objects = val.objects.into_iter().map(|object| {
                    let uri = uri.clone();
                    let key = StorageKey::new(namespace.clone(), object.oid);

                    async {
                        let size = storage.size(&key).await;

                        let (namespace, _) = key.into_parts();
                        Ok(basic_response(
                            uri, &headers, &storage, object, operation, size,
                            namespace,
                        )
                        .await)
                    }
                });

                let objects = future::try_join_all(objects).await?;
                let mut transfer = Some(lfs::Transfer::Basic);
                if let Some(transfers) = val.transfers {
                    if transfers.contains(&lfs::Transfer::LfsRs) {
                        transfer = Some(lfs::Transfer::LfsRs)
                    }
                }
                let response = lfs::BatchResponse { transfer, objects };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(full(into_json(&response)?))?)
            }
            Err(err) => {
                let response = lfs::BatchResponseError {
                    locks: None,
                    message: err.to_string(),
                    documentation_url: None,
                    request_id: None,
                };

                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(full(into_json(&response)?))?)
            }
        }
    }

    async fn list_locks(
        locks: L,
        req: Req,
        namespace: Namespace,
    ) -> Result<Response<BoxBody>, Error> {
        let params: Option<HashMap<String, String>> =
            req.uri().query().map(|q| {
                form_urlencoded::parse(q.as_bytes())
                    .into_owned()
                    .collect::<HashMap<String, String>>()
            });

        let path = params.as_ref().and_then(|p| p.get("path").cloned());
        let id = params.as_ref().and_then(|p| p.get("id").cloned());
        let cursor = params.as_ref().and_then(|p| p.get("cursor").cloned());
        let limit = params
            .as_ref()
            .and_then(|p| p.get("limit").cloned())
            .map(|n| n.parse::<u64>().unwrap_or(0));

        match locks
            .list_locks(namespace.to_string(), path, id, cursor, limit)
            .await
        {
            Ok(locks) => {
                let resp = ListLocksResponse {
                    locks: locks.locks,
                    next_cursor: locks.next_cursor,
                };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }

    async fn create_lock(
        locks: L,
        req: Req,
        namespace: Namespace,
        owner: String,
    ) -> Result<Response<BoxBody>, Error> {
        let val: CreateLockRequest = from_json(req.into_body()).await?;

        match locks
            .create_lock(namespace.to_string(), val.path, owner)
            .await
        {
            Ok(lock) => {
                let resp = LockOuter {
                    lock: Lock {
                        id: lock.id.to_string(),
                        path: lock.path,
                        locked_at: lock.locked_at.to_string(),
                        owner: lock
                            .owner
                            .map(|owner| OwnerInfo { name: owner.name }),
                    },
                };

                Ok(Response::builder()
                    .status(StatusCode::CREATED)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }

    async fn create_lock_batch(
        locks: L,
        req: Req,
        namespace: Namespace,
        owner: String,
    ) -> Result<Response<BoxBody>, Error> {
        let val: CreateLockBatchRequest = from_json(req.into_body()).await?;

        match locks
            .create_locks(namespace.to_string(), val.paths, owner)
            .await
        {
            Ok(batch) => {
                let resp = LockBatchOuter { batch };

                Ok(Response::builder()
                    .status(StatusCode::CREATED)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }

    async fn list_locks_for_verification(
        locks: L,
        req: Req,
        namespace: Namespace,
        owner: String,
    ) -> Result<Response<BoxBody>, Error> {
        let val: VerifyLocksRequest = from_json(req.into_body()).await?;
        match locks
            .verify_locks(namespace.to_string(), owner, val.cursor, val.limit)
            .await
        {
            Ok(locks) => {
                let resp = VerifyLocksResponse {
                    ours: locks.ours,
                    theirs: locks.theirs,
                    next_cursor: locks.next_cursor,
                };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }

    async fn release_lock(
        locks: L,
        req: Req,
        namespace: Namespace,
        id: String,
        owner: String,
    ) -> Result<Response<BoxBody>, Error> {
        let val: ReleaseLockRequest = from_json(req.into_body()).await?;
        match locks
            .release_lock(namespace.to_string(), owner, id, val.force)
            .await
        {
            Ok(lock) => {
                let resp = LockOuter {
                    lock: Lock {
                        id: lock.id,
                        path: lock.path,
                        locked_at: lock.locked_at,
                        owner: lock
                            .owner
                            .map(|owner| OwnerInfo { name: owner.name }),
                    },
                };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }

    async fn release_lock_batch(
        locks: L,
        req: Req,
        namespace: Namespace,
        owner: String,
    ) -> Result<Response<BoxBody>, Error> {
        let val: ReleaseLockBatchRequest = from_json(req.into_body()).await?;
        match locks
            .release_locks(namespace.to_string(), owner, val.paths, val.force)
            .await
        {
            Ok(batch) => {
                let resp = LockBatchOuter { batch };

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(full(into_json(&resp)?))?)
            }
            Err(err) => {
                let (status, body) = handle_lock_error_response(err);
                Ok(Response::builder()
                    .status(status)
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.git-lfs+json",
                    )
                    .body(body)?)
            }
        }
    }
}

async fn basic_response<E, S>(
    uri: Uri,
    headers: &HeaderMap,
    storage: &S,
    object: lfs::RequestObject,
    op: lfs::Operation,
    size: Result<Option<u64>, E>,
    namespace: Namespace,
) -> lfs::ResponseObject
where
    E: fmt::Display,
    S: Storage,
{
    if let Ok(Some(size)) = size {
        // Ensure that the client and server agree on the size of the object.
        if object.size != size {
            return lfs::ResponseObject {
                oid: object.oid,
                size,
                error: Some(lfs::ObjectError {
                    code: 400,
                    message: format!(
                        "bad object size: requested={}, actual={}",
                        object.size, size
                    ),
                }),
                authenticated: Some(true),
                actions: None,
            };
        }
    }

    let size = match size {
        Ok(size) => size,
        Err(err) => {
            log::error!("batch response error: {}", err);

            // Return a generic "500 - Internal Server Error" for objects that
            // we failed to get the size of. This is usually caused by some
            // intermittent problem on the storage backend. A retry strategy
            // should be implemented on the storage backend to help mitigate
            // this possibility because the git-lfs client does not currenty
            // implement retries in this case.
            return lfs::ResponseObject {
                oid: object.oid,
                size: object.size,
                error: Some(lfs::ObjectError {
                    code: 500,
                    message: err.to_string(),
                }),
                authenticated: Some(true),
                actions: None,
            };
        }
    };

    match op {
        lfs::Operation::Upload => {
            // If the object does exist, then we should not return any action.
            //
            // If the object does not exist, then we should return an upload
            // action.
            let upload_expiry_secs = UPLOAD_EXPIRATION.as_secs() as i32;
            match size {
                Some(size) => lfs::ResponseObject {
                    oid: object.oid,
                    size,
                    error: None,
                    authenticated: Some(true),
                    actions: None,
                },
                None => {
                    // If we're returning a pre-signed URL, don't also reflect the auth header back
                    // to the client.
                    let (upload_url, header) = match storage
                        .upload_url(
                            &StorageKey::new(namespace.clone(), object.oid),
                            UPLOAD_EXPIRATION,
                        )
                        .await
                    {
                        Some(url) => (url, None),
                        None => (
                            format!(
                                "{}api/{}/object/{}",
                                uri, namespace, object.oid
                            ),
                            extract_auth_header(headers),
                        ),
                    };

                    lfs::ResponseObject {
                        oid: object.oid,
                        size: object.size,
                        error: None,
                        authenticated: Some(true),
                        actions: Some(lfs::Actions {
                            download: None,
                            upload: Some(lfs::Action {
                                href: upload_url,
                                header,
                                expires_in: Some(upload_expiry_secs),
                                expires_at: None,
                            }),
                            verify: Some(lfs::Action {
                                href: format!(
                                    "{}api/{}/objects/verify",
                                    uri, namespace
                                ),
                                header: extract_auth_header(headers),
                                expires_in: None,
                                expires_at: None,
                            }),
                        }),
                    }
                }
            }
        }
        lfs::Operation::Download => {
            // If the object does not exist, then we should return a 404 error
            // for this object.
            match size {
                Some(size) => lfs::ResponseObject {
                    oid: object.oid,
                    size,
                    error: None,
                    authenticated: Some(true),
                    actions: Some(lfs::Actions {
                        download: Some(lfs::Action {
                            href: storage
                                .public_url(&StorageKey::new(
                                    namespace.clone(),
                                    object.oid,
                                ))
                                .unwrap_or_else(|| {
                                    format!(
                                        "{}api/{}/object/{}",
                                        uri, namespace, object.oid
                                    )
                                }),
                            header: extract_auth_header(headers),
                            expires_in: None,
                            expires_at: None,
                        }),
                        upload: None,
                        verify: None,
                    }),
                },
                None => lfs::ResponseObject {
                    oid: object.oid,
                    size: object.size,
                    error: Some(lfs::ObjectError {
                        code: 404,
                        message: "object not found".into(),
                    }),
                    authenticated: Some(true),
                    actions: None,
                },
            }
        }
    }
}

/// Extracts the authorization headers so that they can be reflected back to the
/// `git-lfs` client.  If we're behind a reverse proxy that provides
/// authentication, the `git-lfs` client will send an `Authorization` header on
/// the first connection, however in order for subsequent requests to also be
/// authenticated, the `header` field in the `lfs::ResponseObject` must be
/// populated.
fn extract_auth_header(
    headers: &HeaderMap,
) -> Option<BTreeMap<String, String>> {
    let headers = headers.iter().filter_map(|(k, v)| {
        if k == http::header::AUTHORIZATION {
            let value = String::from_utf8_lossy(v.as_bytes()).to_string();
            Some((k.to_string(), value))
        } else {
            None
        }
    });
    let map = BTreeMap::from_iter(headers);
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

impl<S, L> Service<Req> for App<S, L>
where
    S: Storage + Clone + Send + Sync + 'static,
    S::Error: Into<Error> + 'static,
    L: LockStorage + Clone + Send + Sync + 'static,
    Error: From<S::Error>,
{
    type Response = Response<BoxBody>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[cfg_attr(
        feature = "otel",
        instrument(
            level = "info",
            skip(self, req),
            name = "http.request",
            fields(
                method = req.method().as_str(),
                path = req.uri().path(),
                query = req.uri().query().unwrap_or_default(),
                headers
            )
        )
    )]
    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        #[cfg(feature = "otel")]
        {
            let span = tracing::Span::current();

            span.record(
                "headers",
                format!("{}", RedactedHeaders(req.headers().clone())),
            );
            let ctx = span.context();

            if req.uri().path() == "/" {
                Box::pin(future::ready(Self::index(req)).with_context(ctx))
            } else if req.uri().path().starts_with("/api/") {
                Box::pin(
                    Self::api(self.storage.clone(), self.locks.clone(), req)
                        .with_context(ctx),
                )
            } else {
                Box::pin(future::ready(Self::not_found(req)).with_context(ctx))
            }
        }

        #[cfg(not(feature = "otel"))]
        if req.uri().path() == "/" {
            Box::pin(future::ready(Self::index(req)))
        } else if req.uri().path().starts_with("/api/") {
            Box::pin(Self::api(self.storage.clone(), self.locks.clone(), req))
        } else {
            Box::pin(future::ready(Self::not_found(req)))
        }
    }
}
