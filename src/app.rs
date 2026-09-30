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
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    TryStreamExt,
    future::{self, BoxFuture},
};

use http::{self, HeaderMap, StatusCode, Uri, header};
use http_body_util::{BodyDataStream, BodyExt, StreamBody};
use hyper::{
    self, Method, Request, Response,
    body::{Frame, Incoming},
};
use tower::Service;
use url::form_urlencoded;

use askama::Template;
use bytes::Bytes;

use crate::auth::UserRepoInfo;
use crate::error::Error;
use crate::error::{BadRequest, ClientAborted};
use crate::hyperext::RequestExt;
use crate::lfs;
use crate::locks::{
    CreateLockBatchRequest, CreateLockRequest, ListLocksResponse, Lock,
    LockBatchOuter, LockOuter, LockStorage, LockStoreError, OwnerInfo,
    ReleaseLockBatchRequest, ReleaseLockRequest, VerifyLocksRequest,
    VerifyLocksResponse,
};
use crate::logger::BatchSummary;
use crate::sha256::{Sha256VerifyError, VerifyStream};
use crate::stats::STATS;
use crate::storage::{LFSObject, Namespace, Storage, StorageKey};
use crate::{empty, from_json, full, into_json};
use parking_lot::Mutex;

#[cfg(feature = "otel")]
use tracing::instrument;

/// How long presigned URLs last. git-lfs asks for new URLs once they expire,
/// and S3 only checks the signature when a transfer starts.
///
/// A URL can't outlive the credentials that signed it, and those are only
/// known to have half the credential refresh buffer left, so this must be at
/// most that. See `storage::DEFAULT_CREDENTIAL_REFRESH_BUFFER`.
const PRESIGNED_URL_EXPIRATION: Duration = Duration::from_secs(15 * 60);

const _: () = assert!(
    PRESIGNED_URL_EXPIRATION.as_secs() * 2
        <= crate::storage::DEFAULT_CREDENTIAL_REFRESH_BUFFER.as_secs(),
    "presigned URLs must last at most half the credential refresh buffer",
);

/// The largest upload that is sent to a presigned URL: S3's limit for a single
/// PUT. git-lfs uploads each object in one request, so larger objects are
/// uploaded through the server, which sends them to S3 in parts.
const MAX_PRESIGNED_UPLOAD_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// The response to a lock store's error: one the client can act on for its
/// own mistakes, and otherwise the error back, for `Logger` to answer as the
/// server failing.
fn handle_lock_error_response(
    err: anyhow::Error,
) -> Result<(StatusCode, BoxBody), Error> {
    let lock_error = |status, message: String, locks| {
        let body = into_json(&lfs::BatchResponseError {
            locks,
            message,
            documentation_url: None,
            request_id: None,
        })
        .unwrap_or_default();
        Ok((status, full(body)))
    };
    match err.downcast_ref::<LockStoreError>() {
        Some(e @ LockStoreError::CreateConflict(l)) => {
            lock_error(StatusCode::CONFLICT, e.to_string(), Some(l.clone()))
        }
        Some(LockStoreError::NotImplemented) => {
            Ok((StatusCode::NOT_FOUND, empty()))
        }
        Some(
            e @ (LockStoreError::DeleteNotFound(_)
            | LockStoreError::LockNotFound(_)),
        ) => lock_error(StatusCode::NOT_FOUND, e.to_string(), None),
        Some(e @ LockStoreError::BadRequest(_)) => {
            lock_error(StatusCode::BAD_REQUEST, e.to_string(), None)
        }
        Some(e @ LockStoreError::Forbidden(_)) => {
            lock_error(StatusCode::FORBIDDEN, e.to_string(), None)
        }
        // The store failed: a Redis or DynamoDB error, or a file it couldn't
        // write.
        _ => Err(err),
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
    fn index(req: Req) -> Result<Response<BoxBody>, Error> {
        let template = IndexTemplate {
            title: "Rudolfs",
            api: req.base_uri().path_and_query("/api").build()?,
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(full(template.render()?))?)
    }

    /// Generates a "404 not found" response.
    fn not_found(_req: Req) -> Result<Response<BoxBody>, Error> {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(full("Not found"))?)
    }

    /// Generates a "403 forbidden" response.
    fn forbidden(_req: Req) -> Result<Response<BoxBody>, Error> {
        Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("Lfs-Authenticate", "Basic realm=\"GitHub\"")
            .body(empty())?)
    }

    /// A "403 forbidden" for lock requests from a user whose GitHub username
    /// couldn't be found (GitHub's `GET /user` failed, e.g. for a token that
    /// can't read the user). Locks record their owner, so there is none to
    /// record.
    fn no_username() -> Result<Response<BoxBody>, Error> {
        Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(full(
                r#"{"message":"Locking needs your GitHub username, and GitHub didn't return one for these credentials."}"#,
            ))?)
    }

    /// Handles `/api` routes.
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
                    .body(full("Missing org/project in URL"))?);
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
                            .body(full("Missing OID parameter."))?);
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

                            let Some(owner) = user.username.clone() else {
                                return Self::no_username();
                            };

                            Self::create_lock(locks, req, namespace, owner)
                                .await
                        }
                        (&Method::POST, Some("batch")) => {
                            if !user.permissions.unwrap_or_default().push {
                                return Self::forbidden(req);
                            }

                            match parts.next() {
                                Some("lock") => {
                                    let Some(owner) = user.username.clone()
                                    else {
                                        return Self::no_username();
                                    };
                                    Self::create_lock_batch(
                                        locks, req, namespace, owner,
                                    )
                                    .await
                                }
                                Some("unlock") => {
                                    let Some(owner) = user.username.clone()
                                    else {
                                        return Self::no_username();
                                    };
                                    Self::release_lock_batch(
                                        locks, req, namespace, owner,
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

                            let Some(owner) = user.username.clone() else {
                                return Self::no_username();
                            };

                            Self::list_locks_for_verification(
                                locks, req, namespace, owner,
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
                                    let Some(owner) = user.username.clone()
                                    else {
                                        return Self::no_username();
                                    };
                                    Self::release_lock(
                                        locks, req, namespace, id, owner,
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
        instrument(level = "info", skip_all, fields(lfs.oid = %key.oid()))
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
                    // Counted as sent, so an aborted download counts only
                    // what the client got.
                    .inspect_ok(|chunk| STATS.downloaded(chunk.len() as u64))
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
        instrument(level = "info", skip_all, fields(lfs.oid = %key.oid()))
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

        // The object is checked against its OID here, rather than by the
        // storage stack, so that a mismatch is known to be the client's.
        // Whether reading the body failed, or it didn't match, is noted, as
        // the storage stack doesn't keep its error. Behind a disk cache, a
        // storage failure can still be followed by the client going away
        // during the next chunk's read, and is then taken as the client's.
        // The storage's error is still in the chain, and the window is one
        // chunk.
        let body_failed = Arc::new(AtomicBool::new(false));
        let mismatch = Arc::new(Mutex::new(None));
        let oid = *key.oid();
        let body = BodyDataStream::new(req.into_body())
            .inspect_ok(|chunk: &Bytes| STATS.uploaded(chunk.len() as u64))
            .map_err(UploadError::Body);
        let stream = VerifyStream::new(body, len, oid).map_err({
            let (body_failed, mismatch) =
                (body_failed.clone(), mismatch.clone());
            move |err| match err {
                UploadError::Body(err) => {
                    body_failed.store(true, Ordering::Relaxed);
                    std::io::Error::other(err)
                }
                UploadError::Mismatch(err) => {
                    *mismatch.lock() = Some(err.clone());
                    std::io::Error::other(err)
                }
            }
        });

        let object = LFSObject::new(len, Box::pin(stream));

        if let Err(err) = storage.put(key, object).await {
            let err = Error::from(err);
            if let Some(mismatch) = mismatch.lock().take() {
                return Err(BadRequest(mismatch.into()).into());
            }
            if body_failed.load(Ordering::Relaxed) {
                return Err(ClientAborted(err).into());
            }
            return Err(err);
        }

        Ok(Response::builder().status(StatusCode::OK).body(empty())?)
    }

    /// Verifies that an LFS object exists on the server.
    #[cfg_attr(feature = "otel", instrument(level = "info", skip_all))]
    async fn verify(
        storage: S,
        req: Request<Incoming>,
        namespace: Namespace,
    ) -> Result<Response<BoxBody>, Error> {
        let val: lfs::VerifyRequest = from_json(req.into_body()).await?;
        let key = StorageKey::new(namespace, val.oid);

        if let Some(size) = storage.size(&key).await?
            && size == val.size
        {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(empty())?);
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
        instrument(
            level = "info",
            skip_all,
            fields(
                lfs.operation,
                lfs.objects,
                lfs.objects.presigned,
                lfs.objects.missing
            )
        )
    )]
    async fn batch(
        storage: S,
        req: Request<Incoming>,
        namespace: Namespace,
    ) -> Result<Response<BoxBody>, Error> {
        // Get the host name and scheme.
        let uri = req.base_uri().path_and_query("/").build()?;
        let headers = req.headers().clone();

        // JSON that doesn't parse is answered with a 400 by `Logger`.
        let val = from_json::<lfs::BatchRequest>(req.into_body()).await?;
        let operation = val.operation;

        // For each object, check if it exists in the storage backend.
        let objects = val.objects.into_iter().map(|object| {
            let uri = uri.clone();
            let key = StorageKey::new(namespace.clone(), object.oid);

            async {
                let size = storage.size(&key).await;

                let (namespace, _) = key.into_parts();
                Ok(basic_response(
                    uri, &headers, &storage, object, operation, size, namespace,
                )
                .await)
            }
        });

        let objects = future::try_join_all(objects).await?;
        let mut transfer = Some(lfs::Transfer::Basic);
        if let Some(transfers) = val.transfers
            && transfers.contains(&lfs::Transfer::LfsRs)
        {
            transfer = Some(lfs::Transfer::LfsRs)
        }
        let summary = batch_summary(operation, &objects, &uri);
        let response = lfs::BatchResponse { transfer, objects };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .extension(summary)
            .body(full(into_json(&response)?))?)
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
                let (status, body) = handle_lock_error_response(err)?;
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
                let (status, body) = handle_lock_error_response(err)?;
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
                let (status, body) = handle_lock_error_response(err)?;
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
                let (status, body) = handle_lock_error_response(err)?;
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
                let (status, body) = handle_lock_error_response(err)?;
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
                let (status, body) = handle_lock_error_response(err)?;
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

/// Sums up a batch response for its request span and log line, and records it
/// on the batch span. Objects are presigned when their action goes straight to
/// S3 rather than through this server.
fn batch_summary(
    operation: lfs::Operation,
    objects: &[lfs::ResponseObject],
    server: &Uri,
) -> BatchSummary {
    let server = server.to_string();
    let presigned = objects
        .iter()
        .filter_map(|object| object.actions.as_ref())
        .filter_map(|actions| {
            actions.download.as_ref().or(actions.upload.as_ref())
        })
        .filter(|action| !action.href.starts_with(&server))
        .count();
    let summary = BatchSummary {
        operation: match operation {
            lfs::Operation::Upload => "upload",
            lfs::Operation::Download => "download",
        },
        objects: objects.len(),
        presigned,
        missing: objects
            .iter()
            .filter(|object| object.error.is_some())
            .count(),
    };

    // The batch handler's span only exists with `otel`; without it, the
    // current span is the request's, which records its own.
    #[cfg(feature = "otel")]
    {
        let span = tracing::Span::current();
        span.record("lfs.operation", summary.operation);
        span.record("lfs.objects", summary.objects as i64);
        span.record("lfs.objects.presigned", summary.presigned as i64);
        span.record("lfs.objects.missing", summary.missing as i64);
    }
    summary
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
            tracing::error!("batch response error: {err:#}");

            // Return a generic "500 - Internal Server Error" for objects that
            // we failed to get the size of. This is usually caused by some
            // intermittent problem on the storage backend. A retry strategy
            // should be implemented on the storage backend to help mitigate
            // this possibility because the git-lfs client does not currenty
            // implement retries in this case. It doesn't say why, as the error
            // may name storage details, but gives the trace to look for.
            let request_id = crate::logger::trace_id(&tracing::Span::current());
            let message = match request_id {
                Some(id) => format!("Internal server error (request ID {id})"),
                None => "Internal server error".to_string(),
            };
            return lfs::ResponseObject {
                oid: object.oid,
                size: object.size,
                error: Some(lfs::ObjectError { code: 500, message }),
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
            let upload_expiry_secs = PRESIGNED_URL_EXPIRATION.as_secs() as i32;
            match size {
                Some(size) => lfs::ResponseObject {
                    oid: object.oid,
                    size,
                    error: None,
                    authenticated: Some(true),
                    actions: None,
                },
                None => {
                    let presigned_url = if object.size
                        <= MAX_PRESIGNED_UPLOAD_SIZE
                    {
                        storage
                            .upload_url(
                                &StorageKey::new(namespace.clone(), object.oid),
                                PRESIGNED_URL_EXPIRATION,
                            )
                            .await
                    } else {
                        None
                    };

                    // If we're returning a pre-signed URL, don't also reflect
                    // the auth header back to the client.
                    let (upload_url, header) = match presigned_url {
                        Some(url) => {
                            STATS.presigned_upload();
                            (url, None)
                        }
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
            // If we're returning a pre-signed URL, don't also reflect
            // the auth header back to the client.
            let (download_url, header, presigned) = match storage
                .download_url(
                    &StorageKey::new(namespace.clone(), object.oid),
                    PRESIGNED_URL_EXPIRATION,
                )
                .await
            {
                Some(url) => (url, None, true),
                None => (
                    storage
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
                    extract_auth_header(headers),
                    false,
                ),
            };

            if presigned && size.is_some() {
                STATS.presigned_download();
            }

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
                            href: download_url,
                            header,
                            // So that git-lfs asks for a new URL, rather than
                            // using one that has expired.
                            expires_in: presigned.then_some(
                                PRESIGNED_URL_EXPIRATION.as_secs() as i32,
                            ),
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
    if map.is_empty() { None } else { Some(map) }
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

    // The request's span is `Logger`'s, which this is called within.
    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if req.uri().path() == "/" {
            Box::pin(future::ready(Self::index(req)))
        } else if req.uri().path().starts_with("/api/") {
            Box::pin(Self::api(self.storage.clone(), self.locks.clone(), req))
        } else {
            Box::pin(future::ready(Self::not_found(req)))
        }
    }
}

/// Why an upload's body stream failed.
#[derive(Debug)]
enum UploadError {
    /// Reading it from the client failed.
    Body(hyper::Error),
    /// It didn't match its OID.
    Mismatch(Sha256VerifyError),
}

impl From<Sha256VerifyError> for UploadError {
    fn from(err: Sha256VerifyError) -> Self {
        UploadError::Mismatch(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageStream;
    use async_trait::async_trait;

    #[test]
    fn lock_errors_are_answered_for_what_they_are() {
        let status = |err: LockStoreError| {
            handle_lock_error_response(anyhow::anyhow!(err))
                .map(|(status, _)| status)
                .ok()
        };
        assert_eq!(
            status(crate::locks::held_by_another("alice", "bob")),
            Some(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            status(LockStoreError::LockNotFound("x".into())),
            Some(StatusCode::NOT_FOUND)
        );
        assert_eq!(
            status(LockStoreError::DeleteNotFound("x".into())),
            Some(StatusCode::NOT_FOUND)
        );
        assert_eq!(
            status(LockStoreError::BadRequest("x".into())),
            Some(StatusCode::BAD_REQUEST)
        );
        // The store failing is left to `Logger`, which answers with a 503.
        assert_eq!(
            status(LockStoreError::InternalServerError("x".into())),
            None
        );
        assert!(
            handle_lock_error_response(anyhow::anyhow!("disk full")).is_err()
        );
    }

    /// A store that presigns every upload and holds nothing.
    struct Presigning;

    #[async_trait]
    impl Storage for Presigning {
        type Error = std::io::Error;

        async fn get(
            &self,
            _: &StorageKey,
        ) -> Result<Option<LFSObject>, Self::Error> {
            Ok(None)
        }
        async fn put(
            &self,
            _: StorageKey,
            _: LFSObject,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn size(
            &self,
            _: &StorageKey,
        ) -> Result<Option<u64>, Self::Error> {
            Ok(None)
        }
        async fn delete(&self, _: &StorageKey) -> Result<(), Self::Error> {
            Ok(())
        }
        fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
            Box::pin(futures::stream::empty())
        }
        fn public_url(&self, _: &StorageKey) -> Option<String> {
            None
        }
        async fn upload_url(
            &self,
            _: &StorageKey,
            _: Duration,
        ) -> Option<String> {
            Some("https://presigned.example/upload".into())
        }
        async fn download_url(
            &self,
            _: &StorageKey,
            _: Duration,
        ) -> Option<String> {
            None
        }
    }

    /// The upload action the batch response gives for a new object of `size`.
    async fn upload_action(size: u64) -> lfs::Action {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Basic dXNlcjp0b2tlbg==".parse().unwrap(),
        );
        let object = lfs::RequestObject {
            oid: "b1fbeefc23e6a149a6f7d0c2fb635bfc78f7ddc2da963ea9c6a63eb324260e6d"
                .parse()
                .unwrap(),
            size,
        };

        let response = basic_response(
            "http://lfs.example/".parse().unwrap(),
            &headers,
            &Presigning,
            object,
            lfs::Operation::Upload,
            Ok::<_, std::io::Error>(None),
            Namespace::new("org".into(), "project".into()),
        )
        .await;

        response.actions.unwrap().upload.unwrap()
    }

    /// S3 takes a single PUT of up to 5 GiB; git-lfs can't split an upload.
    #[tokio::test]
    async fn uploads_up_to_5_gib_are_presigned() {
        let action = upload_action(MAX_PRESIGNED_UPLOAD_SIZE).await;
        assert_eq!(action.href, "https://presigned.example/upload");
        assert!(action.header.is_none());
    }

    /// Larger uploads go through the server, which sends them to S3 in parts.
    #[tokio::test]
    async fn larger_uploads_go_through_the_server() {
        let action = upload_action(MAX_PRESIGNED_UPLOAD_SIZE + 1).await;
        assert!(
            action
                .href
                .starts_with("http://lfs.example/api/org/project/object/"),
            "{}",
            action.href
        );
        // The client needs its credentials to upload to the server.
        let header = action.header.unwrap();
        assert_eq!(header["authorization"], "Basic dXNlcjp0b2tlbg==");
    }
}
