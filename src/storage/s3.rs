// Copyright (c) 2020 Jason White
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
use anyhow::Context;
use async_trait::async_trait;
use aws_config::identity::IdentityCache;
use aws_config::{Region, SdkConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::{ProvideCredentials as _, SharedCredentialsProvider};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::{
    complete_multipart_upload::CompleteMultipartUploadError,
    create_multipart_upload::CreateMultipartUploadError,
    get_object::GetObjectError, head_object::HeadObjectError,
    put_object::PutObjectError, upload_part::UploadPartError,
};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::{
    BucketAccelerateStatus, CompletedMultipartUpload, CompletedPart,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::{Bytes, BytesMut};
use futures::{TryStreamExt, stream};
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

#[cfg(feature = "otel")]
use tracing::instrument;

use super::{LFSObject, Storage, StorageKey, StorageStream};
use crate::lru;
use crate::stats::STATS;
use derive_more::{Display, From};
use parking_lot::Mutex;
use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, ready};
use std::time::Duration;

#[derive(Debug, From, Display)]
pub enum Error {
    Get(GetObjectError),
    Put(PutObjectError),
    CreateMultipart(CreateMultipartUploadError),
    Upload(UploadPartError),
    CompleteMultipart(CompleteMultipartUploadError),
    Head(HeadObjectError),

    Stream(std::io::Error),

    /// The uploaded object is too large.
    TooLarge(u64),
}

impl From<SdkError<GetObjectError, HttpResponse>> for Error {
    fn from(err: SdkError<GetObjectError, HttpResponse>) -> Self {
        Error::Get(err.into_service_error())
    }
}

impl From<SdkError<PutObjectError, HttpResponse>> for Error {
    fn from(err: SdkError<PutObjectError, HttpResponse>) -> Self {
        Error::Put(err.into_service_error())
    }
}

impl From<SdkError<CreateMultipartUploadError, HttpResponse>> for Error {
    fn from(err: SdkError<CreateMultipartUploadError, HttpResponse>) -> Self {
        Error::CreateMultipart(err.into_service_error())
    }
}

impl From<SdkError<UploadPartError, HttpResponse>> for Error {
    fn from(err: SdkError<UploadPartError, HttpResponse>) -> Self {
        Error::Upload(err.into_service_error())
    }
}

impl From<SdkError<CompleteMultipartUploadError, HttpResponse>> for Error {
    fn from(err: SdkError<CompleteMultipartUploadError, HttpResponse>) -> Self {
        Error::CompleteMultipart(err.into_service_error())
    }
}

impl From<SdkError<HeadObjectError, HttpResponse>> for Error {
    fn from(err: SdkError<HeadObjectError, HttpResponse>) -> Self {
        Error::Head(err.into_service_error())
    }
}

impl ::std::error::Error for Error {}

/// How long before its credentials expire the S3 clients get new ones, by
/// default.
///
/// A presigned URL can't outlive the credentials that signed it, and the SDK
/// only refreshes credentials 10 seconds before they expire. So URLs signed
/// near the end of a credential period expired early, the SDK logged an
/// EXPIRATION_WARNING, and clients could get 403s partway through a batch.
///
/// The SDK randomizes the buffer: it refreshes credentials anywhere from half
/// the buffer to the whole of it before they expire. So credentials used to
/// sign a URL have at least half the buffer left, and presigned URLs, which
/// last `PRESIGNED_URL_EXPIRATION` (15 minutes, in `app.rs`), must last no more
/// than that to get their full lifetime. `app.rs` checks this at compile time.
///
/// This relies on the credential source handing out *new* credentials when
/// asked this long before they expire. EKS Pod Identity does (its agent renews
/// its 6-hour credentials after at most 3 hours), as do IRSA / web identity,
/// SSO and AssumeRole profiles, which fetch new credentials every time. Static
/// keys don't expire, so this changes nothing for them.
///
/// An EC2 instance role (IMDS) or an ECS task role does NOT: the credentials
/// endpoint serves the same credentials until shortly before they expire (about
/// 5 minutes, for IMDS). The SDK would then ask it again on almost every
/// request for the rest of each credential period, and IMDS rate-limits. Use
/// `--s3-credential-refresh-buffer 0` there, which keeps the SDK's default.
pub const DEFAULT_CREDENTIAL_REFRESH_BUFFER: Duration =
    Duration::from_secs(35 * 60);

/// The size of the parts objects are uploaded in. S3 allows up to 10,000 parts,
/// so this caps objects at about 1 TB, and at least 5 MiB except for the last.
/// A multipart upload holds up to two parts in memory.
pub const DEFAULT_PART_SIZE: usize = 100 * 1024 * 1024;

/// S3's minimum part size, except for the last part.
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Amazon S3 storage backend.
pub struct Backend {
    /// S3 client.
    client: Client,

    /// Name of the bucket to use.
    bucket: String,

    /// Prefix for objects.
    prefix: String,

    /// URL for the CDN. Example: https://lfscdn.myawesomegit.com
    cdn: Option<String>,

    /// S3 client for generating presigned URLs
    accelerate_client: Option<Client>,

    /// LRU cache for object sizes to avoid repeated HEAD requests.
    /// None if caching is disabled.
    size_cache: Option<Arc<Mutex<lru::Cache<StorageKey>>>>,

    /// Maximum number of entries in the size cache
    max_cache_entries: Option<usize>,

    /// See [`DEFAULT_CREDENTIAL_REFRESH_BUFFER`]. Zero is the SDK's default.
    credential_refresh_buffer: Duration,

    /// See [`DEFAULT_PART_SIZE`].
    part_size: usize,

    /// Only used to report the credentials' lifetime at startup; the clients
    /// have their own, behind the credential cache.
    credentials_provider: Option<SharedCredentialsProvider>,

    /// Number of cache hits (only tracked when cache is enabled)
    cache_hits: AtomicUsize,

    /// Number of cache misses (only tracked when cache is enabled)
    cache_misses: AtomicUsize,
}

impl Backend {
    /// Loads the AWS configuration from the environment, connects to the
    /// bucket and checks that it is usable.
    pub async fn new(
        bucket: String,
        prefix: String,
        cdn: Option<String>,
        s3_accelerate: bool,
        size_cache_entries: usize,
    ) -> Result<Self, anyhow::Error> {
        let sdk_config = Self::load_config().await?;
        let backend = Self::from_config(
            &sdk_config,
            bucket,
            prefix,
            cdn,
            s3_accelerate,
            size_cache_entries,
            DEFAULT_CREDENTIAL_REFRESH_BUFFER,
        );
        backend.check().await?;
        Ok(backend)
    }

    /// Loads the AWS configuration from the default credential and region
    /// chains. If `$AWS_S3_ENDPOINT` is set, it is used as a custom endpoint
    /// (e.g. an S3-compatible server) and a region must also be set.
    pub async fn load_config() -> Result<SdkConfig, anyhow::Error> {
        let mut shared_config =
            aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12());

        if let Ok(endpoint) = std::env::var("AWS_S3_ENDPOINT") {
            // If a custom endpoint is set, do not use the AWS default
            // (us-east-1). Instead, check environment variables for a region
            // name.
            let name = std::env::var("AWS_DEFAULT_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .context(
                    "$AWS_S3_ENDPOINT was set without $AWS_DEFAULT_REGION or \
                     $AWS_REGION being set. Custom endpoints don't make sense \
                     without also setting a region.",
                )?;
            shared_config = shared_config
                .endpoint_url(endpoint)
                .region(Region::new(name));
        }

        Ok(shared_config.load().await)
    }

    /// Creates the backend from an already loaded AWS configuration. This
    /// makes no requests; call [`Backend::check`] to verify the bucket.
    ///
    /// If the configuration has a custom endpoint, path-style addressing is
    /// used, since S3-compatible servers generally don't route virtual-hosted
    /// bucket names.
    pub fn from_config(
        sdk_config: &SdkConfig,
        bucket: String,
        mut prefix: String,
        cdn: Option<String>,
        s3_accelerate: bool,
        size_cache_entries: usize,
        credential_refresh_buffer: Duration,
    ) -> Self {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        // One credential cache for both clients, so they refresh together.
        let mut identity_cache = IdentityCache::lazy();
        if !credential_refresh_buffer.is_zero() {
            identity_cache =
                identity_cache.buffer_time(credential_refresh_buffer);
        }
        let identity_cache = identity_cache.build();

        let service_config = aws_sdk_s3::config::Builder::from(sdk_config)
            .identity_cache(identity_cache.clone())
            .force_path_style(sdk_config.endpoint_url().is_some())
            .build();
        let client = Client::from_conf(service_config);

        // S3 client used for signing accelerate upload and download URLs.
        let accelerate_client = if s3_accelerate {
            let service_config = aws_sdk_s3::config::Builder::from(sdk_config)
                .identity_cache(identity_cache)
                .accelerate(true)
                .build();
            Some(Client::from_conf(service_config))
        } else {
            None
        };

        // Initialize the size cache
        let (size_cache, max_cache_entries) = if size_cache_entries > 0 {
            tracing::info!(
                "Initialized S3 size cache with capacity for {} entries",
                size_cache_entries
            );
            (
                Some(Arc::new(Mutex::new(lru::Cache::new()))),
                Some(size_cache_entries),
            )
        } else {
            tracing::info!("S3 size cache is disabled");
            (None, None)
        };

        Backend {
            client,
            bucket,
            prefix,
            cdn,
            accelerate_client,
            size_cache,
            max_cache_entries,
            credential_refresh_buffer,
            credentials_provider: sdk_config.credentials_provider(),
            part_size: DEFAULT_PART_SIZE,
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
        }
    }

    /// Checks that the bucket exists and that our credentials work, and that
    /// transfer acceleration is enabled if we expect to use it. This catches
    /// very common configuration errors early on in application startup.
    pub async fn check(&self) -> Result<(), anyhow::Error> {
        let bucket = &self.bucket;

        self.client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .with_context(|| {
                format!("Failed to connect to S3 bucket '{bucket}'")
            })?;

        tracing::info!(
            "Connected to S3 bucket '{}' at region '{}'",
            bucket,
            self.client
                .config()
                .region()
                .map_or("us-east-1", |region| region.as_ref())
        );

        self.check_credential_lifetime().await;

        if self.accelerate_client.is_some() {
            let resp = self
                .client
                .get_bucket_accelerate_configuration()
                .bucket(bucket)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Failed to check S3 transfer acceleration for bucket \
                         '{bucket}'"
                    )
                })?;

            match resp.status {
                Some(BucketAccelerateStatus::Enabled) => tracing::info!(
                    "S3 transfer acceleration is enabled for bucket '{}'",
                    bucket
                ),
                Some(BucketAccelerateStatus::Suspended) => anyhow::bail!(
                    "S3 transfer acceleration is suspended for bucket \
                     '{bucket}'. Please enable S3 transfer acceleration for \
                     this bucket or disable configuration"
                ),
                // S3 omits the status if acceleration was never configured.
                None => anyhow::bail!(
                    "S3 transfer acceleration is not enabled for bucket \
                     '{bucket}'. Please enable S3 transfer acceleration for \
                     this bucket or disable configuration"
                ),
                status => tracing::warn!(
                    "S3 transfer acceleration is in an unknown state '{:?}' \
                     for bucket '{}'",
                    status,
                    bucket
                ),
            }
        }

        Ok(())
    }

    /// Sets the size of the parts objects are uploaded in; see
    /// [`DEFAULT_PART_SIZE`].
    ///
    /// # Panics
    ///
    /// If `part_size` is less than S3's minimum, [`MIN_PART_SIZE`].
    pub fn with_part_size(mut self, part_size: usize) -> Self {
        assert!(
            part_size >= MIN_PART_SIZE,
            "S3 parts must be at least {MIN_PART_SIZE} bytes"
        );
        self.part_size = part_size;
        self
    }

    /// Logs how long the credentials last, and warns if that is too short for
    /// the refresh buffer: the SDK would then fetch credentials on almost every
    /// request. This is a sample at startup, not a guarantee; see
    /// [`DEFAULT_CREDENTIAL_REFRESH_BUFFER`].
    async fn check_credential_lifetime(&self) {
        let Some(provider) = &self.credentials_provider else {
            return;
        };
        let credentials = match provider.provide_credentials().await {
            Ok(credentials) => credentials,
            Err(err) => {
                tracing::debug!(
                    "Couldn't check the credentials' lifetime: {err}"
                );
                return;
            }
        };
        // Static keys don't expire.
        let Some(expiry) = credentials.expiry() else {
            return;
        };

        let remaining = expiry
            .duration_since(std::time::SystemTime::now())
            .unwrap_or_default();
        let remaining = Duration::from_secs(remaining.as_secs());
        tracing::info!(
            "AWS credentials expire in {}",
            humantime::format_duration(remaining)
        );

        let buffer = self.credential_refresh_buffer;
        if !buffer.is_zero() && remaining < buffer + Duration::from_secs(5 * 60)
        {
            tracing::warn!(
                "AWS credentials have only {} left, and are refreshed {} \
                 before they expire (--s3-credential-refresh-buffer). If the \
                 credential source keeps serving the same credentials until \
                 they nearly expire, as EC2 instance and ECS task roles do, \
                 they will be fetched again on almost every request. Set \
                 --s3-credential-refresh-buffer 0 for those.",
                humantime::format_duration(remaining),
                humantime::format_duration(buffer),
            );
        }
    }

    fn key_to_path(&self, key: &StorageKey) -> String {
        if self.prefix.is_empty() {
            format!("{}/{}", key.namespace(), key.oid().path())
        } else {
            format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
        }
    }

    /// Returns cache statistics: (hits, misses, current_entries)
    /// Returns (0, 0, 0) if caching is disabled.
    pub fn cache_stats(&self) -> (usize, usize, usize) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let entries = self
            .size_cache
            .as_ref()
            .map(|c| c.lock().len())
            .unwrap_or(0);
        (hits, misses, entries)
    }
}

impl Backend {
    /// Uploads `value` in parts, reading the next part while the previous one
    /// uploads. Waits for any part in flight before returning, even on error,
    /// so that an abort afterwards can't race it.
    #[expect(
        clippy::result_large_err,
        reason = "the same error type as `Storage::put`, which it returns"
    )]
    async fn upload_parts(
        &self,
        path: &str,
        upload_id: &str,
        value: LFSObject,
        in_flight: &InFlight,
    ) -> Result<Vec<CompletedPart>, Error> {
        let mut body = value.stream().into_async_read().compat();
        let mut parts = Vec::new();
        let mut part_number = 1;

        let result = loop {
            let chunk = match read_part(&mut body, self.part_size).await {
                Ok(chunk) => chunk,
                Err(err) => break Err(Error::Stream(err)),
            };
            let last = chunk.len() < self.part_size;

            if let Some(previous) = join_in_flight(in_flight).await {
                match previous {
                    Ok(part) => parts.push(part),
                    Err(err) => break Err(err),
                }
            }

            // An object that ends on a part boundary needs no empty last part,
            // but an empty object needs one part.
            if chunk.is_empty() && part_number > 1 {
                break Ok(());
            }

            *in_flight.lock() = Some(tokio::spawn(upload_part(
                self.client.clone(),
                self.bucket.clone(),
                path.to_string(),
                upload_id.to_string(),
                part_number,
                chunk,
            )));
            part_number += 1;

            if last {
                break Ok(());
            }
        };

        // Waited for even on error, so that an abort can't race the part.
        if let Some(part) = join_in_flight(in_flight).await
            && result.is_ok()
        {
            parts.push(part?);
        }

        result.map(|()| parts)
    }
}

/// A part being uploaded.
type PartUpload = JoinHandle<Result<CompletedPart, Error>>;

/// The part upload in flight, where [`AbortOnDrop`] can wait for it.
type InFlight = Arc<Mutex<Option<PartUpload>>>;

/// Aborts a multipart upload when dropped, unless disarmed.
///
/// When a client disconnects, hyper drops the request, and `put` with it, so
/// `put` never sees an error and can't abort the upload itself. Its parts would
/// otherwise stay in the bucket, billed, until a lifecycle rule removes them.
struct AbortOnDrop {
    client: Client,
    bucket: String,
    path: String,
    upload_id: String,
    in_flight: InFlight,
    armed: bool,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Without a runtime the process is exiting; a lifecycle rule has to
        // clean up.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let part = self.in_flight.lock().take();
        let client = self.client.clone();
        let bucket = std::mem::take(&mut self.bucket);
        let path = std::mem::take(&mut self.path);
        let upload_id = std::mem::take(&mut self.upload_id);
        runtime.spawn(async move {
            // Let the part in flight finish first, or it could be stored after
            // the abort.
            if let Some(part) = part {
                let _ = part.await;
            }
            tracing::info!(
                "Aborting the multipart upload of {path}: the request was \
                 dropped"
            );
            abort_upload(&client, &bucket, &path, &upload_id).await;
        });
    }
}

async fn abort_upload(
    client: &Client,
    bucket: &str,
    path: &str,
    upload_id: &str,
) {
    let abort = client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(path)
        .upload_id(upload_id)
        .send()
        .await;
    match abort {
        Ok(_) => {}
        // Already aborted, e.g. by both `put` and its drop guard when `put` was
        // dropped during its own abort.
        Err(err)
            if matches!(
                err.as_service_error(),
                Some(AbortMultipartUploadError::NoSuchUpload(_))
            ) =>
        {
            tracing::debug!(
                "Multipart upload {upload_id} of {path} was already gone"
            );
        }
        Err(err) => tracing::warn!(
            "Failed to abort multipart upload {upload_id} of {path}; its \
             parts stay in the bucket until a lifecycle rule removes them: {}",
            DisplayErrorContext(err)
        ),
    }
}

/// Reads up to `size` bytes, fewer only at the end of the stream.
async fn read_part<R>(reader: &mut R, size: usize) -> std::io::Result<Bytes>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = BytesMut::with_capacity(size);
    while buffer.len() < size {
        let remaining = (size - buffer.len()) as u64;
        if (&mut *reader).take(remaining).read_buf(&mut buffer).await? == 0 {
            break;
        }
    }
    Ok(buffer.freeze())
}

#[expect(
    clippy::result_large_err,
    reason = "the same error type as `Storage::put`, which it returns"
)]
async fn upload_part(
    client: Client,
    bucket: String,
    path: String,
    upload_id: String,
    part_number: i32,
    chunk: Bytes,
) -> Result<CompletedPart, Error> {
    let resp = client
        .upload_part()
        .bucket(bucket)
        .key(path)
        .part_number(part_number)
        .upload_id(upload_id)
        .body(ByteStream::new(SdkBody::from(chunk)))
        .send()
        .await
        .map_err(Error::from)?;

    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(resp.e_tag.unwrap_or_default())
        .build())
}

#[expect(
    clippy::result_large_err,
    reason = "the same error type as `Storage::put`, which it returns"
)]
/// Waits for the part in flight, if there is one, and empties the slot.
///
/// The part stays in the slot until it has finished, so that if `put` is
/// dropped meanwhile, [`AbortOnDrop`] finds it and waits for it before
/// aborting. The lock is only held within each poll, never across an await.
async fn join_in_flight(
    in_flight: &InFlight,
) -> Option<Result<CompletedPart, Error>> {
    std::future::poll_fn(|cx| {
        let mut slot = in_flight.lock();
        let Some(part) = slot.as_mut() else {
            return Poll::Ready(None);
        };
        let result = ready!(Pin::new(part).poll(cx));
        *slot = None;
        Poll::Ready(Some(result.unwrap_or_else(|err| {
            Err(Error::Stream(std::io::Error::other(format!(
                "part upload failed: {err}"
            ))))
        })))
    })
    .await
}

#[async_trait]
impl Storage for Backend {
    type Error = Error;

    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        let resp = match self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .send()
            .await
        {
            Ok(get_object_output) => Ok(Some(LFSObject::new(
                get_object_output.content_length.unwrap() as u64,
                Box::pin(ReaderStream::new(
                    get_object_output.body.into_async_read(),
                )),
            ))),
            Err(e) => {
                let e = e.into_service_error();
                if let GetObjectError::NoSuchKey(_) = e {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }?;
        Ok(resp)
    }

    /// Uploads the object as a multipart upload, in parts of `part_size`, so
    /// that objects over S3's 5 GiB limit for a single PUT can be stored.
    ///
    /// The next part is read from the client while the current one uploads,
    /// so the client keeps sending; a stalled connection can be closed by a
    /// load balancer's idle timeout. At most one part is in flight, so this
    /// holds up to two parts in memory.
    ///
    /// If anything fails, the upload is aborted, so that the parts already
    /// sent aren't left in the bucket (and billed).
    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self, value)))]
    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let path = self.key_to_path(&key);

        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await
            .map_err(Error::from)?
            .upload_id
            // Only missing if S3 has a bug.
            .expect("CreateMultipartUpload returned no upload id");

        let mut guard = AbortOnDrop {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            path: path.clone(),
            upload_id: upload_id.clone(),
            in_flight: InFlight::default(),
            armed: true,
        };

        let result = async {
            let parts = self
                .upload_parts(&path, &upload_id, value, &guard.in_flight)
                .await?;

            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&path)
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map_err(Error::from)?;
            Ok(())
        }
        .await;

        if result.is_err() {
            // `upload_parts` has already waited for any part in flight.
            abort_upload(&self.client, &self.bucket, &path, &upload_id).await;
        }
        guard.armed = false;

        result
    }

    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        // Check cache first if enabled
        if let Some(cache) = &self.size_cache {
            if let Some(size) = cache.lock().get_refresh(key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                STATS.s3_size_cache_hit();
                tracing::debug!("S3 size cache hit for {}", key.oid());
                return Ok(Some(size));
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            STATS.s3_size_cache_miss();
        }

        // Cache miss - perform HEAD request
        let resp = self
            .client
            .head_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .send()
            .await;

        match resp {
            Ok(head_object_output) => {
                let size = head_object_output.content_length.unwrap() as u64;

                // Cache the size if caching is enabled
                if let (Some(cache), Some(max_entries)) =
                    (&self.size_cache, self.max_cache_entries)
                {
                    let mut cache = cache.lock();
                    cache.push(key.clone(), size);

                    // Prune cache by entry count if needed
                    while cache.len() > max_entries {
                        cache.pop();
                    }
                }

                Ok(Some(size))
            }
            Err(e) => {
                let e = e.into_service_error();
                if let HeadObjectError::NotFound(_) = e {
                    // Don't cache negative results
                    Ok(None)
                } else {
                    Err(Error::Head(e))
                }
            }
        }
    }

    /// This never deletes objects from S3 and always returns success. This may
    /// be changed in the future.
    async fn delete(&self, _key: &StorageKey) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Always returns an empty stream. This may be changed in the future.
    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        Box::pin(stream::empty())
    }

    // Public URL is used to send a static download URL to the client for the
    // CDN config
    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.cdn
            .as_ref()
            .map(|cdn| format!("{}/{}", cdn, self.key_to_path(key)))
    }

    // Upload URL is used for the CDN config and the S3 Transfer Acceleration
    // config.
    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        // Don't use a presigned URL if we're not using a CDN or S3 Transfer
        // Acceleration. Otherwise, uploads will bypass the encryption
        // process and fail to download.
        if self.cdn.is_none() && self.accelerate_client.is_none() {
            return None;
        }

        let presigning_config =
            PresigningConfig::expires_in(expires_in).unwrap();

        let client = if let Some(client) = &self.accelerate_client {
            client
        } else {
            &self.client
        };

        let resp = client
            .put_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .presigned(presigning_config)
            .await;

        let presigned_url = match resp {
            Ok(presigned_request) => presigned_request.uri().to_string(),
            _ => return None,
        };

        Some(presigned_url)
    }

    // Download URL is only used when S3 Transfer Acceleration is enabled.
    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn download_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        // Don't use a presigned URL if we're not using S3 Transfer
        // Acceleration. Otherwise, uploads will bypass the encryption
        // process and fail to download.
        self.accelerate_client.as_ref()?;

        let presigning_config =
            PresigningConfig::expires_in(expires_in).unwrap();

        let resp = self
            .accelerate_client
            .as_ref()?
            .get_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .presigned(presigning_config)
            .await;

        let presigned_url = match resp {
            Ok(presigned_request) => presigned_request.uri().to_string(),
            _ => return None,
        };

        Some(presigned_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lfs::Oid;
    use crate::storage::Namespace;
    use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};

    const ACCESS_KEY_ID: &str = "AKIDLFSRSTESTPLUMBING";

    /// A configuration with fixed credentials, built without consulting the
    /// environment so the test can't pick up a developer's real credentials.
    fn config() -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::v2026_01_12())
            .region(Region::new("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(
                Credentials::new(ACCESS_KEY_ID, "secret", None, None, "test"),
            ))
            .build()
    }

    fn key() -> StorageKey {
        StorageKey::new(
            Namespace::new("org".into(), "project".into()),
            Oid::from([0xab; 32]),
        )
    }

    fn assert_accelerated(url: &str, backend: &Backend) {
        let url = url::Url::parse(url).unwrap();
        assert_eq!(url.host_str(), Some("bucket.s3-accelerate.amazonaws.com"));
        assert_eq!(url.path(), format!("/{}", backend.key_to_path(&key())));

        let credential = url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-Credential")
            .map(|(_, value)| value.into_owned())
            .expect("presigned URL is not signed");
        assert!(
            credential.starts_with(&format!("{ACCESS_KEY_ID}/"))
                && credential.ends_with("/us-west-2/s3/aws4_request"),
            "signed with unexpected credential scope: {credential}"
        );
    }

    /// The accelerate client must sign with the same credentials and region
    /// as the main client. Presigning is local, so this needs no bucket.
    #[tokio::test]
    async fn accelerate_urls_use_configured_credentials() {
        let backend = Backend::from_config(
            &config(),
            "bucket".into(),
            "lfs".into(),
            None,
            true,
            0,
            Duration::ZERO,
        );
        let expires_in = Duration::from_secs(60);

        let url = backend.upload_url(&key(), expires_in).await.unwrap();
        assert_accelerated(&url, &backend);

        let url = backend.download_url(&key(), expires_in).await.unwrap();
        assert_accelerated(&url, &backend);
    }

    /// Without acceleration or a CDN, transfers go through the server.
    #[tokio::test]
    async fn no_presigned_urls_without_acceleration() {
        let backend = Backend::from_config(
            &config(),
            "bucket".into(),
            "lfs".into(),
            None,
            false,
            0,
            Duration::ZERO,
        );
        let expires_in = Duration::from_secs(60);

        assert_eq!(backend.upload_url(&key(), expires_in).await, None);
        assert_eq!(backend.download_url(&key(), expires_in).await, None);
    }

    #[test]
    fn key_paths() {
        let path = |prefix: &str| {
            Backend::from_config(
                &config(),
                "bucket".into(),
                prefix.into(),
                None,
                false,
                0,
                Duration::ZERO,
            )
            .key_to_path(&key())
        };
        let oid = key().oid().path().to_string();

        assert_eq!(path("lfs"), format!("lfs/org/project/{oid}"));
        assert_eq!(path("lfs//"), format!("lfs/org/project/{oid}"));
        assert_eq!(path(""), format!("org/project/{oid}"));
        assert_eq!(path("/"), format!("org/project/{oid}"));
    }

    /// Presigns a GET with the backend's main client and returns the URL, to
    /// see how the client addresses the bucket.
    async fn presigned_get(sdk_config: &SdkConfig) -> url::Url {
        let backend = Backend::from_config(
            sdk_config,
            "bucket".into(),
            "lfs".into(),
            None,
            false,
            0,
            Duration::ZERO,
        );
        let request = backend
            .client
            .get_object()
            .bucket("bucket")
            .key(backend.key_to_path(&key()))
            .presigned(
                PresigningConfig::expires_in(Duration::from_secs(60)).unwrap(),
            )
            .await
            .unwrap();
        url::Url::parse(request.uri()).unwrap()
    }

    #[tokio::test]
    async fn custom_endpoint_uses_path_style() {
        let custom = config()
            .into_builder()
            .endpoint_url("http://s3.test:9000")
            .build();
        let url = presigned_get(&custom).await;
        assert_eq!(url.host_str(), Some("s3.test"));
        assert!(url.path().starts_with("/bucket/lfs/"), "{url}");

        let url = presigned_get(&config()).await;
        assert_eq!(url.host_str(), Some("bucket.s3.us-west-2.amazonaws.com"));
        assert!(url.path().starts_with("/lfs/"), "{url}");
    }

    mod credential_refresh {
        use super::*;
        use aws_credential_types::provider::future;
        use std::time::SystemTime;

        /// Hands out credentials numbered from 0. The first expire in 10
        /// minutes, within even the shortest refresh the SDK's jitter picks
        /// (half the 35-minute buffer), and later ones in an hour, beyond the
        /// longest (all of it).
        #[derive(Debug, Default)]
        struct Rotating(AtomicUsize);

        impl aws_sdk_s3::config::ProvideCredentials for Rotating {
            fn provide_credentials<'a>(
                &'a self,
            ) -> future::ProvideCredentials<'a>
            where
                Self: 'a,
            {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                let lifetime = if n == 0 { 10 * 60 } else { 60 * 60 };
                future::ProvideCredentials::ready(Ok(Credentials::new(
                    format!("AKIDROTATING{n}"),
                    "secret",
                    None,
                    Some(SystemTime::now() + Duration::from_secs(lifetime)),
                    "test",
                )))
            }
        }

        fn backend(buffer: Duration) -> Backend {
            let config = SdkConfig::builder()
                .behavior_version(aws_config::BehaviorVersion::v2026_01_12())
                .region(Region::new("us-west-2"))
                .credentials_provider(SharedCredentialsProvider::new(
                    Rotating::default(),
                ))
                .build();
            Backend::from_config(
                &config,
                "bucket".into(),
                "lfs".into(),
                None,
                true,
                0,
                buffer,
            )
        }

        /// The access key ids that sign two successive presigned URLs.
        async fn signing_keys(buffer: Duration) -> Vec<String> {
            let backend = backend(buffer);

            let mut keys = Vec::new();
            for _ in 0..2 {
                let url = backend
                    .upload_url(&key(), Duration::from_secs(15 * 60))
                    .await
                    .unwrap();
                let url = url::Url::parse(&url).unwrap();
                let (_, credential) = url
                    .query_pairs()
                    .find(|(name, _)| name == "X-Amz-Credential")
                    .unwrap();
                keys.push(credential.split('/').next().unwrap().to_string());
            }
            keys
        }

        /// The first credentials are used as they come; once they are within
        /// the buffer of expiring, new ones are fetched before signing.
        ///
        /// Each client draws its own random jitter, so this is repeated: with
        /// credentials the jitter only sometimes catches, it failed about one
        /// time in seven.
        #[tokio::test]
        async fn refreshes_credentials_that_would_cut_urls_short() {
            for _ in 0..50 {
                assert_eq!(
                    signing_keys(DEFAULT_CREDENTIAL_REFRESH_BUFFER).await,
                    ["AKIDROTATING0", "AKIDROTATING1"]
                );
            }
        }

        /// Zero keeps the SDK's 10-second buffer, so the 10-minute credentials
        /// keep signing URLs that will expire with them.
        #[tokio::test]
        async fn zero_keeps_the_sdk_default() {
            assert_eq!(
                signing_keys(Duration::ZERO).await,
                ["AKIDROTATING0", "AKIDROTATING0"]
            );
        }

        /// What `check_credential_lifetime` logs, with the fake provider's
        /// 10-minute credentials.
        async fn lifetime_log(buffer: Duration) -> String {
            use std::sync::{Arc, Mutex};
            use tracing::instrument::WithSubscriber;

            #[derive(Clone, Default)]
            struct Buffer(Arc<Mutex<Vec<u8>>>);
            impl std::io::Write for Buffer {
                fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                    self.0.lock().unwrap().extend_from_slice(buf);
                    Ok(buf.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }

            let buffer_out = Buffer::default();
            let writer = buffer_out.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish();
            backend(buffer)
                .check_credential_lifetime()
                .with_subscriber(subscriber)
                .await;

            let bytes = buffer_out.0.lock().unwrap().clone();
            String::from_utf8(bytes).unwrap()
        }

        #[tokio::test]
        async fn warns_when_credentials_are_too_short_for_the_buffer() {
            let log = lifetime_log(DEFAULT_CREDENTIAL_REFRESH_BUFFER).await;
            assert!(log.contains("AWS credentials expire in"), "{log}");
            assert!(log.contains("WARN"), "{log}");
            assert!(log.contains("--s3-credential-refresh-buffer 0"), "{log}");
        }

        #[tokio::test]
        async fn no_warning_with_the_sdk_default() {
            let log = lifetime_log(Duration::ZERO).await;
            assert!(log.contains("AWS credentials expire in"), "{log}");
            assert!(!log.contains("WARN"), "{log}");
        }
    }

    /// What `abort_upload` logs when S3 answers the abort with `status` and
    /// `body`.
    async fn abort_log(status: u16, body: &str) -> String {
        use std::sync::Mutex as StdMutex;
        use tracing::instrument::WithSubscriber;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        #[derive(Clone, Default)]
        struct Buffer(Arc<StdMutex<Vec<u8>>>);
        impl std::io::Write for Buffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .mount(&server)
            .await;
        let config = config().into_builder().endpoint_url(server.uri()).build();
        let backend = Backend::from_config(
            &config,
            "bucket".into(),
            "lfs".into(),
            None,
            false,
            0,
            Duration::ZERO,
        );

        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        abort_upload(&backend.client, "bucket", "lfs/key", "upload-1")
            .with_subscriber(subscriber)
            .await;

        let bytes = buffer.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    /// An upload that is already gone, e.g. aborted twice when `put` is
    /// dropped during its own abort, is not a failure to warn about.
    #[tokio::test]
    async fn aborting_a_gone_upload_is_not_a_warning() {
        let log = abort_log(
            404,
            "<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>",
        )
        .await;
        assert!(!log.contains("WARN"), "{log}");
    }

    #[tokio::test]
    async fn failed_aborts_are_warnings() {
        let log = abort_log(
            500,
            "<Error><Code>InternalError</Code><Message>oops</Message></Error>",
        )
        .await;
        assert!(log.contains("WARN"), "{log}");
    }

    /// Against a fake S3 whose part uploads are slow, so that `put` can be
    /// dropped while it waits for one.
    mod dropped_while_waiting {
        use super::*;
        use futures::StreamExt;
        use std::sync::Mutex as StdMutex;
        use std::time::Instant;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

        const PART_UPLOAD_TIME: Duration = Duration::from_secs(2);

        /// Responds as `template`, recording when each request arrived.
        struct Recorded(Arc<StdMutex<Vec<Instant>>>, ResponseTemplate);

        impl Respond for Recorded {
            fn respond(&self, _: &Request) -> ResponseTemplate {
                self.0.lock().unwrap().push(Instant::now());
                self.1.clone()
            }
        }

        #[tokio::test]
        async fn abort_waits_for_the_part_in_flight() {
            let server = MockServer::start().await;
            let parts = Arc::new(StdMutex::new(Vec::new()));
            let aborts = Arc::new(StdMutex::new(Vec::new()));
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    "<InitiateMultipartUploadResult><Bucket>bucket</\
                     Bucket><Key>key</Key><UploadId>upload-1</UploadId></\
                     InitiateMultipartUploadResult>",
                ))
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .respond_with(Recorded(
                    parts.clone(),
                    ResponseTemplate::new(200)
                        .insert_header("etag", "\"part\"")
                        .set_delay(PART_UPLOAD_TIME),
                ))
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .respond_with(Recorded(
                    aborts.clone(),
                    ResponseTemplate::new(204),
                ))
                .mount(&server)
                .await;

            let config =
                config().into_builder().endpoint_url(server.uri()).build();
            let backend = Backend::from_config(
                &config,
                "bucket".into(),
                "lfs".into(),
                None,
                false,
                0,
                Duration::ZERO,
            )
            .with_part_size(MIN_PART_SIZE);

            // Two parts arrive at once, so `put` is soon waiting for part 1
            // to finish uploading before it can start part 2; then the client
            // stalls.
            let data: Vec<std::io::Result<Bytes>> =
                vec![Ok(Bytes::from(vec![1u8; 2 * MIN_PART_SIZE]))];
            let stream = stream::iter(data).chain(stream::pending());
            let object =
                LFSObject::new(3 * MIN_PART_SIZE as u64, Box::pin(stream));

            let put = backend.put(key(), object);
            let dropped =
                tokio::time::timeout(Duration::from_millis(500), put).await;
            assert!(dropped.is_err(), "put should still have been waiting");

            let deadline = Instant::now() + PART_UPLOAD_TIME * 3;
            while aborts.lock().unwrap().is_empty() && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            let part_started = parts.lock().unwrap()[0];
            let aborted =
                *aborts.lock().unwrap().first().expect("never aborted");
            assert!(
                aborted.duration_since(part_started) >= PART_UPLOAD_TIME,
                "aborted {:?} into a {:?} part upload",
                aborted.duration_since(part_started),
                PART_UPLOAD_TIME
            );
        }
    }

    /// Retries are left to the SDK. These check what it retries, against a fake
    /// S3, with the configuration `load_config` builds.
    mod retries {
        use super::*;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        async fn backend(server: &MockServer) -> Backend {
            // As `Backend::load_config` does, but with fixed credentials.
            let config = aws_config::defaults(
                aws_config::BehaviorVersion::v2026_01_12(),
            )
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                ACCESS_KEY_ID,
                "secret",
                None,
                None,
                "test",
            ))
            .endpoint_url(server.uri())
            .load()
            .await;
            Backend::from_config(
                &config,
                "bucket".into(),
                "lfs".into(),
                None,
                false,
                0,
                Duration::ZERO,
            )
        }

        fn head(status: u16) -> Mock {
            Mock::given(method("HEAD")).respond_with(
                ResponseTemplate::new(status)
                    .insert_header("content-length", "3"),
            )
        }

        async fn requests(server: &MockServer) -> usize {
            server.received_requests().await.unwrap().len()
        }

        #[tokio::test]
        async fn transient_errors_are_retried() {
            let server = MockServer::start().await;
            head(503)
                .up_to_n_times(2)
                .with_priority(1)
                .mount(&server)
                .await;
            head(200).mount(&server).await;

            let size = backend(&server).await.size(&key()).await.unwrap();

            assert_eq!(size, Some(3));
            assert_eq!(requests(&server).await, 3);
        }

        #[tokio::test]
        async fn retries_stop_after_three_attempts() {
            let server = MockServer::start().await;
            head(500).mount(&server).await;

            assert!(backend(&server).await.size(&key()).await.is_err());
            assert_eq!(requests(&server).await, 3);
        }

        #[tokio::test]
        async fn permanent_errors_are_not_retried() {
            let server = MockServer::start().await;
            head(403).mount(&server).await;

            assert!(backend(&server).await.size(&key()).await.is_err());
            assert_eq!(requests(&server).await, 1);
        }

        #[tokio::test]
        async fn missing_objects_are_not_errors() {
            let server = MockServer::start().await;
            head(404).mount(&server).await;

            let size = backend(&server).await.size(&key()).await.unwrap();

            assert_eq!(size, None);
            assert_eq!(requests(&server).await, 1);
        }
    }
}
