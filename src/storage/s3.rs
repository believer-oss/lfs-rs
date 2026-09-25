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
use aws_config::{Region, SdkConfig};
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
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
use aws_sdk_s3::Client;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::BytesMut;
use futures::{stream, TryStreamExt};
use tokio::io::AsyncReadExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

#[cfg(feature = "otel")]
use tracing::instrument;

use super::{LFSObject, Storage, StorageKey, StorageStream};
use crate::lru;
use derive_more::{Display, From};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
        );
        backend.check().await?;
        Ok(backend)
    }

    /// Loads the AWS configuration from the default credential and region
    /// chains. If `$AWS_S3_ENDPOINT` is set, it is used as a custom endpoint
    /// (e.g. an S3-compatible server) and a region must also be set.
    pub async fn load_config() -> Result<SdkConfig, anyhow::Error> {
        let mut shared_config =
            aws_config::defaults(aws_config::BehaviorVersion::v2024_03_28());

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
    ) -> Self {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        let service_config = aws_sdk_s3::config::Builder::from(sdk_config)
            .force_path_style(sdk_config.endpoint_url().is_some())
            .build();
        let client = Client::from_conf(service_config);

        // S3 client used for signing accelerate upload and download URLs.
        let accelerate_client = if s3_accelerate {
            let service_config = aws_sdk_s3::config::Builder::from(sdk_config)
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

    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let (_len, stream) = value.into_parts();

        // Create a multipart upload. Use UploadPart and CompleteMultipartUpload
        // to upload the file.
        let multipart_upload_resp = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(&key))
            .send()
            .await
            .map_err(Error::from)?;

        // Okay to unwrap. This would only be None there is a bug in S3
        let upload_id = multipart_upload_resp.upload_id.unwrap();

        // 100 MB
        const CHUNK_SIZE: usize = 100 * 1024 * 1024;

        let mut buffer = BytesMut::with_capacity(CHUNK_SIZE);
        let mut part_number = 1;
        let mut completed_parts: Vec<aws_sdk_s3::types::CompletedPart> =
            Vec::new();
        let mut streaming_body = stream.into_async_read().compat();

        loop {
            let size = streaming_body.read_buf(&mut buffer).await?;

            if buffer.len() < CHUNK_SIZE && size != 0 {
                continue;
            }

            let chunk = buffer.split().freeze();

            let stream = ByteStream::new(SdkBody::from(chunk));

            let upload_part_resp = self
                .client
                .upload_part()
                .bucket(self.bucket.clone())
                .key(self.key_to_path(&key))
                .part_number(part_number)
                .upload_id(upload_id.clone())
                .body(stream)
                .send()
                .await
                .map_err(Error::from)?;

            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(upload_part_resp.e_tag.unwrap_or_default())
                    .build(),
            );

            if size == 0 {
                // The stream has ended.
                break;
            } else {
                part_number += 1;
            };
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let _complete_multipart_upload_resp = self
            .client
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(&key))
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(Error::from)?;

        Ok(())
    }

    #[cfg_attr(feature = "otel", instrument(level = "info", skip(self)))]
    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        // Check cache first if enabled
        if let Some(cache) = &self.size_cache {
            if let Some(size) = cache.lock().get_refresh(key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("S3 size cache hit for {}", key.oid());
                return Ok(Some(size));
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
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
            .behavior_version(aws_config::BehaviorVersion::v2024_03_28())
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
}
