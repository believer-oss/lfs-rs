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
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::{
    complete_multipart_upload::CompleteMultipartUploadError,
    create_multipart_upload::CreateMultipartUploadError,
    get_object::GetObjectError, head_bucket::HeadBucketError,
    head_object::HeadObjectError, put_object::PutObjectError,
    upload_part::UploadPartError,
};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::BytesMut;
use futures::{stream, TryStreamExt};
use tokio::io::AsyncReadExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;
use tracing::instrument;

use super::{LFSObject, Storage, StorageKey, StorageStream};
use derive_more::{Display, From};
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

    /// Initialization error.
    Init(InitError),

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

#[derive(Debug, Display)]
pub enum InitError {
    #[display("Invalid S3 bucket name")]
    Bucket,

    #[display("Invalid S3 credentials")]
    Credentials,

    #[display("{}", _0)]
    Other(String),
}

impl InitError {
    /// Converts the initialization error into a backoff error. Useful for not
    /// retrying certain errors.
    pub fn into_backoff(self) -> backoff::Error<InitError> {
        // Certain types of errors should never be retried.
        match self {
            InitError::Bucket | InitError::Credentials => {
                backoff::Error::Permanent(self)
            }
            _ => backoff::Error::Transient {
                err: self,
                retry_after: None, /* NOTE: None causes us to follow retry
                                    * policy here */
            },
        }
    }
}

impl From<HeadBucketError> for InitError {
    fn from(err: HeadBucketError) -> Self {
        match err {
            HeadBucketError::NotFound(_not_found) => InitError::Bucket,
            x => match x.meta().code() {
                Some(status) => InitError::Other(format!(
                    "S3 returned HTTP status {}",
                    status
                )),
                None => InitError::Other(x.to_string()),
            },
        }
    }
}

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
}

impl Backend {
    pub async fn new(
        bucket: String,
        mut prefix: String,
        cdn: Option<String>,
    ) -> Result<Self, Error> {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        let (region, endpoint_url) = if let Ok(endpoint) =
            std::env::var("AWS_S3_ENDPOINT")
        {
            // If a custom endpoint is set, do not use the AWS default
            // (us-east-1). Instead, check environment variables for a region
            // name.
            let name = std::env::var("AWS_DEFAULT_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .map_err(|_| {
                    InitError::Other(
                        "$AWS_S3_ENDPOINT was set without $AWS_DEFAULT_REGION \
                         or $AWS_REGION being set. Custom endpoints don't \
                         make sense without also setting a region."
                            .into(),
                    )
                })?;
            (Region::new(name), Some(endpoint))
        } else {
            (Region::new("us-east-1"), None)
        };

        let client: Client;
        let mut shared_config =
            aws_config::defaults(aws_config::BehaviorVersion::v2024_03_28());
        if endpoint_url.is_some() {
            shared_config = shared_config.endpoint_url(endpoint_url.unwrap());
            shared_config = shared_config.region(region.clone());
        }
        let sdk_config = shared_config.load().await;
        client = Client::new(&sdk_config);

        // Perform a HEAD operation to check that the bucket exists and that
        // our credentials work. This helps catch very common errors early on
        // in application startup.
        let resp = client.head_bucket().bucket(bucket.clone()).send().await;
        if resp.is_err() {
            log::error!("Failed to connect to S3 bucket '{}'", bucket);
        } else {
            log::info!(
                "Connecting to S3 bucket '{}' at region '{}'",
                bucket,
                sdk_config
                    .region()
                    .unwrap_or(&aws_config::Region::new("us-east-1"))
            );
        }

        Ok(Backend {
            client,
            bucket,
            prefix,
            cdn,
        })
    }

    fn key_to_path(&self, key: &StorageKey) -> String {
        format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
    }
}

#[async_trait]
impl Storage for Backend {
    type Error = Error;

    #[instrument(skip(self))]
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

    #[instrument(skip(self, value))]
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

    #[instrument(skip(self))]
    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        let resp = self
            .client
            .head_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .send()
            .await;

        match resp {
            Ok(head_object_output) => {
                Ok(Some(head_object_output.content_length.unwrap() as u64))
            }
            Err(e) => {
                let e = e.into_service_error();
                if let HeadObjectError::NotFound(_) = e {
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

    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.cdn
            .as_ref()
            .map(|cdn| format!("{}/{}", cdn, self.key_to_path(key)))
    }

    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        // Don't use a presigned URL if we're not using a CDN. Otherwise,
        // uploads will bypass the encryption process and fail to download.
        self.cdn.as_ref()?;

        let presigning_config =
            PresigningConfig::expires_in(expires_in).unwrap();
        let resp = self
            .client
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
