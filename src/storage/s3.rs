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
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use derive_more::{Display, From};
use futures::{stream, stream::TryStreamExt};
use http::{HeaderMap, StatusCode};
use tokio::io::AsyncReadExt;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{
    CompletedMultipartUpload,
    CompletedPart
};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_smithy_types::byte_stream::{ByteStream, Length};

use super::{LFSObject, Storage, StorageKey, StorageStream};
use log::Level::Error;
use std::time::Duration;

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

    region: aws_config::Region,
}

impl Backend {
    pub async fn new(
        bucket: String,
        mut prefix: String,
        cdn: Option<String>,
    ) -> Self {
        // Ensure the prefix doesn't end with a '/'.
        while prefix.ends_with('/') {
            prefix.pop();
        }

        let client: Client;
        let shared_config = aws_config::load_defaults(BehaviorVersion::v2024_03_28());
        let sdk_config = shared_config.load().await;
        client = Client::new(&sdk_config);

        // Perform a HEAD operation to check that the bucket exists and that
        // our credentials work. This helps catch very common errors early on
        // in application startup.
        let resp = client
            .head_bucket()
            .bucket(bucket.clone())
            .send()
            .await;
        if resp.is_err() {
            log::error!("Failed to connect to S3 bucket '{}'", bucket);
        }
        else {
            log::info!(
                "Connecting to S3 bucket '{}' at region '{}'",
                bucket,
                sdk_config.region().unwrap_or_default()
            );
        }

        Backend {
            client,
            bucket,
            prefix,
            cdn,
            region: sdk_config.region().unwrap_or_default(),
        }
    }
}

impl Backend {
    fn key_to_path(&self, key: &StorageKey) -> String {
        format!("{}/{}/{}", self.prefix, key.namespace(), key.oid().path())
    }
}

#[async_trait]
impl Storage for Backend
{
    type Error = aws_sdk_s3::Error;

    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .send()
            .await;

        match resp {
            Ok(get_object_output) => Ok(Some(LFSObject::new(
                get_object_output.content_length.unwrap() as u64,
                Box::pin(get_object_output.body.map_ok(Bytes::from)),
            ))),
            Err(GetObjectError::NoSuchKey(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        let (_len, futures_stream) = value.into_parts();

        // Create a multipart upload. Use UploadPart and CompleteMultipartUpload to upload the file.
        let multipart_upload_resp = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(&key))
            .send()
            .await?;

        // Okay to unwrap. This would only be None there is a bug in S3
        let upload_id = multipart_upload_resp.upload_id.unwrap();

        // 100 MB
        const CHUNK_SIZE: u64 = 100 * 1024 * 1024;

        let path = self.key_to_path(&key);
        let file_size = tokio::fs::metadata(path.clone())
            .await
            .unwrap()
            .len();

        let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
        let mut last_chunk_size = file_size % CHUNK_SIZE;
        if last_chunk_size == 0 {
            last_chunk_size = CHUNK_SIZE;
            chunk_count -= 1;
        }

        let mut upload_parts : Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();

        for chunk_index in 0..chunk_count {
            let curr_chunk = if chunk_count - 1 == chunk_index {
                last_chunk_size
            } else {
                CHUNK_SIZE
            };

            // convert byte_stream to aws_smithy_types::byte_stream::ByteStream
            let stream = ByteStream::read_from()
                .path(path.clone())
                .offset(chunk_index * CHUNK_SIZE)
                .length(Length::Exact(curr_chunk))
                .build()
                .await
                .unwrap();

            // chunk_index needs to start at 0, but part_number starts at 1
            let part_number = (chunk_index as i32) + 1;
            let upload_part_resp = self
                .client
                .upload_part()
                .bucket(self.bucket.clone())
                .key(self.key_to_path(&key))
                .part_number(part_number)
                .upload_id(upload_id.clone())
                .body(stream)
                .send()
                .await?;

            upload_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(upload_part_resp.e_tag.unwrap_or_default())
                    .build(),
            );
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _complete_multipart_upload_resp = self
            .client
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(&key))
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        let resp = self
            .client
            .head_object()
            .bucket(self.bucket.clone())
            .key(self.key_to_path(key))
            .send()
            .await?;

        match resp {
            Ok(head_object_output) => Ok(Some(head_object_output.content_length.unwrap() as u64)),
            Err(HeadObjectError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
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

        let presigning_config = PresigningConfig::expires_in(expires_in).unwrap();
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
