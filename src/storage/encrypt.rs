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
use chacha20::XChaCha20;
use chacha20::cipher::{KeyIvInit, StreamCipher};
use std::io;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{Stream, stream::StreamExt};

use super::{LFSObject, Storage, StorageKey, StorageStream};

/// A storage adaptor that encrypts/decrypts all data that passes through.
pub struct Backend<S> {
    storage: S,
    key: [u8; 32],
}

impl<S> Backend<S> {
    pub fn new(key: [u8; 32], storage: S) -> Self {
        Backend { key, storage }
    }
}

fn xor_stream<S>(
    mut cipher: XChaCha20,
    stream: S,
) -> impl Stream<Item = Result<Bytes, io::Error>>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
{
    stream.map(move |bytes| {
        let mut bytes = BytesMut::from(bytes?.as_ref());

        cipher.try_apply_keystream(bytes.as_mut()).map_err(|_| {
            io::Error::other("reached end of xchacha20 keystream")
        })?;

        Ok(bytes.freeze())
    })
}

#[async_trait]
impl<S> Storage for Backend<S>
where
    S: Storage + Send + Sync + 'static,
    S::Error: 'static,
{
    type Error = S::Error;

    async fn get(
        &self,
        key: &StorageKey,
    ) -> Result<Option<LFSObject>, Self::Error> {
        // Use the first part of the SHA256 as the nonce.
        let mut nonce: [u8; 24] = [0; 24];
        nonce.copy_from_slice(&key.oid().bytes()[0..24]);

        let cipher = XChaCha20::new(&self.key.into(), &nonce.into());

        Ok(self.storage.get(key).await?.map(move |obj| {
            let (len, stream) = obj.into_parts();
            LFSObject::new(len, Box::pin(xor_stream(cipher, stream)))
        }))
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Self::Error> {
        // Use the first part of the SHA256 as the nonce.
        let mut nonce: [u8; 24] = [0; 24];
        nonce.copy_from_slice(&key.oid().bytes()[0..24]);

        let cipher = XChaCha20::new(&self.key.into(), &nonce.into());

        let (len, stream) = value.into_parts();
        let stream = xor_stream(cipher, stream);

        self.storage
            .put(key, LFSObject::new(len, Box::pin(stream)))
            .await
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Self::Error> {
        self.storage.size(key).await
    }

    async fn delete(&self, key: &StorageKey) -> Result<(), Self::Error> {
        self.storage.delete(key).await
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Self::Error> {
        self.storage.list()
    }

    async fn total_size(&self) -> Option<u64> {
        self.storage.total_size().await
    }

    async fn max_size(&self) -> Option<u64> {
        self.storage.max_size().await
    }

    fn public_url(&self, key: &StorageKey) -> Option<String> {
        self.storage.public_url(key)
    }

    async fn upload_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        self.storage.upload_url(key, expires_in).await
    }

    async fn download_url(
        &self,
        key: &StorageKey,
        expires_in: Duration,
    ) -> Option<String> {
        self.storage.download_url(key, expires_in).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{TryStreamExt, stream};

    const KEY: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
        0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    ];

    fn nonce() -> [u8; 24] {
        std::array::from_fn(|i| 0xa0 + i as u8)
    }

    fn plaintext() -> Vec<u8> {
        (0..300u32).map(|i| (i * 7 % 251) as u8).collect()
    }

    /// What the `chacha` crate, used before `chacha20`, produced for
    /// `plaintext()`. Objects already stored encrypted must still decrypt,
    /// so this must never change.
    const CIPHERTEXT: &str = "b96c244bb106b8a5a5ed2613ef0b667520c0d188707c370702729fdd589f641eb6cabf6cba76c2ff32e3fa7db12b2923dc840652832b1a079aea92b518a63c8e8020d9c756b54b5a63663d9e6f158ff301ebd763fc5baeb26f390b60693662957b361290808ba81747029ea1983ebe36ba000bcde12a99f7ade964865a5bf30be9e47bd80759d1fd5ea29fb09bb7cd87c86cc8513625953763b06c56cf01705d7e292ced49da45ecd0e2690ed84bf2757df27ee485e18483f3ec453ede4739a1074b0414a9d390b5a2a1c2303f667639a0385455837e592f2b4eaf6e1f2de399f7fb0dfa7ffea78e5d42110b49f360d1e4c514b0f9a83d8a1171a140e323f8485e442f579d0ae6ddac9058ecc37bf1287196fb7645ea9d54e6522fee225934fbb6f38e95f489bbf114652fca";

    /// Encrypts `plaintext()` as a stream of chunks of the given sizes.
    async fn encrypt(chunks: &[usize]) -> Vec<u8> {
        let data = plaintext();
        let mut offset = 0;
        let chunks: Vec<Result<Bytes, io::Error>> = chunks
            .iter()
            .map(|&len| {
                let chunk = Bytes::copy_from_slice(&data[offset..offset + len]);
                offset += len;
                Ok(chunk)
            })
            .collect();
        assert_eq!(offset, data.len());

        let cipher = XChaCha20::new(&KEY.into(), &nonce().into());
        let out: Vec<Bytes> = xor_stream(cipher, stream::iter(chunks))
            .try_collect()
            .await
            .unwrap();
        out.concat()
    }

    #[tokio::test]
    async fn matches_previous_ciphertext() {
        let expected = hex::decode(CIPHERTEXT).unwrap();
        assert_eq!(encrypt(&[1, 63, 64, 100, 72]).await, expected);
    }

    /// Chunks can be any size and split blocks anywhere; the keystream
    /// carries on across them.
    #[tokio::test]
    async fn chunking_does_not_matter() {
        let expected = hex::decode(CIPHERTEXT).unwrap();
        assert_eq!(encrypt(&[300]).await, expected);
        assert_eq!(encrypt(&[64, 64, 64, 64, 44]).await, expected);
        assert_eq!(
            encrypt(&[7; 42].iter().copied().chain([6]).collect::<Vec<_>>())
                .await,
            expected
        );
    }

    /// XOR with the same keystream decrypts.
    #[tokio::test]
    async fn decrypts() {
        let ciphertext = Bytes::from(hex::decode(CIPHERTEXT).unwrap());
        let cipher = XChaCha20::new(&KEY.into(), &nonce().into());
        let out: Vec<Bytes> =
            xor_stream(cipher, stream::iter([Ok::<_, io::Error>(ciphertext)]))
                .try_collect()
                .await
                .unwrap();
        assert_eq!(out.concat(), plaintext());
    }
}
