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
use core::pin::Pin;
use core::task::{Context, Poll};

use std::fmt;
use std::ops;
use std::str::FromStr;

use futures::{Stream, ready};
use hex::{FromHex, FromHexError, ToHex};
use serde::{
    Deserialize, Serialize,
    de::{self, Deserializer, Visitor},
    ser::Serializer,
};

use sha2::{self, Digest};

/// An error associated with parsing a SHA256.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sha256Error(FromHexError);

impl From<FromHexError> for Sha256Error {
    fn from(error: FromHexError) -> Self {
        Sha256Error(error)
    }
}

impl fmt::Display for Sha256Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Sha256Error {}

/// A Git LFS object ID (i.e., a SHA256).
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct Sha256([u8; 32]);

impl Sha256 {
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns an object that can be formatted as a path.
    pub fn path(&'_ self) -> Sha256Path<'_> {
        Sha256Path(self)
    }
}

pub struct Sha256Path<'a>(&'a Sha256);

impl fmt::Display for Sha256Path<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:02x}/{:02x}/{}",
            self.0.bytes()[0],
            self.0.bytes()[1],
            self.0
        )
    }
}

impl AsRef<[u8]> for Sha256 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 32]> for Sha256 {
    fn from(arr: [u8; 32]) -> Self {
        Sha256(arr)
    }
}

/// Converts the output of a SHA256 hasher, without naming the array type the
/// `digest` crate happens to use.
impl From<sha2::digest::Output<sha2::Sha256>> for Sha256 {
    fn from(arr: sha2::digest::Output<sha2::Sha256>) -> Self {
        Sha256(arr.into())
    }
}

impl FromHex for Sha256 {
    type Error = Sha256Error;

    fn from_hex<T>(hex: T) -> Result<Self, Self::Error>
    where
        T: AsRef<[u8]>,
    {
        Ok(Sha256::from(<[u8; 32]>::from_hex(hex)?))
    }
}

impl FromStr for Sha256 {
    type Err = Sha256Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl fmt::LowerHex for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(hex::encode(self).as_str())
    }
}

impl fmt::Display for Sha256 {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::LowerHex>::fmt(self, f)
    }
}

impl fmt::Debug for Sha256 {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Display>::fmt(self, f)
    }
}

impl ops::Deref for Sha256 {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for Sha256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let hex: String = self.encode_hex();
            serializer.serialize_str(hex.as_str())
        } else {
            // Serialize as a byte array with known length.
            serializer.serialize_bytes(self.0.as_ref())
        }
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Sha256Visitor;

        impl Visitor<'_> for Sha256Visitor {
            type Value = Sha256;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "hex string or 32 bytes")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let v = Sha256::from_hex(v).map_err(|e| match e.0 {
                    FromHexError::InvalidHexCharacter { c, .. } => {
                        E::invalid_value(
                            de::Unexpected::Char(c),
                            &"string with only hexadecimal characters",
                        )
                    }
                    FromHexError::InvalidStringLength => E::invalid_length(
                        v.len(),
                        &"hex string with a valid length",
                    ),
                    FromHexError::OddLength => E::invalid_length(
                        v.len(),
                        &"hex string with an even length",
                    ),
                })?;

                Ok(v)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() != 32 {
                    return Err(E::invalid_length(v.len(), &"32 bytes"));
                }

                let mut inner = <[u8; 32]>::default();
                inner.copy_from_slice(v);

                Ok(Sha256::from(inner))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(Sha256Visitor)
        } else {
            deserializer.deserialize_bytes(Sha256Visitor)
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct Sha256VerifyError {
    pub expected: Sha256,
    pub found: Sha256,
}

impl fmt::Display for Sha256VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "expected SHA256 '{}', but found '{}'",
            self.expected, self.found
        )
    }
}

impl std::error::Error for Sha256VerifyError {}

/// A stream adaptor that verifies the SHA256 of a byte stream.
pub struct VerifyStream<S> {
    /// The underlying stream.
    stream: S,

    /// The total size of the stream.
    total: u64,

    /// The size so far.
    len: u64,

    /// The expected SHA256.
    expected: Sha256,

    /// The current state of the hasher.
    hasher: sha2::Sha256,

    /// Whether the digest has been checked. It is checked on the chunk that
    /// completes the stream, or at its end if no chunk did: an empty stream,
    /// or one shorter than `total`.
    checked: bool,

    /// Whether the source has ended. It isn't polled again after that.
    ended: bool,
}

impl<S> VerifyStream<S> {
    pub fn new(stream: S, total: u64, expected: Sha256) -> Self {
        VerifyStream {
            stream,
            total,
            len: 0,
            expected,
            hasher: sha2::Sha256::default(),
            checked: false,
            ended: false,
        }
    }
}

impl<S, T, E> Stream for VerifyStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: AsRef<[u8]>,
    E: From<Sha256VerifyError>,
{
    type Item = S::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }

        match ready!(Stream::poll_next(Pin::new(&mut self.stream), cx)) {
            Some(bytes) => match bytes {
                Ok(bytes) => {
                    self.len += bytes.as_ref().len() as u64;

                    // Continuously hash the bytes as we receive them.
                    self.hasher.update(bytes.as_ref());

                    if self.checked {
                        // Past the end of the object, where only empty chunks
                        // may come.
                        if bytes.as_ref().is_empty() {
                            return Poll::Ready(Some(Ok(bytes)));
                        }
                        let found = Sha256::from(self.hasher.finalize_reset());
                        return Poll::Ready(Some(Err(E::from(
                            Sha256VerifyError {
                                found,
                                expected: self.expected,
                            },
                        ))));
                    }

                    if self.len >= self.total {
                        // This is the last chunk in the stream. Verify that the
                        // digest matches.
                        self.checked = true;
                        let found = Sha256::from(self.hasher.finalize_reset());

                        if found == self.expected {
                            Poll::Ready(Some(Ok(bytes)))
                        } else {
                            Poll::Ready(Some(Err(E::from(Sha256VerifyError {
                                found,
                                expected: self.expected,
                            }))))
                        }
                    } else {
                        Poll::Ready(Some(Ok(bytes)))
                    }
                }
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            None if self.checked => {
                self.ended = true;
                Poll::Ready(None)
            }
            None => {
                self.ended = true;
                // The stream ended without a chunk completing it: it is empty,
                // or shorter than it should be. Otherwise an empty upload would
                // be stored under any OID.
                self.checked = true;
                let found = Sha256::from(self.hasher.finalize_reset());
                if self.len == self.total && found == self.expected {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(E::from(Sha256VerifyError {
                        found,
                        expected: self.expected,
                    }))))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256() {
        let s =
            "b1fbeefc23e6a149a6f7d0c2fb635bfc78f7ddc2da963ea9c6a63eb324260e6d";
        assert_eq!(Sha256::from_str(s).unwrap().to_string(), s);
    }
}

#[cfg(test)]
mod verify_tests {
    use super::*;
    use futures::StreamExt;
    use futures::stream;

    type Chunk = Result<Vec<u8>, Sha256VerifyError>;

    fn oid(data: &[u8]) -> Sha256 {
        Sha256::from(sha2::Sha256::digest(data))
    }

    /// Runs `chunks` through a verifier expecting `data`, and returns whether
    /// it reported a mismatch.
    fn rejects(data: &[u8], chunks: &[&[u8]]) -> bool {
        let chunks: Vec<Chunk> =
            chunks.iter().map(|c| Ok(c.to_vec())).collect();
        let stream = VerifyStream::new(
            stream::iter(chunks),
            data.len() as u64,
            oid(data),
        );
        let results: Vec<Chunk> = futures::executor::block_on(stream.collect());
        results.iter().any(Result::is_err)
    }

    #[test]
    fn accepts_the_right_bytes_however_chunked() {
        assert!(!rejects(b"hello world", &[b"hello world"]));
        assert!(!rejects(b"hello world", &[b"hello", b" ", b"world"]));
        // An empty chunk after the end is harmless.
        assert!(!rejects(b"hello world", &[b"hello world", b""]));
        assert!(!rejects(b"", &[]));
        assert!(!rejects(b"", &[b""]));
    }

    #[test]
    fn rejects_the_wrong_bytes() {
        assert!(rejects(b"hello world", &[b"hello there"]));
        // More than expected.
        assert!(rejects(b"hello world", &[b"hello world", b"!"]));
    }

    /// A source that panics if polled again after it ended, as some streams
    /// may.
    struct Unfused(std::collections::VecDeque<Chunk>, bool);

    impl Stream for Unfused {
        type Item = Chunk;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _: &mut Context,
        ) -> Poll<Option<Chunk>> {
            assert!(!self.1, "polled after it ended");
            let next = self.0.pop_front();
            self.1 = next.is_none();
            Poll::Ready(next)
        }
    }

    /// After reporting a short stream at its end, the verifier ends too,
    /// rather than polling its finished source again.
    #[test]
    fn ends_after_reporting_a_short_stream() {
        let source = Unfused([Ok(b"hello".to_vec())].into(), false);
        let stream = VerifyStream::new(source, 11, oid(b"hello world"));
        let results: Vec<Chunk> = futures::executor::block_on(stream.collect());
        assert_eq!(results.len(), 2);
        assert!(results[1].is_err());
    }

    /// A stream that ends early, or an empty one, was never checked, so an
    /// empty upload could be stored under any OID.
    #[test]
    fn rejects_short_and_empty_streams() {
        assert!(rejects(b"hello world", &[b"hello"]));
        assert!(rejects(b"hello world", &[]));
        assert!(rejects(b"hello world", &[b""]));
    }
}
