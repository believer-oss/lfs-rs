use core::fmt;
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
use std::io;

use crate::sha256::{Sha256Error, Sha256VerifyError};
use crate::storage::{self, Storage};

// Define a type so we can return multiple types of errors
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Http(http::Error),
    Hyper(hyper::Error),
    HyperUtil(hyper_util::client::legacy::Error),
    Json(serde_json::Error),
    Sha256(Sha256Error),
    Sha256Verify(Sha256VerifyError),
    S3(Box<storage::S3Error>),
    S3DiskCache(<storage::S3DiskCache as Storage>::Error),
    Askama(askama::Error),
    Infallible(std::convert::Infallible),
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<http::Error> for Error {
    fn from(err: http::Error) -> Self {
        Error::Http(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Self {
        Error::Hyper(err)
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(err: hyper_util::client::legacy::Error) -> Self {
        Error::HyperUtil(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err)
    }
}

impl From<Sha256Error> for Error {
    fn from(err: Sha256Error) -> Self {
        Error::Sha256(err)
    }
}

impl From<Sha256VerifyError> for Error {
    fn from(err: Sha256VerifyError) -> Self {
        Error::Sha256Verify(err)
    }
}

impl From<storage::S3Error> for Error {
    fn from(err: storage::S3Error) -> Self {
        Error::S3(Box::new(err))
    }
}

impl From<<storage::S3DiskCache as Storage>::Error> for Error {
    fn from(err: <storage::S3DiskCache as Storage>::Error) -> Self {
        Error::S3DiskCache(err)
    }
}

impl From<askama::Error> for Error {
    fn from(err: askama::Error) -> Self {
        Error::Askama(err)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(err: std::convert::Infallible) -> Self {
        Error::Infallible(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(error) => write!(f, "I/O error: {}", error),
            Error::Http(error) => write!(f, "HTTP error: {}", error),
            Error::Hyper(error) => write!(f, "Hyper error: {}", error),
            Error::HyperUtil(error) => write!(f, "HyperUtil error: {}", error),
            Error::Json(error) => write!(f, "JSON error: {}", error),
            Error::Sha256(sha256_error) => {
                write!(f, "SHA-256 error: {}", sha256_error)
            }
            Error::Sha256Verify(sha256_verify_error) => {
                write!(f, "SHA-256 verification error: {}", sha256_verify_error)
            }
            Error::S3(error) => write!(f, "S3 error: {}", error),
            Error::S3DiskCache(_) => write!(f, "S3 disk cache error"),
            Error::Askama(error) => write!(f, "Askama error: {}", error),
            Error::Infallible(infallible) => {
                write!(f, "Infallible error: {}", infallible)
            }
        }
    }
}
