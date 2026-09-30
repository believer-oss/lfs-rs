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

pub use anyhow::Error;

use std::error::Error as StdError;
use std::fmt;

/// The client stopped sending a request's body partway. Handlers return this
/// rather than whatever their storage made of the body ending, so that the
/// request is logged as the client going away and not as the server failing.
///
/// Only a request's own body can say this: a request's errors can also come
/// from the server's own connections (to GitHub, or S3), which fail in the
/// same ways.
#[derive(Debug)]
pub struct ClientAborted(pub Error);

impl fmt::Display for ClientAborted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the client stopped sending the request body")
    }
}

impl StdError for ClientAborted {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        let err: &(dyn StdError + Send + Sync + 'static) = self.0.as_ref();
        Some(err)
    }
}

/// The request was invalid: its JSON didn't parse, or its object didn't
/// match its OID. It is answered with a 400 saying why.
#[derive(Debug)]
pub struct BadRequest(pub Error);

impl fmt::Display for BadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bad request: {:#}", self.0)
    }
}

impl StdError for BadRequest {}

/// The first error of type `E` among `err` and the errors it came from.
pub fn find<'a, E: StdError + 'static>(
    err: &'a (dyn StdError + 'static),
) -> Option<&'a E> {
    let mut next = Some(err);
    while let Some(err) = next {
        if let Some(err) = err.downcast_ref::<E>() {
            return Some(err);
        }
        next = err.source();
    }
    None
}
