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
use core::task::{Context, Poll};

use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use futures::future::{BoxFuture, FutureExt};
use humantime::format_duration;
use hyper::{Request, Response, body::Incoming};
use tower::Service;

use crate::app::BoxBody;
use crate::auth::UserRepoInfo;
use crate::stats::{RequestClass, STATS};

/// Wraps a service to provide logging on both the request and the response.
#[derive(Debug, Clone)]
pub struct Logger<S> {
    remote_addr: SocketAddr,
    service: S,
}

impl<S> Logger<S> {
    pub fn new(remote_addr: SocketAddr, service: S) -> Self {
        Logger {
            remote_addr,
            service,
        }
    }
}

impl<S> Service<Request<Incoming>> for Logger<S>
where
    S: Service<Request<Incoming>, Response = Response<BoxBody>>,
    S::Future: Send + 'static,
    S::Error: fmt::Display + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let remote_addr = self.remote_addr;
        let class = RequestClass::of(&method, uri.path());

        let start = Instant::now();

        Box::pin(self.service.call(req).inspect(
            move |response| match response {
                Ok(response) => {
                    STATS.request(class, response.status());
                    #[cfg(feature = "otel")]
                    metrics::record(
                        class,
                        &method,
                        Some(response.status()),
                        start.elapsed(),
                    );

                    // Set by the auth layer when GitHub authentication is on.
                    let user = response
                        .extensions()
                        .get::<UserRepoInfo>()
                        .and_then(|user| user.username.as_deref())
                        .unwrap_or("-");

                    // `GET /` is the health check, which would otherwise be
                    // most of the log.
                    if uri.path() == "/" && response.status().is_success() {
                        tracing::debug!(
                            "[{}:{}] {} {} - {} ({})",
                            remote_addr.ip(),
                            remote_addr.port(),
                            method,
                            uri,
                            response.status(),
                            format_duration(start.elapsed()),
                        );
                    } else {
                        tracing::info!(
                            "[{}:{}] {} {} {} - {} ({})",
                            remote_addr.ip(),
                            remote_addr.port(),
                            user,
                            method,
                            uri,
                            response.status(),
                            format_duration(start.elapsed()),
                        );
                    }
                }
                Err(err) => {
                    STATS.failed_request(class);
                    #[cfg(feature = "otel")]
                    metrics::record(class, &method, None, start.elapsed());

                    tracing::error!(
                        "[{}:{}] {} {} - {} ({})",
                        remote_addr.ip(),
                        remote_addr.port(),
                        method,
                        uri,
                        err,
                        format_duration(start.elapsed()),
                    )
                }
            },
        ))
    }
}

/// Request latency as an OTel histogram, following the HTTP semantic
/// conventions. Everything else is counted in [`STATS`] and exported by
/// `init_tracing`.
#[cfg(feature = "otel")]
mod metrics {
    use std::sync::LazyLock;
    use std::time::Duration;

    use http::{Method, StatusCode};
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::Histogram;

    use crate::stats::RequestClass;

    // Created on first use, which is after `main` has installed the meter
    // provider. Without one (as in tests) it is a no-op.
    static DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(env!("CARGO_PKG_NAME"))
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of HTTP server requests.")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                30.0, 60.0, 300.0,
            ])
            .build()
    });

    pub fn record(
        class: RequestClass,
        method: &Method,
        status: Option<StatusCode>,
        elapsed: Duration,
    ) {
        let mut attributes = vec![
            KeyValue::new("http.request.method", method.to_string()),
            KeyValue::new("lfs.request.class", class.as_str()),
        ];
        match status {
            Some(status) => attributes.push(KeyValue::new(
                "http.response.status_code",
                i64::from(status.as_u16()),
            )),
            None => attributes.push(KeyValue::new("error.type", "_OTHER")),
        }
        DURATION.record(elapsed.as_secs_f64(), &attributes);
    }
}
