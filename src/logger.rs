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

//! The outermost service: one span and one log line per request.
//!
//! Each request (other than the health check) gets a `request` span that is
//! the root of its trace, with the route, status, user and repo, and that
//! lasts until the response body has been sent. The request log line is
//! written, and the request counted, at the same moment.

use core::task::{Context, Poll, ready};

use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use http::{Method, StatusCode, Uri, header};
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::{Request, Response};
use tower::Service;
use tracing::field::Empty;
use tracing::{Instrument, Span};

use crate::app::BoxBody;
use crate::auth::UserRepoInfo;
use crate::stats::{RequestClass, STATS};

/// What a batch request asked for, put on its response by the batch handler
/// for the request span and log line.
#[derive(Clone, Copy, Debug)]
pub struct BatchSummary {
    pub operation: &'static str,
    pub objects: usize,
    pub presigned: usize,
    pub missing: usize,
}

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

/// The route of `path`, with the org, project, OID and lock id replaced by
/// placeholders, so that span names stay few.
pub fn route(path: &str) -> &'static str {
    let mut parts = path.split('/').filter(|p| !p.is_empty());
    match (parts.next(), parts.next(), parts.next()) {
        (None, ..) => return "/",
        (Some("api"), Some(_), Some(_)) => {}
        _ => return "other",
    }
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("objects"), Some("batch"), None, _) => {
            "/api/{org}/{project}/objects/batch"
        }
        (Some("objects"), Some("verify"), None, _) => {
            "/api/{org}/{project}/objects/verify"
        }
        (Some("object"), Some(_), None, _) => {
            "/api/{org}/{project}/object/{oid}"
        }
        (Some("locks"), None, ..) => "/api/{org}/{project}/locks",
        (Some("locks"), Some("verify"), None, _) => {
            "/api/{org}/{project}/locks/verify"
        }
        (Some("locks"), Some("batch"), Some("lock"), None) => {
            "/api/{org}/{project}/locks/batch/lock"
        }
        (Some("locks"), Some("batch"), Some("unlock"), None) => {
            "/api/{org}/{project}/locks/batch/unlock"
        }
        (Some("locks"), Some(_), Some("unlock"), None) => {
            "/api/{org}/{project}/locks/{id}/unlock"
        }
        _ => "other",
    }
}

/// The `org/project` a request is for, if any.
fn repo(path: &str) -> Option<String> {
    let mut parts = path.split('/').filter(|p| !p.is_empty());
    match (parts.next(), parts.next(), parts.next()) {
        (Some("api"), Some(org), Some(project)) => {
            Some(format!("{org}/{project}"))
        }
        _ => None,
    }
}

/// The client's address: the last `X-Forwarded-For` entry, or else the
/// peer's. An AWS load balancer, by default, appends the address it got the
/// request from, so the last entry is the one it vouches for. Anything before
/// it came from the client. It is used as given, so log it quoted.
fn client_address(req: &Request<Incoming>, peer: SocketAddr) -> String {
    req.headers()
        .get_all("x-forwarded-for")
        .iter()
        .next_back()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .map(|client| client.trim().to_string())
        .filter(|client| !client.is_empty())
        .unwrap_or_else(|| peer.ip().to_string())
}

/// The client stopped sending a request's body partway. Handlers return this
/// rather than whatever their storage made of the body ending, so that the
/// request is logged as the client going away and not as the server failing.
///
/// Only a request's own body can say this: a request's errors can also come
/// from the server's own connections (to GitHub, or S3), which fail in the
/// same ways.
#[derive(Debug)]
pub struct ClientAborted(pub anyhow::Error);

impl fmt::Display for ClientAborted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the client stopped sending the request body")
    }
}

impl std::error::Error for ClientAborted {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let err: &(dyn std::error::Error + Send + Sync + 'static) =
            self.0.as_ref();
        Some(err)
    }
}

/// Whether `err`, or an error it came from, is [`ClientAborted`].
fn is_client_abort(err: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(err), |err| err.source())
        .any(|err| err.is::<ClientAborted>())
}

impl<S> Service<Request<Incoming>> for Logger<S>
where
    S: Service<Request<Incoming>, Response = Response<BoxBody>>,
    S::Future: Send + 'static,
    S::Error: fmt::Display
        + AsRef<dyn std::error::Error + Send + Sync + 'static>
        + Send
        + 'static,
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
        let class = RequestClass::of(&method, uri.path());
        let start = Instant::now();

        // `GET /` is the health check: no span, and only a debug line unless
        // it fails, as it would otherwise be most of both.
        if class == RequestClass::Health {
            return Box::pin(self.service.call(req).inspect(move |response| {
                #[cfg(feature = "otel")]
                let elapsed = start.elapsed();
                match response {
                    Ok(response) => {
                        let status = response.status();
                        #[cfg(feature = "otel")]
                        metrics::record(
                            class,
                            &method,
                            Some(status),
                            None,
                            elapsed,
                        );
                        STATS.request(class, status);
                        if status.is_success() {
                            tracing::debug!("{method} {uri} - {status}");
                        } else {
                            tracing::warn!("{method} {uri} - {status}");
                        }
                    }
                    Err(err) => {
                        #[cfg(feature = "otel")]
                        metrics::record(
                            class,
                            &method,
                            None,
                            Some("_OTHER"),
                            elapsed,
                        );
                        STATS.failed_request(class);
                        tracing::error!("{method} {uri} - {err:#}");
                    }
                }
            }));
        }

        let route = route(uri.path());
        let client = client_address(&req, self.remote_addr);
        let received = req
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let repo = repo(uri.path());

        // Integers go on spans as i64s: tracing-opentelemetry records u64s
        // as strings.
        let span = tracing::info_span!(
            "request",
            otel.name = format!("{method} {route}"),
            otel.kind = "server",
            otel.status_code = Empty,
            otel.status_description = Empty,
            http.request.method = %method,
            http.route = route,
            url.path = uri.path(),
            http.response.status_code = Empty,
            http.request.body.size = received.map(|n| n as i64),
            http.response.body.size = Empty,
            client.address = %client,
            user_agent.original = Empty,
            lfs.repo = repo.as_deref(),
            enduser.id = Empty,
            lfs.operation = Empty,
            lfs.objects = Empty,
        );
        if let Some(agent) = req
            .headers()
            .get(header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
        {
            span.record("user_agent.original", agent);
        }

        // The inner services create their spans when called, so call them
        // inside this one for those to be its children.
        let response = span.in_scope(|| self.service.call(req));
        let span_for_future = span.clone();

        // If the client goes away before there is a response, hyper drops
        // this future, and with it `pending`, which then finishes the request.
        let pending = Pending(Some(Finish {
            span,
            start,
            class,
            method,
            uri,
            client,
            status: None,
            user: None,
            repo,
            received,
            sent: 0,
            length: None,
            batch: None,
        }));

        Box::pin(async move {
            let mut pending = pending;
            let result = response.instrument(span_for_future).await;
            let Some(mut finish) = pending.0.take() else {
                return result;
            };

            let response = match result {
                Ok(response) => response,
                Err(err) => {
                    let outcome = if is_client_abort(err.as_ref()) {
                        Outcome::Aborted
                    } else {
                        Outcome::Error(format!("{err:#}"))
                    };
                    finish.done(outcome);
                    return Err(err);
                }
            };

            let status = response.status();
            finish.status = Some(status);
            finish.span.record(
                "http.response.status_code",
                i64::from(status.as_u16()),
            );

            finish.user = response
                .extensions()
                .get::<UserRepoInfo>()
                .and_then(|user| user.username.clone());
            if let Some(user) = &finish.user {
                finish.span.record("enduser.id", user.as_str());
            }
            finish.batch = response.extensions().get::<BatchSummary>().copied();
            if let Some(batch) = finish.batch {
                finish.span.record("lfs.operation", batch.operation);
                finish.span.record("lfs.objects", batch.objects as i64);
            }
            finish.length = response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());

            Ok(response.map(|body| {
                CompletionBody {
                    inner: body,
                    finish: Some(finish),
                }
                .boxed_unsync()
            }))
        })
    }
}

/// How a request ended.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// The whole response was sent.
    Completed,
    /// The client went away before the response was sent.
    Aborted,
    /// Producing the response's body failed.
    Failed(String),
    /// The request failed without a response.
    Error(String),
}

impl Outcome {
    /// The `error.type` for the latency metric, when the status doesn't say.
    /// An abort is one whether or not a response had started, so that
    /// transfers the client gave up on can be told from ones it got.
    #[cfg(any(feature = "otel", test))]
    fn error_type(&self) -> Option<&'static str> {
        match self {
            Outcome::Completed => None,
            Outcome::Aborted => Some("aborted"),
            Outcome::Failed(_) => Some("response_body"),
            Outcome::Error(_) => Some("_OTHER"),
        }
    }
}

/// What is known about a request, for when it finishes.
struct Finish {
    span: Span,
    start: Instant,
    class: RequestClass,
    method: Method,
    uri: Uri,
    client: String,
    /// The response's status, once there is a response.
    status: Option<StatusCode>,
    user: Option<String>,
    repo: Option<String>,
    received: Option<u64>,
    sent: u64,
    /// The response's `Content-Length`, if it has one.
    length: Option<u64>,
    batch: Option<BatchSummary>,
}

/// Finishes a request that is dropped before its response is made.
struct Pending(Option<Finish>);

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(finish) = self.0.take() {
            finish.done(Outcome::Aborted);
        }
    }
}

fn millis(start: Instant) -> f64 {
    (start.elapsed().as_secs_f64() * 10_000.0).round() / 10.0
}

impl Finish {
    /// How a response whose body ended ended: short of its `Content-Length`
    /// is a failure, which hyper reports to the client by closing the
    /// connection.
    fn at_end(&self) -> Outcome {
        match self.length {
            Some(length) if length != self.sent => Outcome::Failed(format!(
                "the body ended after {} of {length} bytes",
                self.sent
            )),
            _ => Outcome::Completed,
        }
    }

    /// How a response whose body was dropped ended. hyper doesn't always
    /// poll a body to its end, so a dropped one was sent in full when:
    /// - it had none to send: a HEAD, 204 or 304 response, or one that says
    ///   it's already over, such as an empty one;
    /// - its `Content-Length` bytes were sent, after which hyper stops.
    fn at_drop(&self, end_stream: bool) -> Outcome {
        let bodiless = self.method == Method::HEAD
            || self.status == Some(StatusCode::NO_CONTENT)
            || self.status == Some(StatusCode::NOT_MODIFIED);
        if bodiless || end_stream || self.length == Some(self.sent) {
            Outcome::Completed
        } else {
            Outcome::Aborted
        }
    }

    /// Ends the request: counts it, completes its span and logs it.
    fn done(self, outcome: Outcome) {
        // A body that failed partway is counted as the server error it was,
        // whatever status its head said.
        match (&outcome, self.status) {
            (Outcome::Failed(_) | Outcome::Error(_), _) => {
                STATS.failed_request(self.class)
            }
            (_, Some(status)) => STATS.request(self.class, status),
            (_, None) => STATS.unanswered_request(self.class),
        }
        #[cfg(feature = "otel")]
        metrics::record(
            self.class,
            &self.method,
            self.status,
            outcome.error_type(),
            self.start.elapsed(),
        );

        let span = &self.span;
        span.record("http.response.body.size", self.sent as i64);
        // An aborted request is an error on the span, so that it can be
        // found, but git-lfs cancels transfers routinely, so it is logged at
        // info.
        let server_error = self.status.is_some_and(|s| s.is_server_error());
        if server_error || outcome != Outcome::Completed {
            let description = match &outcome {
                Outcome::Aborted if self.status.is_none() => {
                    "the client went away before the response was made"
                }
                Outcome::Aborted => {
                    "the client went away before the response was sent"
                }
                Outcome::Failed(err) | Outcome::Error(err) => err.as_str(),
                Outcome::Completed => self
                    .status
                    .and_then(|s| s.canonical_reason())
                    .unwrap_or("server error"),
            };
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", description);
        }

        let level = match &outcome {
            Outcome::Error(_) => tracing::Level::ERROR,
            Outcome::Failed(_) => tracing::Level::WARN,
            _ if server_error => tracing::Level::WARN,
            _ => tracing::Level::INFO,
        };
        let (outcome, error) = match &outcome {
            Outcome::Completed => ("completed", None),
            Outcome::Aborted => ("aborted", None),
            Outcome::Failed(err) => ("failed", Some(err.as_str())),
            Outcome::Error(err) => ("error", Some(err.as_str())),
        };
        let trace_id = trace_id(span);

        // Logged outside the span: the line carries what matters itself.
        // `client` is quoted, as it comes from the client.
        macro_rules! log {
            ($level:expr) => {
                tracing::event!(
                    parent: None,
                    $level,
                    client = ?self.client,
                    user = self.user.as_deref().unwrap_or("-"),
                    method = %self.method,
                    path = %self.uri.path(),
                    status = self.status.map(|s| s.as_u16()),
                    duration_ms = millis(self.start),
                    sent = self.sent,
                    received = self.received,
                    repo = self.repo.as_deref(),
                    op = self.batch.map(|b| b.operation),
                    objects = self.batch.map(|b| b.objects),
                    presigned = self.batch.map(|b| b.presigned),
                    missing = self.batch.map(|b| b.missing),
                    outcome,
                    error,
                    trace_id,
                    "request"
                )
            };
        }
        match level {
            tracing::Level::ERROR => log!(tracing::Level::ERROR),
            tracing::Level::WARN => log!(tracing::Level::WARN),
            _ => log!(tracing::Level::INFO),
        }

        // A span ends when it was last exited, not when it is closed, so this
        // is what ends it.
        drop(self.span.enter());
    }
}

/// The OpenTelemetry trace id of `span`, for linking a log line to its trace.
#[cfg(feature = "otel")]
fn trace_id(span: &Span) -> Option<String> {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let context = span.context();
    let span_context = context.span().span_context().clone();
    span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string())
}

#[cfg(not(feature = "otel"))]
fn trace_id(_span: &Span) -> Option<String> {
    None
}

/// A response body that finishes its request when it has been sent, or when
/// it is dropped unsent because the client went away.
struct CompletionBody<B: Body> {
    inner: B,
    finish: Option<Finish>,
}

impl<B> Body for CompletionBody<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        // In the request's span, so that events from the body's stream (such
        // as a corrupted object being found) are in its trace. This costs an
        // enter and exit per frame, a few locks and atomics, which is small
        // next to the frame's I/O. `done` sets the span's end on its own.
        let span = self.finish.as_ref().map(|finish| finish.span.clone());
        let _entered = span.as_ref().map(Span::enter);

        let frame = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        match &frame {
            Some(Ok(frame)) => {
                if let (Some(data), Some(finish)) =
                    (frame.data_ref(), self.finish.as_mut())
                {
                    finish.sent += data.len() as u64;
                }
            }
            Some(Err(err)) => {
                if let Some(finish) = self.finish.take() {
                    finish.done(Outcome::Failed(format!("{err:#}")));
                }
            }
            None => {
                if let Some(finish) = self.finish.take() {
                    let outcome = finish.at_end();
                    finish.done(outcome);
                }
            }
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B: Body> Drop for CompletionBody<B> {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            let outcome = finish.at_drop(self.inner.is_end_stream());
            finish.done(outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_have_placeholders() {
        let oid =
            "b1fbeefc23e6a149a6f7d0c2fb635bfc78f7ddc2da963ea9c6a63eb324260e6d";
        assert_eq!(route("/"), "/");
        assert_eq!(
            route("/api/org/repo/objects/batch"),
            "/api/{org}/{project}/objects/batch"
        );
        assert_eq!(
            route(&format!("/api/org/repo/object/{oid}")),
            "/api/{org}/{project}/object/{oid}"
        );
        assert_eq!(
            route("/api/org/repo/locks/1234/unlock"),
            "/api/{org}/{project}/locks/{id}/unlock"
        );
        assert_eq!(
            route("/api/org/repo/locks/batch/unlock"),
            "/api/{org}/{project}/locks/batch/unlock"
        );
        // Anything else, such as scanners probing for paths, is one route.
        assert_eq!(route("/wp-login.php"), "other");
        assert_eq!(route("/api/org"), "other");
    }

    fn finish(
        method: Method,
        status: StatusCode,
        length: Option<u64>,
        sent: u64,
    ) -> Finish {
        Finish {
            span: Span::none(),
            start: Instant::now(),
            class: RequestClass::Download,
            method,
            uri: Uri::from_static("/"),
            client: "127.0.0.1".into(),
            status: Some(status),
            user: None,
            repo: None,
            received: None,
            sent,
            length,
            batch: None,
        }
    }

    #[test]
    fn aborts_are_errors_on_the_metric() {
        assert_eq!(Outcome::Aborted.error_type(), Some("aborted"));
        assert_eq!(Outcome::Completed.error_type(), None);
    }

    #[test]
    fn bodies_that_end_short_failed() {
        let short = finish(Method::GET, StatusCode::OK, Some(10), 4);
        assert!(matches!(short.at_end(), Outcome::Failed(_)));

        let whole = finish(Method::GET, StatusCode::OK, Some(10), 10);
        assert_eq!(whole.at_end(), Outcome::Completed);
        let chunked = finish(Method::GET, StatusCode::OK, None, 4);
        assert_eq!(chunked.at_end(), Outcome::Completed);
    }

    #[test]
    fn dropped_bodies_were_sent_when_there_was_nothing_left() {
        use StatusCode as S;
        let sent = [
            finish(Method::HEAD, S::NOT_FOUND, Some(9), 0),
            finish(Method::GET, S::NO_CONTENT, None, 0),
            finish(Method::GET, S::NOT_MODIFIED, Some(10), 0),
            finish(Method::GET, S::OK, Some(10), 10),
        ];
        for finish in &sent {
            assert_eq!(finish.at_drop(false), Outcome::Completed);
        }

        let aborted = finish(Method::GET, S::OK, Some(10), 4);
        assert_eq!(aborted.at_drop(false), Outcome::Aborted);
        assert_eq!(aborted.at_drop(true), Outcome::Completed);
    }

    #[test]
    fn repos_come_from_the_path() {
        assert_eq!(
            repo("/api/org/repo/objects/batch").as_deref(),
            Some("org/repo")
        );
        assert_eq!(repo("/"), None);
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

    /// Records a request that took `elapsed`. `error` is its `error.type`:
    /// why it failed, when its status doesn't say.
    pub fn record(
        class: RequestClass,
        method: &Method,
        status: Option<StatusCode>,
        error: Option<&'static str>,
        elapsed: Duration,
    ) {
        let attributes = attributes(class, method, status, error);
        DURATION.record(elapsed.as_secs_f64(), &attributes);
    }

    fn attributes(
        class: RequestClass,
        method: &Method,
        status: Option<StatusCode>,
        error: Option<&'static str>,
    ) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("http.request.method", method.to_string()),
            KeyValue::new("lfs.request.class", class.as_str()),
        ];
        if let Some(status) = status {
            attributes.push(KeyValue::new(
                "http.response.status_code",
                i64::from(status.as_u16()),
            ));
        }
        // As the HTTP conventions say: a server error's type is its status.
        match (error, status) {
            (Some(error), _) => {
                attributes.push(KeyValue::new("error.type", error));
            }
            (None, Some(status)) if status.is_server_error() => {
                attributes.push(KeyValue::new(
                    "error.type",
                    status.as_u16().to_string(),
                ));
            }
            (None, _) => {}
        }
        attributes
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn error_type(
            status: Option<StatusCode>,
            error: Option<&'static str>,
        ) -> Option<String> {
            attributes(RequestClass::Batch, &Method::POST, status, error)
                .into_iter()
                .find(|kv| kv.key.as_str() == "error.type")
                .map(|kv| kv.value.to_string())
        }

        #[test]
        fn server_errors_have_an_error_type() {
            let status = Some(StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(error_type(status, None).as_deref(), Some("500"));
            assert_eq!(
                error_type(Some(StatusCode::OK), Some("response_body"))
                    .as_deref(),
                Some("response_body")
            );
            assert_eq!(
                error_type(None, Some("_OTHER")).as_deref(),
                Some("_OTHER")
            );
            assert_eq!(error_type(Some(StatusCode::NOT_FOUND), None), None);
            assert_eq!(error_type(Some(StatusCode::OK), None), None);
        }
    }
}
