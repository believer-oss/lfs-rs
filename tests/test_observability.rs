// Copyright (c) 2021 Jason White
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

//! What the server reports about requests: their spans and their log lines.
#![cfg(feature = "otel")]

mod common;

use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lfs_rs::{Cache, LocalLs, LocalServerBuilder, S3ServerBuilder};
use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
use opentelemetry::{Key, Value};
use opentelemetry_sdk::trace::{
    InMemorySpanExporter, SdkTracerProvider, SpanData,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use common::SERVER_ADDR;

/// `testuser1:pass`, a user the mock GitHub knows.
const AUTH: &str = "Basic dGVzdHVzZXIxOnBhc3M=";

/// Well beyond what socket buffers hold, even tuned up, so sending it waits
/// on the client.
const OBJECT_SIZE: usize = 32 * 1024 * 1024;

/// As a load balancer that appends the address it got the request from
/// sends it, after one the client sent.
const FORWARDED_FOR: &str = "198.51.100.1, 203.0.113.7";

/// A server with spans and log lines captured.
struct Observed {
    addr: SocketAddr,
    spans: InMemorySpanExporter,
    logs: Arc<Mutex<Vec<u8>>>,
    _provider: SdkTracerProvider,
    _data: tempfile::TempDir,
    _mock: MockServer,
    _guard: tracing::subscriber::DefaultGuard,
}

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A GitHub that knows `testuser1`, who can push to `test/test`, and takes
/// `delay` to say so.
async fn mock_github(delay: Duration) -> MockServer {
    let github = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/test/test"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(
                    r#"{"id": 1, "permissions": {"push": true, "pull": true}}"#,
                    "application/json",
                )
                .set_delay(delay),
        )
        .mount(&github)
        .await;
    Mock::given(method("GET"))
        .and(path("/user"))
        .and(header("Authorization", AUTH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(r#"{"login": "testuser1"}"#, "application/json"),
        )
        .mount(&github)
        .await;
    github
}

/// Starts a server on this thread, which must be a current-thread runtime's,
/// so that the server's tasks report to this subscriber.
async fn observed() -> Result<Observed, Box<dyn std::error::Error>> {
    observed_with(Options::default()).await
}

/// How [`observed_with`] sets up the server.
#[derive(Default)]
struct Options {
    /// How long the mock GitHub takes to answer.
    github_delay: Duration,
    /// A GitHub to use instead of the mock.
    github: Option<String>,
    /// S3 to store objects in, behind a disk cache as in production,
    /// instead of local storage.
    s3: Option<common::S3Target>,
}

/// [`observed`], set up as `options` says.
async fn observed_with(
    options: Options,
) -> Result<Observed, Box<dyn std::error::Error>> {
    let spans = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(spans.clone())
        .build();
    let logs = Arc::new(Mutex::new(Vec::new()));

    let writer = LogWriter(logs.clone());
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("test"))
                .with_filter(
                    Targets::new().with_target("lfs_rs", LevelFilter::DEBUG),
                ),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(
                    Targets::new().with_target("lfs_rs", LevelFilter::DEBUG),
                ),
        );
    let guard = tracing::subscriber::set_default(subscriber);

    let data = tempfile::TempDir::new()?;
    let mock = mock_github(options.github_delay).await;
    let github = options.github.unwrap_or_else(|| mock.uri());
    let locks = LocalLs::new(data.path().join("locks")).await?;
    let (server, addr) = match options.s3 {
        Some(s3) => {
            let mut server = S3ServerBuilder::new(s3.bucket, None);
            server.prefix("observability".into());
            server.sdk_config(s3.config);
            server.cache(Cache::new(data.path().join("cache"), 1 << 30));
            server.authenticated(true);
            server.authentication_server(github);
            server.spawn(SERVER_ADDR, locks).await?
        }
        None => {
            let objects = data.path().join("objects");
            let mut server = LocalServerBuilder::new(objects, None);
            server.authenticated(true);
            server.authentication_server(github);
            server.spawn(SERVER_ADDR, locks).await?
        }
    };
    tokio::spawn(server);

    Ok(Observed {
        addr,
        spans,
        logs,
        _provider: provider,
        _data: data,
        _mock: mock,
        _guard: guard,
    })
}

impl Observed {
    /// Sends a request and returns the connection with the response unread.
    async fn send(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> std::io::Result<TcpStream> {
        let headers = format!(
            "X-Forwarded-For: {FORWARDED_FOR}\r\nContent-Length: {}\r\n",
            body.len()
        );
        let mut stream = self.send_head(method, path, &headers).await?;
        stream.write_all(body).await?;
        Ok(stream)
    }

    /// Sends a request's head, with `headers` (each ending in CRLF) as well
    /// as the usual ones, and returns the connection.
    async fn send_head(
        &self,
        method: &str,
        path: &str,
        headers: &str,
    ) -> std::io::Result<TcpStream> {
        // A small receive buffer, so that a big response can't all be
        // buffered before the client reads it.
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_recv_buffer_size(16 * 1024)?;
        let mut stream = socket.connect(self.addr).await?;
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: \
             {AUTH}\r\nUser-Agent: git-lfs/test\r\n{headers}Connection: \
             close\r\n\r\n",
        );
        stream.write_all(head.as_bytes()).await?;
        Ok(stream)
    }

    /// Waits for the server to have closed `n` connections.
    async fn closed(&self, n: usize) {
        for _ in 0..200 {
            if self.logs().matches("connection dropped").count() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{n} connections weren't closed:\n{}", self.logs());
    }

    /// Sends a request and returns the response's status and body.
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> std::io::Result<(u16, Vec<u8>)> {
        let mut response = Vec::new();
        self.send(method, path, body)
            .await?
            .read_to_end(&mut response)
            .await?;
        Ok(parse(&response))
    }

    /// Waits for the root span named `name` to end.
    async fn root(&self, name: &str) -> SpanData {
        for _ in 0..200 {
            let root = self
                .spans
                .get_finished_spans()
                .unwrap()
                .into_iter()
                .find(|span| span.name == name && is_root(span));
            if let Some(root) = root {
                return root;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no root span {name}; got {:?}", self.names());
    }

    fn names(&self) -> Vec<String> {
        self.spans
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|span| span.name.into_owned())
            .collect()
    }

    fn logs(&self) -> String {
        String::from_utf8(self.logs.lock().unwrap().clone()).unwrap()
    }

    /// Uploads an object of [`OBJECT_SIZE`] bytes, returning its path.
    async fn upload(&self) -> Result<String, Box<dyn std::error::Error>> {
        let object = vec![7u8; OBJECT_SIZE];
        let oid: String = Sha256::digest(&object)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let path = format!("/api/test/test/object/{oid}");
        let (status, _) = self.request("PUT", &path, &object).await?;
        assert_eq!(status, 200);
        Ok(path)
    }
}

/// The status and body of a raw HTTP/1.1 response.
fn parse(response: &[u8]) -> (u16, Vec<u8>) {
    let end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no end of headers");
    let head = std::str::from_utf8(&response[..end]).unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, response[end + 4..].to_vec())
}

fn is_root(span: &SpanData) -> bool {
    span.parent_span_id == opentelemetry::trace::SpanId::INVALID
}

fn attr(span: &SpanData, key: &str) -> Option<Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key == Key::from(key.to_string()))
        .map(|kv| kv.value.clone())
}

fn duration(span: &SpanData) -> Duration {
    span.end_time.duration_since(span.start_time).unwrap()
}

/// The request's log line, found by its trace id.
fn line_for(logs: &str, span: &SpanData) -> String {
    let trace_id = span.span_context.trace_id().to_string();
    logs.lines()
        .find(|line| line.contains(&format!("trace_id=\"{trace_id}\"")))
        .unwrap_or_else(|| panic!("no log line for {trace_id} in:\n{logs}"))
        .to_string()
}

/// A download's span is the root of its trace, says what the request was,
/// and lasts until the body has been sent, not just until the headers were.
#[tokio::test]
async fn request_spans_cover_the_whole_response()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;
    let path = server.upload().await?;

    let upload = server.root("PUT /api/{org}/{project}/object/{oid}").await;
    assert_eq!(
        attr(&upload, "http.request.body.size"),
        Some(Value::I64(OBJECT_SIZE as i64))
    );

    // Read the response slowly, so that sending it takes a while.
    let mut stream = server.send("GET", &path, b"").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = parse(&response);
    assert_eq!((status, body.len()), (200, OBJECT_SIZE));

    let root = server.root("GET /api/{org}/{project}/object/{oid}").await;
    assert_eq!(root.span_kind, SpanKind::Server);
    assert_eq!(root.status, Status::Unset);
    assert!(
        duration(&root) >= Duration::from_millis(300),
        "the span ended after {:?}, before the body was sent",
        duration(&root)
    );
    for (key, value) in [
        ("http.request.method", Value::from("GET")),
        ("http.route", "/api/{org}/{project}/object/{oid}".into()),
        ("url.path", path.clone().into()),
        ("http.response.status_code", Value::I64(200)),
        ("http.response.body.size", Value::I64(OBJECT_SIZE as i64)),
        ("lfs.repo", "test/test".into()),
        ("enduser.id", "testuser1".into()),
        ("client.address", "203.0.113.7".into()),
        ("user_agent.original", "git-lfs/test".into()),
    ] {
        assert_eq!(attr(&root, key), Some(value), "{key}");
    }

    // The handler's span is in the same trace, under the request's.
    let spans = server.spans.get_finished_spans()?;
    let download = spans
        .iter()
        .find(|span| span.name == "download")
        .expect("no download span");
    assert_eq!(
        download.span_context.trace_id(),
        root.span_context.trace_id()
    );
    assert_eq!(download.parent_span_id, root.span_context.span_id());

    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    for field in [
        "client=\"203.0.113.7\"",
        "user=\"testuser1\"",
        "method=GET",
        "status=200",
        &format!("sent={OBJECT_SIZE}"),
        "repo=\"test/test\"",
        "outcome=\"completed\"",
    ] {
        assert!(line.contains(field), "no {field} in {line}");
    }
    Ok(())
}

/// A download the client walks away from is an error on its span. git-lfs
/// cancels transfers routinely, so it is only logged at info.
#[tokio::test]
async fn aborts_are_marked() -> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;
    let path = server.upload().await?;

    let mut stream = server.send("GET", &path, b"").await?;
    let mut first = [0u8; 1024];
    let _ = stream.read(&mut first).await?;
    drop(stream);

    let root = server.root("GET /api/{org}/{project}/object/{oid}").await;
    assert!(
        matches!(root.status, Status::Error { .. }),
        "{:?}",
        root.status
    );
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("outcome=\"aborted\""), "{line}");

    // Nor does hyper log the connection's end as an error.
    server.closed(2).await;
    assert!(!server.logs().contains(" ERROR "), "{}", server.logs());
    Ok(())
}

/// A 5xx response marks the request's span as an error.
#[tokio::test]
async fn server_errors_are_errors() -> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;

    // Releasing a lock that doesn't exist is a 500.
    let (status, _) = server
        .request("POST", "/api/test/test/locks/nonexistent/unlock", b"{}")
        .await?;
    assert_eq!(status, 500);

    let root = server
        .root("POST /api/{org}/{project}/locks/{id}/unlock")
        .await;
    assert!(
        matches!(root.status, Status::Error { .. }),
        "{:?}",
        root.status
    );
    assert_eq!(
        attr(&root, "http.response.status_code"),
        Some(Value::I64(500))
    );
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" WARN "), "{line}");
    Ok(())
}

/// Health checks are most requests, and would be most traces: they get none.
#[tokio::test]
async fn health_checks_have_no_spans() -> Result<(), Box<dyn std::error::Error>>
{
    let server = observed().await?;

    let (status, _) = server.request("GET", "/", b"").await?;
    assert_eq!(status, 200);
    // Anything else still gets a span, so this waits for the server.
    let (status, _) = server.request("GET", "/nowhere", b"").await?;
    assert_eq!(status, 404);
    server.root("GET other").await;

    // Only that request, and the server starting.
    assert_eq!(server.names(), ["startup", "GET other"]);
    assert!(!server.logs().contains("path=/ "), "{}", server.logs());
    Ok(())
}

/// A batch's span and log line say what it asked for.
#[tokio::test]
async fn batches_say_what_they_asked_for()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;
    let path = server.upload().await?;
    let oid = path.rsplit('/').next().unwrap();
    let missing = "0".repeat(64);

    let batch = format!(
        r#"{{"operation": "download", "objects": [
            {{"oid": "{oid}", "size": {OBJECT_SIZE}}},
            {{"oid": "{missing}", "size": 1}}
        ]}}"#
    );
    let (status, _) = server
        .request("POST", "/api/test/test/objects/batch", batch.as_bytes())
        .await?;
    assert_eq!(status, 200);

    let root = server.root("POST /api/{org}/{project}/objects/batch").await;
    assert_eq!(attr(&root, "lfs.operation"), Some("download".into()));
    assert_eq!(attr(&root, "lfs.objects"), Some(Value::I64(2)));

    let spans = server.spans.get_finished_spans()?;
    let handler = spans
        .iter()
        .find(|span| span.name == "batch")
        .expect("no batch span");
    for (key, value) in [
        ("lfs.operation", Value::from("download")),
        ("lfs.objects", Value::I64(2)),
        ("lfs.objects.presigned", Value::I64(0)),
        ("lfs.objects.missing", Value::I64(1)),
    ] {
        assert_eq!(attr(handler, key), Some(value), "{key}");
    }

    let line = line_for(&server.logs(), &root);
    for field in ["op=\"download\"", "objects=2", "presigned=0", "missing=1"] {
        assert!(line.contains(field), "no {field} in {line}");
    }
    Ok(())
}

/// hyper sends no body for a HEAD request, which is not the client going
/// away.
#[tokio::test]
async fn head_requests_are_complete() -> Result<(), Box<dyn std::error::Error>>
{
    let server = observed().await?;

    let (status, _) = server.request("HEAD", "/nowhere", b"").await?;
    assert_eq!(status, 404);

    let root = server.root("HEAD other").await;
    assert_eq!(root.status, Status::Unset);
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("outcome=\"completed\""), "{line}");
    Ok(())
}

/// Only the last `X-Forwarded-For` entry, the load balancer's, is the
/// client's address, and whatever it holds stays in the log line's `client`
/// field.
#[tokio::test]
async fn forwarded_for_cannot_forge_log_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;

    let forged = "10.0.0.1, 203.0.113.7 status=500 user=\"admin\"";
    let headers = format!("X-Forwarded-For: {forged}\r\nContent-Length: 0\r\n");
    let mut response = Vec::new();
    server
        .send_head("GET", "/nowhere", &headers)
        .await?
        .read_to_end(&mut response)
        .await?;

    let root = server.root("GET other").await;
    assert_eq!(
        attr(&root, "client.address"),
        Some("203.0.113.7 status=500 user=\"admin\"".into())
    );
    let line = line_for(&server.logs(), &root);
    assert!(
        line.contains(
            r#"client="203.0.113.7 status=500 user=\"admin\"" user="-""#
        ),
        "{line}"
    );
    assert!(line.contains(" status=404 "), "{line}");
    Ok(())
}

/// A client that goes away before there is a response, here while the
/// server is still asking GitHub about it, still gets its request logged,
/// and its span is an error.
#[tokio::test]
async fn requests_abandoned_before_a_response_are_marked()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed_with(Options {
        github_delay: Duration::from_secs(1),
        ..Options::default()
    })
    .await?;

    let stream = server.send("GET", "/api/test/test/locks", b"").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(stream);

    let root = server.root("GET /api/{org}/{project}/locks").await;
    assert!(
        matches!(root.status, Status::Error { .. }),
        "{:?}",
        root.status
    );
    assert_eq!(attr(&root, "http.response.status_code"), None);
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("outcome=\"aborted\""), "{line}");
    assert!(!line.contains(" status="), "{line}");

    server.closed(1).await;
    assert!(!server.logs().contains(" ERROR "), "{}", server.logs());
    Ok(())
}

/// An upload the client stops sending is the client going away, not the
/// server failing. That includes S3 behind a disk cache, which copies the
/// body to both and loses what its error was.
#[tokio::test]
async fn aborted_uploads_are_not_server_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let mut backends = vec![None];
    if let Some(s3) = common::s3_target("aborted S3 uploads").await {
        backends.push(Some(s3));
    }
    for s3 in backends {
        let backend = if s3.is_some() { "S3" } else { "local" };
        let server = observed_with(Options {
            s3,
            ..Options::default()
        })
        .await?;

        let path = format!("/api/test/test/object/{}", "a".repeat(64));
        let headers = "Content-Length: 1000000\r\n";
        let mut stream = server.send_head("PUT", &path, headers).await?;
        stream.write_all(&[7u8; 10]).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stream);

        let root = server.root("PUT /api/{org}/{project}/object/{oid}").await;
        assert!(
            matches!(root.status, Status::Error { .. }),
            "{backend}: {:?}",
            root.status
        );
        let line = line_for(&server.logs(), &root);
        assert!(line.contains(" INFO "), "{backend}: {line}");
        assert!(line.contains("outcome=\"aborted\""), "{backend}: {line}");

        server.closed(1).await;
        let logs = server.logs();
        assert!(!logs.contains(" ERROR "), "{backend}: {logs}");
    }
    Ok(())
}

/// A GitHub that fails the way a client does, by closing the connection, is
/// the server failing: its requests are errors.
#[tokio::test]
async fn github_failures_are_server_errors()
-> Result<(), Box<dyn std::error::Error>> {
    // Accepts connections and closes them without answering.
    let github = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let github_uri = format!("http://{}", github.local_addr()?);
    tokio::spawn(async move {
        while let Ok((connection, _)) = github.accept().await {
            drop(connection);
        }
    });

    let server = observed_with(Options {
        github: Some(github_uri),
        ..Options::default()
    })
    .await?;

    // Answered with a 503 that says only which trace to look at.
    let mut response = Vec::new();
    server
        .send("GET", "/api/test/test/locks", b"")
        .await?
        .read_to_end(&mut response)
        .await?;
    let head = String::from_utf8_lossy(&response).to_lowercase();
    assert!(head.contains("\r\nretry-after: 5\r\n"), "{head}");
    let (status, body) = parse(&response);
    assert_eq!(status, 503);
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body["message"],
        "The server failed to handle the request; try again"
    );

    let root = server.root("GET /api/{org}/{project}/locks").await;
    let trace_id = root.span_context.trace_id().to_string();
    assert_eq!(body["request_id"], trace_id.as_str());
    assert!(
        matches!(root.status, Status::Error { .. }),
        "{:?}",
        root.status
    );
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" ERROR "), "{line}");
    assert!(line.contains(" status=503 "), "{line}");
    // The cause is in the line, not just the outermost error. GitHub's
    // connection is closed or reset, depending on timing.
    assert!(line.contains("SendRequest): connection"), "{line}");
    Ok(())
}

/// A batch request the client stops sending gets no answer, not a 400.
#[tokio::test]
async fn aborted_batches_are_unanswered()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;

    let headers = "Content-Length: 1000\r\n";
    let path = "/api/test/test/objects/batch";
    let mut stream = server.send_head("POST", path, headers).await?;
    stream.write_all(br#"{"operation": "#).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(stream);

    let root = server.root("POST /api/{org}/{project}/objects/batch").await;
    assert_eq!(attr(&root, "http.response.status_code"), None);
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("outcome=\"aborted\""), "{line}");
    assert!(!line.contains(" status="), "{line}");
    Ok(())
}

/// A request whose JSON doesn't parse is answered with a 400 saying why, not
/// dropped as a server error.
#[tokio::test]
async fn malformed_json_is_a_bad_request()
-> Result<(), Box<dyn std::error::Error>> {
    let server = observed().await?;

    for path in ["/api/test/test/locks", "/api/test/test/objects/batch"] {
        let (status, body) = server.request("POST", path, b"{not json").await?;
        assert_eq!(status, 400, "{path}");
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        let message = body["message"].as_str().unwrap_or_default();
        assert!(message.contains("key must be a string"), "{path}: {body}");
    }

    let root = server.root("POST /api/{org}/{project}/locks").await;
    assert_eq!(root.status, Status::Unset);
    let line = line_for(&server.logs(), &root);
    assert!(line.contains(" INFO "), "{line}");
    assert!(line.contains("key must be a string"), "{line}");
    Ok(())
}

/// An upload that doesn't match its OID is the client's mistake: a 400 that
/// says so, and nothing stored. That includes S3 behind a disk cache.
#[tokio::test]
async fn mismatched_uploads_are_bad_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let mut backends = vec![None];
    if let Some(s3) = common::s3_target("mismatched S3 uploads").await {
        backends.push(Some(s3));
    }
    for s3 in backends {
        let backend = if s3.is_some() { "S3" } else { "local" };
        let server = observed_with(Options {
            s3,
            ..Options::default()
        })
        .await?;

        let path = format!("/api/test/test/object/{}", "b".repeat(64));
        let (status, body) = server.request("PUT", &path, &[7u8; 1000]).await?;
        assert_eq!(status, 400, "{backend}");
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        let message = body["message"].as_str().unwrap_or_default();
        assert!(message.contains("expected SHA256"), "{backend}: {body}");

        let root = server.root("PUT /api/{org}/{project}/object/{oid}").await;
        let line = line_for(&server.logs(), &root);
        assert!(line.contains(" INFO "), "{backend}: {line}");
        assert!(!server.logs().contains(" ERROR "), "{backend}");

        let (status, _) = server.request("GET", &path, b"").await?;
        assert_eq!(status, 404, "{backend}: the object was stored");
    }
    Ok(())
}
