//! Process-wide counters for the periodic stats line and the OTLP metrics.
//!
//! The counters are global rather than passed through every storage backend
//! and middleware: a process runs one server, and both consumers want totals
//! for the process. Counters only ever increase; consumers take a
//! [`Snapshot`] and subtract the previous one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http::{Method, StatusCode};

/// The counters. Use [`STATS`].
#[derive(Debug)]
pub struct Stats {
    requests: [AtomicU64; RequestClass::ALL.len()],
    client_errors: AtomicU64,
    server_errors: AtomicU64,

    bytes_uploaded: AtomicU64,
    bytes_downloaded: AtomicU64,
    presigned_uploads: AtomicU64,
    presigned_downloads: AtomicU64,

    disk_cache_hits: AtomicU64,
    disk_cache_misses: AtomicU64,
    disk_cache_bytes: AtomicU64,
    disk_cache_limit: AtomicU64,

    s3_size_cache_hits: AtomicU64,
    s3_size_cache_misses: AtomicU64,

    github_auth_cache_hits: AtomicU64,
    github_api_calls: AtomicU64,
}

pub static STATS: Stats = Stats::new();

/// What a request was for, from its method and path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    Batch,
    Download,
    Upload,
    Verify,
    Locks,
    Health,
    Other,
}

impl RequestClass {
    pub const ALL: [RequestClass; 7] = [
        RequestClass::Batch,
        RequestClass::Download,
        RequestClass::Upload,
        RequestClass::Verify,
        RequestClass::Locks,
        RequestClass::Health,
        RequestClass::Other,
    ];

    /// Classifies a request to `/api/{org}/{project}/...` by what follows the
    /// project, or `/` as the health check.
    pub fn of(method: &Method, path: &str) -> Self {
        if path == "/" {
            return RequestClass::Health;
        }

        let mut parts = path.split('/').filter(|part| !part.is_empty());
        if parts.next() != Some("api") {
            return RequestClass::Other;
        }
        // Skip the org and project.
        let mut parts = parts.skip(2);

        match (parts.next(), parts.next(), method) {
            (Some("objects"), Some("batch"), _) => RequestClass::Batch,
            (Some("objects"), Some("verify"), _) => RequestClass::Verify,
            (Some("object"), Some(_), &Method::GET) => RequestClass::Download,
            (Some("object"), Some(_), &Method::PUT) => RequestClass::Upload,
            (Some("locks"), _, _) => RequestClass::Locks,
            _ => RequestClass::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RequestClass::Batch => "batch",
            RequestClass::Download => "download",
            RequestClass::Upload => "upload",
            RequestClass::Verify => "verify",
            RequestClass::Locks => "locks",
            RequestClass::Health => "health",
            RequestClass::Other => "other",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

impl Stats {
    const fn new() -> Self {
        Stats {
            requests: [const { AtomicU64::new(0) }; RequestClass::ALL.len()],
            client_errors: AtomicU64::new(0),
            server_errors: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
            presigned_uploads: AtomicU64::new(0),
            presigned_downloads: AtomicU64::new(0),
            disk_cache_hits: AtomicU64::new(0),
            disk_cache_misses: AtomicU64::new(0),
            disk_cache_bytes: AtomicU64::new(0),
            disk_cache_limit: AtomicU64::new(0),
            s3_size_cache_hits: AtomicU64::new(0),
            s3_size_cache_misses: AtomicU64::new(0),
            github_auth_cache_hits: AtomicU64::new(0),
            github_api_calls: AtomicU64::new(0),
        }
    }

    pub fn request(&self, class: RequestClass, status: StatusCode) {
        add(&self.requests[class.index()], 1);
        if status.is_client_error() {
            add(&self.client_errors, 1);
        } else if status.is_server_error() {
            add(&self.server_errors, 1);
        }
    }

    /// A request that failed on the server: its service errored, or its
    /// response body failed partway.
    pub fn failed_request(&self, class: RequestClass) {
        add(&self.requests[class.index()], 1);
        add(&self.server_errors, 1);
    }

    /// A request the client gave up on before it got a response.
    pub fn unanswered_request(&self, class: RequestClass) {
        add(&self.requests[class.index()], 1);
    }

    pub fn uploaded(&self, bytes: u64) {
        add(&self.bytes_uploaded, bytes);
    }

    pub fn downloaded(&self, bytes: u64) {
        add(&self.bytes_downloaded, bytes);
    }

    pub fn presigned_upload(&self) {
        add(&self.presigned_uploads, 1);
    }

    pub fn presigned_download(&self) {
        add(&self.presigned_downloads, 1);
    }

    pub fn disk_cache_hit(&self) {
        add(&self.disk_cache_hits, 1);
    }

    pub fn disk_cache_miss(&self) {
        add(&self.disk_cache_misses, 1);
    }

    /// Records the disk cache's current size and its limit (0 = unlimited).
    pub fn disk_cache_size(&self, bytes: u64, limit: u64) {
        self.disk_cache_bytes.store(bytes, Ordering::Relaxed);
        self.disk_cache_limit.store(limit, Ordering::Relaxed);
    }

    pub fn s3_size_cache_hit(&self) {
        add(&self.s3_size_cache_hits, 1);
    }

    pub fn s3_size_cache_miss(&self) {
        add(&self.s3_size_cache_misses, 1);
    }

    pub fn github_auth_cache_hit(&self) {
        add(&self.github_auth_cache_hits, 1);
    }

    pub fn github_api_call(&self) {
        add(&self.github_api_calls, 1);
    }

    pub fn snapshot(&self) -> Snapshot {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Snapshot {
            requests: self.requests.each_ref().map(get),
            client_errors: get(&self.client_errors),
            server_errors: get(&self.server_errors),
            bytes_uploaded: get(&self.bytes_uploaded),
            bytes_downloaded: get(&self.bytes_downloaded),
            presigned_uploads: get(&self.presigned_uploads),
            presigned_downloads: get(&self.presigned_downloads),
            disk_cache_hits: get(&self.disk_cache_hits),
            disk_cache_misses: get(&self.disk_cache_misses),
            disk_cache_bytes: get(&self.disk_cache_bytes),
            disk_cache_limit: get(&self.disk_cache_limit),
            s3_size_cache_hits: get(&self.s3_size_cache_hits),
            s3_size_cache_misses: get(&self.s3_size_cache_misses),
            github_auth_cache_hits: get(&self.github_auth_cache_hits),
            github_api_calls: get(&self.github_api_calls),
        }
    }
}

/// The counters' values at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub requests: [u64; RequestClass::ALL.len()],
    pub client_errors: u64,
    pub server_errors: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub presigned_uploads: u64,
    pub presigned_downloads: u64,
    pub disk_cache_hits: u64,
    pub disk_cache_misses: u64,
    /// A gauge, not a counter.
    pub disk_cache_bytes: u64,
    /// A gauge, not a counter. 0 means unlimited.
    pub disk_cache_limit: u64,
    pub s3_size_cache_hits: u64,
    pub s3_size_cache_misses: u64,
    pub github_auth_cache_hits: u64,
    pub github_api_calls: u64,
}

impl Snapshot {
    pub fn requests(&self, class: RequestClass) -> u64 {
        self.requests[class.index()]
    }

    /// What happened between `earlier` and this snapshot. Gauges keep their
    /// current value.
    pub fn since(&self, earlier: &Snapshot) -> Snapshot {
        let d = |now: u64, then: u64| now.saturating_sub(then);
        Snapshot {
            requests: std::array::from_fn(|i| {
                d(self.requests[i], earlier.requests[i])
            }),
            client_errors: d(self.client_errors, earlier.client_errors),
            server_errors: d(self.server_errors, earlier.server_errors),
            bytes_uploaded: d(self.bytes_uploaded, earlier.bytes_uploaded),
            bytes_downloaded: d(
                self.bytes_downloaded,
                earlier.bytes_downloaded,
            ),
            presigned_uploads: d(
                self.presigned_uploads,
                earlier.presigned_uploads,
            ),
            presigned_downloads: d(
                self.presigned_downloads,
                earlier.presigned_downloads,
            ),
            disk_cache_hits: d(self.disk_cache_hits, earlier.disk_cache_hits),
            disk_cache_misses: d(
                self.disk_cache_misses,
                earlier.disk_cache_misses,
            ),
            disk_cache_bytes: self.disk_cache_bytes,
            disk_cache_limit: self.disk_cache_limit,
            s3_size_cache_hits: d(
                self.s3_size_cache_hits,
                earlier.s3_size_cache_hits,
            ),
            s3_size_cache_misses: d(
                self.s3_size_cache_misses,
                earlier.s3_size_cache_misses,
            ),
            github_auth_cache_hits: d(
                self.github_auth_cache_hits,
                earlier.github_auth_cache_hits,
            ),
            github_api_calls: d(
                self.github_api_calls,
                earlier.github_api_calls,
            ),
        }
    }

    /// Logs this interval's activity at `info`, as one line of `key=value`
    /// fields.
    pub fn log(&self, interval: Duration) {
        let bytes = |n: u64| humansize::format_size(n, humansize::BINARY);
        let rate = |hits: u64, misses: u64| match hits + misses {
            0 => "-".to_string(),
            total => format!("{:.1}%", hits as f64 * 100.0 / total as f64),
        };
        let limit = match self.disk_cache_limit {
            0 => "unlimited".to_string(),
            limit => bytes(limit),
        };

        tracing::info!(
            interval = %humantime::format_duration(interval),
            // Health checks are left out: they would be most of the total.
            requests = RequestClass::ALL
                .iter()
                .filter(|&&class| class != RequestClass::Health)
                .map(|&class| self.requests(class))
                .sum::<u64>(),
            batch = self.requests(RequestClass::Batch),
            download = self.requests(RequestClass::Download),
            upload = self.requests(RequestClass::Upload),
            verify = self.requests(RequestClass::Verify),
            locks = self.requests(RequestClass::Locks),
            client_errors = self.client_errors,
            server_errors = self.server_errors,
            uploaded = %bytes(self.bytes_uploaded),
            downloaded = %bytes(self.bytes_downloaded),
            presigned_uploads = self.presigned_uploads,
            presigned_downloads = self.presigned_downloads,
            disk_cache_hit_rate = %rate(self.disk_cache_hits, self.disk_cache_misses),
            disk_cache_size = %bytes(self.disk_cache_bytes),
            disk_cache_limit = %limit,
            s3_size_cache_hit_rate = %rate(self.s3_size_cache_hits, self.s3_size_cache_misses),
            github_auth_cache_hits = self.github_auth_cache_hits,
            github_api_calls = self.github_api_calls,
            "stats"
        );
    }
}

/// Logs a stats line every `interval` until the process exits.
pub async fn log_every(interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    // The first tick completes immediately.
    ticker.tick().await;

    let mut previous = STATS.snapshot();
    loop {
        ticker.tick().await;
        let current = STATS.snapshot();
        current.since(&previous).log(interval);
        previous = current;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_requests() {
        let class = |method: Method, path| RequestClass::of(&method, path);
        let oid = "/api/org/project/object/abcd";

        assert_eq!(class(Method::GET, "/"), RequestClass::Health);
        assert_eq!(
            class(Method::POST, "/api/org/project/objects/batch"),
            RequestClass::Batch
        );
        assert_eq!(
            class(Method::POST, "/api/org/project/objects/verify"),
            RequestClass::Verify
        );
        assert_eq!(class(Method::GET, oid), RequestClass::Download);
        assert_eq!(class(Method::PUT, oid), RequestClass::Upload);
        assert_eq!(class(Method::DELETE, oid), RequestClass::Other);
        assert_eq!(
            class(Method::GET, "/api/org/project/locks"),
            RequestClass::Locks
        );
        assert_eq!(
            class(Method::POST, "/api/org/project/locks/verify"),
            RequestClass::Locks
        );
        assert_eq!(class(Method::GET, "/favicon.ico"), RequestClass::Other);
        assert_eq!(class(Method::GET, "/api/org"), RequestClass::Other);
    }

    #[test]
    fn counts_requests_and_errors() {
        let stats = Stats::new();
        stats.request(RequestClass::Batch, StatusCode::OK);
        stats.request(RequestClass::Batch, StatusCode::UNAUTHORIZED);
        stats.request(RequestClass::Download, StatusCode::BAD_GATEWAY);
        stats.failed_request(RequestClass::Upload);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.requests(RequestClass::Batch), 2);
        assert_eq!(snapshot.requests(RequestClass::Download), 1);
        assert_eq!(snapshot.requests(RequestClass::Upload), 1);
        assert_eq!(snapshot.client_errors, 1);
        assert_eq!(snapshot.server_errors, 2);
    }

    #[test]
    fn deltas_subtract_counters_but_keep_gauges() {
        let stats = Stats::new();
        stats.uploaded(100);
        stats.disk_cache_hit();
        stats.disk_cache_size(1000, 5000);
        let earlier = stats.snapshot();

        stats.uploaded(50);
        stats.disk_cache_miss();
        stats.disk_cache_size(800, 5000);
        let delta = stats.snapshot().since(&earlier);

        assert_eq!(delta.bytes_uploaded, 50);
        assert_eq!(delta.disk_cache_hits, 0);
        assert_eq!(delta.disk_cache_misses, 1);
        assert_eq!(delta.disk_cache_bytes, 800);
        assert_eq!(delta.disk_cache_limit, 5000);
    }

    /// Captures what `Snapshot::log` writes.
    fn logged(snapshot: &Snapshot) -> String {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Buffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            snapshot.log(Duration::from_secs(3600))
        });

        let bytes = buffer.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn logs_one_line_of_fields() {
        let stats = Stats::new();
        stats.request(RequestClass::Batch, StatusCode::OK);
        stats.request(RequestClass::Download, StatusCode::NOT_FOUND);
        stats.request(RequestClass::Health, StatusCode::OK);
        stats.downloaded(3 * 1024 * 1024);
        stats.disk_cache_hit();
        stats.disk_cache_hit();
        stats.disk_cache_hit();
        stats.disk_cache_miss();
        stats.disk_cache_size(1024, 0);

        let line = logged(&stats.snapshot());

        assert_eq!(line.lines().count(), 1, "{line}");
        for field in [
            "INFO",
            "stats",
            "interval=1h",
            "requests=2",
            "batch=1",
            "download=1",
            "client_errors=1",
            "server_errors=0",
            "downloaded=3 MiB",
            "disk_cache_hit_rate=75.0%",
            "disk_cache_size=1 KiB",
            "disk_cache_limit=unlimited",
            "s3_size_cache_hit_rate=-",
        ] {
            assert!(line.contains(field), "missing {field:?} in {line}");
        }
    }
}
