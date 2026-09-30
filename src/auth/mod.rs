use core::task::{Context, Poll};
use futures::future::BoxFuture;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::stats::STATS;
use crate::storage::Namespace;
use crate::util::{empty, full};
use crate::{
    app::BoxBody,
    error::{Error, RateLimited},
};
use bytes::Bytes;

use http::{self, HeaderMap, HeaderValue, StatusCode, header};
use http_body_util::{BodyExt, LengthLimitError};
use hyper::{self, Request, Response, body::Incoming};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use tower::Service;

use base64::{Engine as _, engine::general_purpose};
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use tracing::{Level, event};

type GithubAuthCache = Arc<RwLock<LinkedHashMap<String, AuthCacheEntry>>>;

/// What GitHub answered about a client's access to a repo.
#[derive(Debug, Clone, Eq, PartialEq)]
enum CachedAnswer {
    Access(UserRepoInfo),
    /// GitHub was rate limiting the credentials, and asked to be left alone
    /// until then.
    RateLimited {
        until: Instant,
        message: String,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthCacheEntry {
    answer: CachedAnswer,
    timestamp: Instant,
    ttl: Duration,
}

/// How long GitHub's answer about a client is used for.
const AUTH_CACHE_TTL: Duration = Duration::from_secs(300);

/// How many answers are cached, the oldest going first.
const AUTH_CACHE_ENTRIES: usize = 10_000;

/// How long it is used for when GitHub failed to give the username: long
/// enough not to ask GitHub on every request while it fails, short enough
/// for locking to work again soon after it recovers.
const AUTH_CACHE_TTL_WITHOUT_USERNAME: Duration = Duration::from_secs(30);

/// The longest a rate limit is answered from the cache, rather than asking
/// GitHub again: GitHub may ban an integration that keeps asking while
/// limited.
const AUTH_CACHE_TTL_RATE_LIMITED: Duration = Duration::from_secs(60);

impl AuthCacheEntry {
    pub fn new(data: UserRepoInfo) -> Self {
        let ttl = match &data.username_error {
            // No longer than GitHub asked to wait, if it rate limited.
            Some(err) => {
                err.retry_at.map_or(AUTH_CACHE_TTL_WITHOUT_USERNAME, |at| {
                    at.saturating_duration_since(Instant::now())
                        .min(AUTH_CACHE_TTL_WITHOUT_USERNAME)
                })
            }
            None => AUTH_CACHE_TTL,
        };
        AuthCacheEntry {
            answer: CachedAnswer::Access(data),
            timestamp: Instant::now(),
            ttl,
        }
    }

    /// GitHub rate limiting, as `limited` says.
    fn rate_limited(limited: &RateLimited) -> Self {
        AuthCacheEntry {
            answer: CachedAnswer::RateLimited {
                until: Instant::now() + limited.retry_after,
                message: limited.message.clone(),
            },
            timestamp: Instant::now(),
            ttl: limited.retry_after.min(AUTH_CACHE_TTL_RATE_LIMITED),
        }
    }

    /// The answer, as an answer to a request now.
    fn answer(&self) -> Result<Option<UserRepoInfo>, Error> {
        match &self.answer {
            CachedAnswer::Access(data) => Ok(Some(data.clone())),
            CachedAnswer::RateLimited { until, message } => Err(RateLimited {
                retry_after: until.saturating_duration_since(Instant::now()),
                message: message.clone(),
            }
            .into()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct Repository {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Permissions>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct UserResp {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login: Option<String>,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserRepoInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Permissions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Why the username couldn't be looked up, when GitHub failed to say.
    /// Only locking (taking, releasing and verifying locks) needs it, so other
    /// requests go ahead without it.
    #[serde(skip)]
    pub username_error: Option<UsernameError>,
}

/// Why GitHub didn't give a username.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UsernameError {
    pub message: String,
    /// Until when GitHub asked to be left alone, if it rate limited.
    pub retry_at: Option<Instant>,
}

impl UsernameError {
    /// The error for a request that needs the username.
    pub fn to_error(&self) -> Error {
        let message =
            format!("looking up the GitHub user failed: {}", self.message);
        match self.retry_at {
            // What is left of GitHub's wait, as this may be cached.
            Some(retry_at) => RateLimited {
                retry_after: retry_at.saturating_duration_since(Instant::now()),
                message,
            }
            .into(),
            None => anyhow::anyhow!(message),
        }
    }
}

#[derive(
    Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize, Default,
)]
pub struct Permissions {
    #[serde(default)]
    pub admin: bool,
    pub push: bool,
    pub pull: bool,
    #[serde(default)]
    pub triage: bool,
    #[serde(default)]
    pub maintain: bool,
}

/// The client for the GitHub API. Clones share one connection pool, so create
/// it once with [`github_client`] and clone it.
pub type GithubClient = Client<HttpsConnector<HttpConnector>, BoxBody>;

/// How many times a GitHub API call is retried when GitHub can't be reached,
/// takes too long or fails, and how long to wait before the first retry
/// (doubling after). git-lfs doesn't retry lock requests at all, so a GitHub
/// hiccup is only survived if the server retries.
const GITHUB_RETRIES: u32 = 2;
const GITHUB_RETRY_DELAY: Duration = Duration::from_millis(250);

/// How long one GitHub API call may take, body included.
const GITHUB_TIMEOUT: Duration = Duration::from_secs(10);

/// How long asking GitHub about a request may take in all, every call and
/// retry included, so that it stays well within a load balancer's idle
/// timeout (60s by default on AWS).
const GITHUB_DEADLINE: Duration = Duration::from_secs(30);

/// How long GitHub asks to be left alone when it rate limits without saying
/// for how long.
const GITHUB_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

/// The longest wait passed on from GitHub: git-lfs gives up on anything over
/// five minutes anyway, and a nonsense value mustn't reach it.
const GITHUB_RATE_LIMIT_WAIT_MAX: Duration = Duration::from_secs(60 * 60);

/// The largest GitHub answer read. Its answers here are a few KiB.
const GITHUB_BODY_LIMIT: usize = 1024 * 1024;

/// Caches `entry`, dropping the oldest entries past `limit`: each repo a
/// client asks about is an entry.
fn cache_answer(
    cache: &mut LinkedHashMap<String, AuthCacheEntry>,
    key: String,
    entry: AuthCacheEntry,
    limit: usize,
) {
    cache.insert(key, entry);
    while cache.len() > limit {
        cache.pop_front();
    }
}

/// GitHub answered with more than [`GITHUB_BODY_LIMIT`].
#[derive(Debug)]
struct AnswerTooLarge;

impl std::fmt::Display for AnswerTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GitHub answered with more than {GITHUB_BODY_LIMIT} bytes"
        )
    }
}

impl std::error::Error for AnswerTooLarge {}

/// A GitHub API answer, read in full.
struct GithubResponse {
    status: StatusCode,
    body: Bytes,
}

/// GETs `url` from GitHub's API with the client's `auth`, by `deadline`,
/// retrying while GitHub can't be reached, takes too long or answers with a
/// server error. Failing, or being rate limited, is an error: the server
/// failing, not the client's credentials not granting access.
async fn github_get(
    client: &GithubClient,
    url: &str,
    auth: &HeaderValue,
    deadline: tokio::time::Instant,
) -> Result<GithubResponse, Error> {
    github_get_timed(client, url, auth, GITHUB_TIMEOUT, deadline).await
}

/// [`github_get`], with each call taking at most `timeout`.
async fn github_get_timed(
    client: &GithubClient,
    url: &str,
    auth: &HeaderValue,
    timeout: Duration,
    deadline: tokio::time::Instant,
) -> Result<GithubResponse, Error> {
    use tokio::time::{Instant, sleep_until, timeout_at};

    let mut delay = GITHUB_RETRY_DELAY;
    let mut retries = GITHUB_RETRIES;
    loop {
        let req = Request::get(url)
            .header(header::ACCEPT, "application/vnd.github+json")
            .header(header::AUTHORIZATION, auth)
            .header(header::USER_AGENT, "rudolfs")
            .body(empty())?;

        STATS.github_api_call();
        let call = async {
            let res = client.request(req).await?;
            let (parts, body) = res.into_parts();
            let body = http_body_util::Limited::new(body, GITHUB_BODY_LIMIT)
                .collect()
                .await
                .map_err(|err| match err.is::<LengthLimitError>() {
                    true => Error::from(AnswerTooLarge),
                    false => anyhow::anyhow!(err),
                })?
                .to_bytes();
            Ok::<_, Error>((parts, body))
        };
        let attempt_deadline = deadline.min(Instant::now() + timeout);
        let failure = match timeout_at(attempt_deadline, call).await {
            Ok(Ok((parts, body))) => {
                if let Some(limited) = rate_limit(&parts, &body) {
                    // Retrying makes it worse, and GitHub may ban an
                    // integration that keeps trying.
                    return Err(limited.into());
                }
                if !parts.status.is_server_error() {
                    return Ok(GithubResponse {
                        status: parts.status,
                        body,
                    });
                }
                anyhow::anyhow!("GitHub answered {}", parts.status)
            }
            // It would be as large again.
            Ok(Err(err)) if err.is::<AnswerTooLarge>() => return Err(err),
            Ok(Err(err)) => err.context("GitHub couldn't be reached"),
            Err(_) => anyhow::anyhow!("GitHub didn't answer in time"),
        };
        let retry_at = Instant::now() + delay;
        if retries == 0 || retry_at >= deadline {
            return Err(failure);
        }
        tracing::warn!("GitHub API call failed, retrying: {failure:#}");
        sleep_until(retry_at).await;
        retries -= 1;
        delay *= 2;
    }
}

/// How GitHub said it is rate limiting, if it is: a 429, or a 403 that says
/// so, in its headers or only in its message, as GitHub answers its limits
/// with either.
fn rate_limit(
    parts: &http::response::Parts,
    body: &[u8],
) -> Option<RateLimited> {
    let headers = &parts.headers;
    let header = |name| headers.get(name).and_then(|v| v.to_str().ok());
    let exhausted = header("x-ratelimit-remaining") == Some("0");
    let limited = match parts.status {
        StatusCode::TOO_MANY_REQUESTS => true,
        StatusCode::FORBIDDEN => {
            exhausted || header("retry-after").is_some() || {
                // Older GitHub Enterprise Servers call their secondary
                // limit abuse detection.
                let message = String::from_utf8_lossy(body).to_lowercase();
                message.contains("rate limit")
                    || message.contains("abuse detection")
            }
        }
        _ => false,
    };
    if !limited {
        return None;
    }

    // As GitHub says to: until `retry-after`, or else the limit's reset if
    // it is spent, or else at least a minute.
    let seconds = |value: &str| value.trim().parse::<u64>().ok();
    let retry_after = header("retry-after")
        .and_then(seconds)
        .map(Duration::from_secs)
        .or_else(|| {
            let reset = header("x-ratelimit-reset").and_then(seconds)?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?;
            exhausted.then(|| Duration::from_secs(reset).saturating_sub(now))
        })
        .unwrap_or(GITHUB_RATE_LIMIT_WAIT)
        .min(GITHUB_RATE_LIMIT_WAIT_MAX);
    Some(RateLimited {
        retry_after,
        message: format!(
            "GitHub is rate limiting these credentials ({})",
            parts.status
        ),
    })
}

/// Creates the GitHub API client, trusting the system's root certificates.
pub fn github_client() -> std::io::Result<GithubClient> {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        // Named explicitly: if another crate enabled a second rustls
        // provider, rustls would have no default and this would panic.
        .with_provider_and_native_roots(
            rustls::crypto::aws_lc_rs::default_provider(),
        )?
        // Tests stand in for GitHub with a plain HTTP server.
        .https_or_http()
        .enable_http1()
        .build();
    Ok(Client::builder(TokioExecutor::new()).build(https))
}

#[derive(Debug, Clone)]
pub struct Auth<S> {
    cache: GithubAuthCache,
    service: S,
    /// Set if requests must be authenticated with GitHub.
    client: Option<GithubClient>,
    server: String,
}

impl<S> Auth<S> {
    /// Wraps `service` so that requests are authenticated with GitHub, if
    /// `client` is given. Otherwise requests pass straight through.
    pub fn new(
        service: S,
        cache: GithubAuthCache,
        client: Option<GithubClient>,
        server: Option<String>,
    ) -> Self {
        let server = server.unwrap_or("https://api.github.com".to_string());

        Auth {
            cache,
            service,
            client,
            server,
        }
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip_all, ret(level = "debug"))
    )]
    async fn authorize(
        headers: &HeaderMap,
        namespace: &Namespace,
        github_auth_cache: GithubAuthCache,
        client: &GithubClient,
        server: &str,
    ) -> Result<Option<UserRepoInfo>, Error> {
        if let Some(auth) = headers.get(header::AUTHORIZATION) {
            // check cache
            let key = Self::encode_auth_key(auth, namespace);

            if let Some(entry) = github_auth_cache.read().get(&key) {
                event!(
                    Level::DEBUG,
                    message = "cache hit",
                    key = &key[0..4],
                    age = entry.timestamp.elapsed().as_secs()
                );

                if entry.timestamp.elapsed() < entry.ttl {
                    STATS.github_auth_cache_hit();
                    return entry.answer();
                }
            }

            let url = format!(
                "{}/repos/{}/{}",
                server,
                namespace.org(),
                namespace.project()
            );

            // Everything asked of GitHub about this request, by then.
            let deadline = tokio::time::Instant::now() + GITHUB_DEADLINE;
            let res = match github_get(client, &url, auth, deadline).await {
                Ok(res) => res,
                Err(err) => {
                    // Remembered, so that GitHub isn't asked again until it
                    // said to.
                    let limited =
                        crate::error::find::<RateLimited>(err.as_ref());
                    if let Some(limited) = limited {
                        cache_answer(
                            &mut github_auth_cache.write(),
                            key.clone(),
                            AuthCacheEntry::rate_limited(limited),
                            AUTH_CACHE_ENTRIES,
                        );
                    }
                    return Err(err);
                }
            };

            event!(Level::DEBUG, status = ?res.status, url = %url);

            if res.status == StatusCode::OK {
                let repository: Repository = serde_json::from_slice(&res.body)?;

                // A token that can't read the user has none. If GitHub failed
                // to say, only locking, which needs the
                // username, fails, and the answer is only
                // cached briefly, so that GitHub is asked again
                // soon.
                let username =
                    Self::get_github_username(auth, client, server, deadline);
                let (username, username_error) = match username.await {
                    Ok(username) => (username, None),
                    Err(err) => {
                        tracing::warn!(
                            "Looking up the GitHub user failed: {err:#}"
                        );
                        let retry_at =
                            crate::error::find::<RateLimited>(err.as_ref())
                                .map(|limited| {
                                    Instant::now() + limited.retry_after
                                });
                        let message = format!("{err:#}");
                        (None, Some(UsernameError { message, retry_at }))
                    }
                };

                let user_info = UserRepoInfo {
                    permissions: repository.permissions,
                    username,
                    username_error,
                };

                cache_answer(
                    &mut github_auth_cache.write(),
                    key,
                    AuthCacheEntry::new(user_info.clone()),
                    AUTH_CACHE_ENTRIES,
                );

                return Ok(Some(user_info));
            }
        }

        Ok(None)
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip_all, ret(level = "debug"))
    )]
    async fn get_github_username(
        auth: &HeaderValue,
        client: &GithubClient,
        server: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Option<String>, Error> {
        let url = format!("{server}/user");
        let res = github_get(client, &url, auth, deadline).await?;

        if res.status == StatusCode::OK {
            let user_info: UserResp = serde_json::from_slice(&res.body)?;

            if let Some(username) = user_info.login {
                return Ok(Some(username));
            }
        }

        Ok(None)
    }

    /// The cache key for `auth`'s access to `namespace`: GitHub's answer is
    /// about that repo only.
    fn encode_auth_key(auth: &HeaderValue, namespace: &Namespace) -> String {
        let mut hasher = Sha256::new();
        hasher.update(auth.as_bytes());
        // A header value can't hold a NUL, so none can run into the repo.
        hasher.update([0]);
        hasher.update(namespace.org().as_bytes());
        hasher.update([0]);
        hasher.update(namespace.project().as_bytes());
        let result = hasher.finalize();

        general_purpose::STANDARD.encode(result)
    }
}

type Req = Request<Incoming>;

impl<S> Service<Req> for Auth<S>
where
    S: Service<Req, Response = Response<BoxBody>>
        + Send
        + Sync
        + Clone
        + 'static,
    S::Future: Send + 'static,
    S::Error: From<http::Error> + From<Error> + 'static,
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

    // Called within the request's span; `authorize` has its own.
    fn call(&mut self, mut req: Req) -> Self::Future {
        tracing::debug!("checking auth for {}", &req.uri());

        let client = match &self.client {
            Some(client) if req.uri().path().starts_with("/api/") => {
                client.clone()
            }
            _ => {
                tracing::trace!("skipping auth");
                return Box::pin(self.service.call(req));
            }
        };

        let mut service = self.service.clone();

        let cache = Arc::clone(&self.cache);

        let server = self.server.clone();

        let auth_fut = async move {
            let mut parts =
                req.uri().path().split('/').filter(|s| !s.is_empty());

            // Skip over the '/api' part.
            assert_eq!(parts.next(), Some("api"));

            // Extract the namespace.
            let namespace = match (parts.next(), parts.next()) {
                (Some(org), Some(project)) => {
                    Namespace::new(org.into(), project.into())
                }
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(full("Missing org/project in URL"))?);
                }
            };

            // All endpoints require authentication, so return early
            // if we have no user or permissions.
            let user = match Self::authorize(
                req.headers(),
                &namespace,
                cache,
                &client,
                &server,
            )
            .await?
            {
                Some(user) if user.permissions.is_some() => user,
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .header("Lfs-Authenticate", "Basic realm=\"GitHub\"")
                        .body(empty())?);
                }
            };

            // The request carries the user to the app, and the response
            // carries it back out to the request log.
            req.extensions_mut().insert(user.clone());
            let mut response = service.call(req).await?;
            response.extensions_mut().insert(user);
            Ok(response)
        };

        Box::pin(auth_fut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// A GitHub that accepts connections, sends `head` on each and then
    /// stalls. Returns its URL.
    async fn stalling_github(head: &'static [u8]) -> String {
        stalling_github_counted(head).await.0
    }

    /// [`stalling_github`], and how many connections it has had.
    async fn stalling_github_counted(
        head: &'static [u8],
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let github =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/user", github.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut connection, _)) = github.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                let _ = connection.write_all(head).await;
                held.push(connection);
            }
        });
        (url, connections)
    }

    /// A GitHub that stalls is given up on, retries included, whether it
    /// stalls before answering or partway through its body.
    #[tokio::test]
    async fn a_stalled_github_times_out() {
        let heads: [&'static [u8]; 2] = [
            b"",
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"login\":",
        ];
        for head in heads {
            let url = stalling_github(head).await;
            let client = github_client().unwrap();
            let auth = HeaderValue::from_static("Basic dGVzdA==");
            let start = tokio::time::Instant::now();
            let deadline = start + Duration::from_secs(20);
            let timeout = Duration::from_millis(100);
            // Bounded here too, so that a regression fails rather than hangs.
            let call =
                github_get_timed(&client, &url, &auth, timeout, deadline);
            let err = tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("a stalled GitHub was waited on for ever")
                .err()
                .expect("a stalled GitHub answered");
            assert!(format!("{err:#}").contains("in time"), "{err:#}");
            // Three tries and two backoffs.
            assert!(start.elapsed() < Duration::from_secs(3));
        }
    }

    /// All the asking is done by the deadline, however many retries are left.
    #[tokio::test]
    async fn github_is_given_up_on_by_the_deadline() {
        let url = stalling_github(b"").await;
        let client = github_client().unwrap();
        let auth = HeaderValue::from_static("Basic dGVzdA==");
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_millis(300);
        let timeout = Duration::from_secs(10);
        let result = github_get_timed(&client, &url, &auth, timeout, deadline);
        assert!(result.await.is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    /// A GitHub answer larger than any it gives here is refused, not read into
    /// memory.
    #[tokio::test]
    async fn huge_github_answers_are_refused() {
        let size = 2 * GITHUB_BODY_LIMIT;
        let mut answer =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n\r\n")
                .into_bytes();
        answer.resize(answer.len() + size, b' ');
        let (url, connections) = stalling_github_counted(answer.leak()).await;
        let client = github_client().unwrap();
        let auth = HeaderValue::from_static("Basic dGVzdA==");
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_secs(5);
        let err = github_get(&client, &url, &auth, deadline)
            .await
            .err()
            .expect("a huge answer was read");
        assert!(err.is::<AnswerTooLarge>(), "{err:#}");
        // And not asked for again.
        let asked = connections.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(asked, 1);
    }

    #[test]
    fn the_cache_is_bounded() {
        let mut cache = LinkedHashMap::new();
        let entry = || AuthCacheEntry::new(UserRepoInfo::default());
        for key in ["a", "b", "c"] {
            cache_answer(&mut cache, key.into(), entry(), 2);
        }
        let keys: Vec<_> = cache.keys().cloned().collect();
        assert_eq!(keys, ["b", "c"]);

        // An answer cached again is the newest, not the first to go.
        cache_answer(&mut cache, "b".into(), entry(), 2);
        cache_answer(&mut cache, "d".into(), entry(), 2);
        let keys: Vec<_> = cache.keys().cloned().collect();
        assert_eq!(keys, ["b", "d"]);
    }

    /// A failed username lookup that GitHub rate limited is cached no longer
    /// than GitHub asked to wait.
    #[test]
    fn cached_rate_limits_last_as_long_as_githubs_wait() {
        let data = UserRepoInfo {
            username_error: Some(UsernameError {
                message: "limited".into(),
                retry_at: Some(Instant::now() + Duration::from_secs(5)),
            }),
            ..UserRepoInfo::default()
        };
        assert!(AuthCacheEntry::new(data).ttl <= Duration::from_secs(5));
    }

    /// A cached rate limit asks for what is left of GitHub's wait.
    #[test]
    fn cached_waits_count_down() {
        let error = UsernameError {
            message: "limited".into(),
            retry_at: Some(Instant::now() + Duration::from_secs(42)),
        };
        std::thread::sleep(Duration::from_millis(1100));
        let err = error.to_error();
        let limited = crate::error::find::<RateLimited>(err.as_ref()).unwrap();
        assert!(limited.retry_after < Duration::from_secs(41));
        assert!(limited.retry_after > Duration::from_secs(39));
    }

    fn parts(
        status: u16,
        headers: &[(&'static str, &'static str)],
    ) -> http::response::Parts {
        let mut builder = Response::builder().status(status);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(()).unwrap().into_parts().0
    }

    #[test]
    fn rate_limits_are_recognized() {
        let limited = |status, headers: &[_], body: &str| {
            rate_limit(&parts(status, headers), body.as_bytes())
                .map(|limited| limited.retry_after.as_secs())
        };
        // GitHub's wait is passed on, or a minute if it gave none.
        assert_eq!(limited(429, &[("retry-after", "30")], ""), Some(30));
        assert_eq!(limited(429, &[], ""), Some(60));
        assert_eq!(limited(403, &[("retry-after", "60")], ""), Some(60));
        assert_eq!(
            limited(403, &[("x-ratelimit-remaining", "0")], ""),
            Some(60)
        );
        let secondary =
            r#"{"message": "You have exceeded a secondary rate limit"}"#;
        assert_eq!(limited(403, &[], secondary), Some(60));
        let abuse =
            r#"{"message": "You have triggered an abuse detection mechanism"}"#;
        assert_eq!(limited(403, &[], abuse), Some(60));
        // A nonsense wait is capped.
        assert_eq!(
            limited(429, &[("retry-after", "99999999")], ""),
            Some(3600)
        );

        // A 403 that isn't a rate limit is the credentials not granting access.
        let saml = concat!(
            r#"{"message": "Resource protected by organization "#,
            r#"SAML enforcement."}"#
        );
        assert_eq!(limited(403, &[], saml), None);
        assert_eq!(
            limited(403, &[("x-ratelimit-remaining", "4999")], ""),
            None
        );
        assert_eq!(limited(200, &[("x-ratelimit-remaining", "0")], ""), None);
    }
}
