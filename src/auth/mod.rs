use core::task::{Context, Poll};
use futures::future::BoxFuture;
use std::{sync::Arc, time::Instant};

use crate::storage::Namespace;
use crate::util::{empty, full};
use crate::{app::BoxBody, error::Error};

use http::{self, header, HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::{
    self,
    body::{Buf, Incoming},
    Request, Response,
};
use hyper_tls::HttpsConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tower::Service;

use base64::{engine::general_purpose, Engine as _};
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(feature = "otel")]
use tracing::instrument;
use tracing::{event, Level};

type GithubAuthCache = Arc<RwLock<LinkedHashMap<String, AuthCacheEntry>>>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthCacheEntry {
    data: UserRepoInfo,
    timestamp: Instant,
}

impl AuthCacheEntry {
    pub fn new(data: UserRepoInfo) -> Self {
        AuthCacheEntry {
            data,
            timestamp: Instant::now(),
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
}

#[derive(
    Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, Default,
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

#[derive(Debug, Clone)]
pub struct Auth<S> {
    cache: GithubAuthCache,
    service: S,
    authenticated: bool,
    server: String,
}

impl<S> Auth<S> {
    pub fn new(
        service: S,
        cache: GithubAuthCache,
        authenticated: bool,
        server: Option<String>,
    ) -> Self {
        let server = server.unwrap_or("https://api.github.com".to_string());

        Auth {
            cache,
            service,
            authenticated,
            server,
        }
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip_all, ret)
    )]
    async fn authorize(
        headers: &HeaderMap,
        namespace: &Namespace,
        github_auth_cache: GithubAuthCache,
        server: &str,
    ) -> Result<Option<UserRepoInfo>, Error> {
        if let Some(auth) = headers.get(header::AUTHORIZATION) {
            // check cache
            let key = Self::encode_auth_key(auth);

            if let Some(entry) = github_auth_cache.read().get(&key) {
                event!(
                    Level::DEBUG,
                    message = "cache hit",
                    key = key,
                    age = entry.timestamp.elapsed().as_secs()
                );

                // todo: make this TTL configurable
                if entry.timestamp.elapsed().as_secs() < 300 {
                    return Ok(Some(entry.data.clone()));
                }
            }

            let client = Client::builder(TokioExecutor::new())
                .build(HttpsConnector::new());

            let url = format!(
                "{}/repos/{}/{}",
                server,
                namespace.org(),
                namespace.project()
            );

            let req = Request::get(url.clone())
                .header(header::ACCEPT, "application/vnd.github+json")
                .header(header::AUTHORIZATION, auth)
                .header(header::USER_AGENT, "rudolfs")
                .body(empty())?;

            let res = client.request(req).await?;

            event!(Level::INFO, status = ?res.status(), url = %url);

            if res.status() == StatusCode::OK {
                let body = res.collect().await?.aggregate();
                let repository: Repository =
                    serde_json::from_reader(body.reader())?;

                let username = Self::get_github_username(auth, server)
                    .await
                    .unwrap_or(None);

                let user_info = UserRepoInfo {
                    permissions: repository.permissions,
                    username,
                };

                let mut cache = github_auth_cache.write();

                cache.insert(
                    Self::encode_auth_key(auth),
                    AuthCacheEntry::new(user_info.clone()),
                );

                return Ok(Some(user_info));
            }
        }

        Ok(None)
    }

    #[cfg_attr(
        feature = "otel",
        tracing::instrument(level = "info", skip_all, ret)
    )]
    async fn get_github_username(
        auth: &HeaderValue,
        server: &str,
    ) -> Result<Option<String>, Error> {
        let client =
            Client::builder(TokioExecutor::new()).build(HttpsConnector::new());

        let req = Request::get(format!("{}/user", server))
            .header(header::ACCEPT, "application/vnd.github+json")
            .header(header::AUTHORIZATION, auth)
            .header(header::USER_AGENT, "rudolfs")
            .body(empty())?;

        let res = client.request(req).await?;

        if res.status() == StatusCode::OK {
            let body = res.collect().await?.aggregate();
            let user_info: UserResp = serde_json::from_reader(body.reader())?;

            if let Some(username) = user_info.login {
                return Ok(Some(username));
            }
        }

        Ok(None)
    }

    fn encode_auth_key(input: &HeaderValue) -> String {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
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

    #[cfg_attr(feature = "otel", instrument(level = "debug", skip_all))]
    fn call(&mut self, mut req: Req) -> Self::Future {
        event!(Level::DEBUG, path = ?req.uri().path());

        if (!self.authenticated) || (!req.uri().path().starts_with("/api/")) {
            return Box::pin(self.service.call(req));
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
                        .body(full("Missing org/project in URL"))?)
                }
            };

            let user =
                Self::authorize(req.headers(), &namespace, cache, &server)
                    .await?;

            // All endpoints require authentication, so return early
            // if we have no user or permissions.
            #[allow(clippy::unnecessary_unwrap)]
            if user.is_none() || user.as_ref().unwrap().permissions.is_none() {
                Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("Lfs-Authenticate", "Basic realm=\"GitHub\"")
                    .body(empty())?)
            } else {
                let ext = req.extensions_mut();
                ext.insert(user.unwrap());
                service.call(req).await
            }
        };
        Box::pin(auth_fut)
    }
}
