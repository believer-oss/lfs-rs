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

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn local_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = common::init_logger();
    let startup_span = common::startup();

    let lock_data = tempfile::TempDir::new()?;
    let lock_data_file = lock_data.path().join("locking.json");
    let locks = lfs_rs::LocalLs::new(lock_data_file).await?;

    common::smoke_test(locks, Some(startup_span)).await
}

/// Locks record their owner's GitHub username. If GitHub doesn't return one
/// for the credentials, locking is refused, rather than panicking.
#[tokio::test(flavor = "multi_thread")]
async fn locking_without_a_github_username_is_forbidden()
-> Result<(), Box<dyn std::error::Error>> {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Push access to the repo, but no `GET /user`, so no username.
    let github = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/test/test"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"permissions":{"push":true,"pull":true}}"#,
            "application/json",
        ))
        .mount(&github)
        .await;

    let data = tempfile::TempDir::new()?;
    let lock_data = tempfile::TempDir::new()?;
    let locks =
        lfs_rs::LocalLs::new(lock_data.path().join("locking.json")).await?;
    let mut server = lfs_rs::LocalServerBuilder::new(data.path().into(), None);
    server.authenticated(true);
    server.authentication_server(github.uri());
    let (server, addr) = server.spawn(common::SERVER_ADDR, locks).await?;
    let server = tokio::spawn(server);

    let request =
        hyper::Request::post(format!("http://{addr}/api/test/test/locks"))
            .header("authorization", "Basic dXNlcjp0b2tlbg==")
            .header("content-type", "application/vnd.git-lfs+json")
            .body(Full::new(Bytes::from_static(
                br#"{"path":"a.bin","ref":{"name":"refs/heads/main"}}"#,
            )))?;
    let response = Client::builder(TokioExecutor::new())
        .build_http()
        .request(request)
        .await?;
    assert_eq!(response.status(), 403);

    server.abort();
    Ok(())
}
