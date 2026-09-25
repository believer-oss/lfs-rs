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

use std::path::Path;

use futures::future::Either;
use lfs_rs::LocalServerBuilder;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::oneshot;

use common::{GitRepo, SERVER_ADDR, init_logger};

#[tokio::test(flavor = "multi_thread")]
async fn local_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = init_logger();
    let _startup_span = common::startup();

    // Make sure our seed is deterministic. This makes it easier to reproduce
    // the same repo every time.
    let mut rng = StdRng::seed_from_u64(42);

    let data = tempfile::TempDir::new()?;
    let key = Some(rng.random());

    let locks = lfs_rs::NoneLs::new();

    let server = LocalServerBuilder::new(data.path().into(), key);
    let (server, addr) = server.spawn(SERVER_ADDR, locks).await?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(futures::future::select(shutdown_rx, server));

    let repo = GitRepo::init(addr)?;
    repo.add_random(Path::new("4mb.bin"), 4 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("8mb.bin"), 8 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("16mb.bin"), 16 * 1024 * 1024, &mut rng)?;
    repo.commit("Add LFS objects")?;

    // Make sure we can push LFS objects to the server.
    repo.lfs_push()?;

    // Push again. This should be super fast.
    repo.lfs_push()?;

    // This should be fast since we already have the data
    repo.lfs_pull()?;

    // Make sure we can re-download the same objects in another repo
    let repo_clone = repo.clone_repo(None).expect("unable to clone");

    // This should be fast since the lfs data should come along properly with
    // the clone
    repo_clone.lfs_pull()?;

    // Add some more files and make sure you can pull those into the clone
    repo.add_random(Path::new("4mb_2.bin"), 4 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("8mb_2.bin"), 8 * 1024 * 1024, &mut rng)?;
    repo.add_random(Path::new("16mb_2.bin"), 16 * 1024 * 1024, &mut rng)?;
    repo.commit("Add LFS objects 2")?;

    repo_clone.pull()?;

    shutdown_tx.send(()).expect("server died too soon");

    if let Either::Right((result, _)) = server.await? {
        // If the server exited first, then propagate the error.
        result?;
    }

    Ok(())
}

/// The index page is the health check, and browsers need its content type.
#[tokio::test(flavor = "multi_thread")]
async fn index_is_html() -> Result<(), Box<dyn std::error::Error>> {
    use http_body_util::Empty;
    use hyper::header::CONTENT_TYPE;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let data = tempfile::TempDir::new()?;
    let server = LocalServerBuilder::new(data.path().into(), None);
    let (server, addr) =
        server.spawn(SERVER_ADDR, lfs_rs::NoneLs::new()).await?;
    let server = tokio::spawn(server);

    let client = Client::builder(TokioExecutor::new())
        .build_http::<Empty<bytes::Bytes>>();
    let response = client.get(format!("http://{addr}/").parse()?).await?;

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/html; charset=utf-8");

    server.abort();
    Ok(())
}
