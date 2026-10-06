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

/// The local backend has no disk cache, so it refuses to start with one
/// rather than run without it.
#[test]
fn local_storage_refuses_a_cache_dir() -> Result<(), Box<dyn std::error::Error>>
{
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let dir = tempfile::TempDir::new()?;
    let mut server = Command::new(env!("CARGO_BIN_EXE_lfs-rs"))
        .arg("--cache-dir")
        .arg(dir.path().join("cache"))
        .args(["--host", "127.0.0.1:0", "local", "--path"])
        .arg(dir.path().join("objects"))
        .env_remove("RUDOLFS_CACHE_DIR")
        .stdout(Stdio::piped())
        .spawn()?;

    // If it starts, it runs until it's stopped.
    let started = Instant::now();
    while server.try_wait()?.is_none() {
        if started.elapsed() > Duration::from_secs(10) {
            server.kill()?;
            panic!("it started");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let output = server.wait_with_output()?;
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--cache-dir is only supported with the s3 backend"),
        "{stdout}"
    );
    Ok(())
}

/// On SIGTERM, which Kubernetes sends to stop a pod, the server refuses new
/// connections but lets a request in flight, such as a long upload, finish,
/// and then exits.
#[cfg(unix)]
#[test]
fn sigterm_lets_requests_in_flight_finish()
-> Result<(), Box<dyn std::error::Error>> {
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpStream;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let dir = tempfile::TempDir::new()?;
    let mut server = Command::new(env!("CARGO_BIN_EXE_lfs-rs"))
        .args(["--host", "127.0.0.1:0", "--shutdown-timeout", "20s"])
        .args(["local", "--path"])
        .arg(dir.path().join("objects"))
        .env("RUDOLFS_LOG", "info")
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .spawn()?;

    // The address it listens on, from its log.
    let mut lines = BufReader::new(server.stdout.take().unwrap()).lines();
    let addr = loop {
        let line = lines.next().ok_or("the server exited")??;
        if let Some(addr) = line.split("Listening on ").nth(1) {
            break addr.trim().to_string();
        }
    };
    // Keep reading the log, so the server never blocks on a full pipe.
    let log = std::thread::spawn(move || {
        lines.map_while(Result::ok).collect::<Vec<_>>().join("\n")
    });

    let object = vec![7u8; 1 << 20];
    let oid: String = Sha256::digest(&object)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut upload = TcpStream::connect(&addr)?;
    write!(
        upload,
        "PUT /api/test/test/object/{oid} HTTP/1.1\r\nHost: \
         localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        object.len()
    )?;
    let (first, rest) = object.split_at(object.len() / 2);
    upload.write_all(first)?;
    upload.flush()?;
    std::thread::sleep(Duration::from_millis(200));

    let status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(status.success());
    std::thread::sleep(Duration::from_millis(500));

    // It no longer takes new connections...
    assert!(TcpStream::connect(&addr).is_err(), "took a new connection");

    // ...but the upload in flight finishes.
    upload.write_all(rest)?;
    let mut response = String::new();
    upload.read_to_string(&mut response)?;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    // And then it exits, well before the 20s it would have waited.
    let started = Instant::now();
    let exit = loop {
        if let Some(exit) = server.try_wait()? {
            break exit;
        }
        if started.elapsed() > Duration::from_secs(10) {
            server.kill()?;
            panic!("it didn't exit");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(exit.success(), "{exit}");
    let log = log.join().unwrap();
    assert!(log.contains("SIGTERM received"), "{log}");
    Ok(())
}
