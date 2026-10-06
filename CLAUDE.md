# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`lfs-rs` is a caching Git LFS server (S3 or local-disk backend, optional
xchacha20 encryption, optional file locking). It began as a private fork of
[rudolfs](https://github.com/jasonwhite/rudolfs). Many env vars, log messages
and Docker paths still say `RUDOLFS_*` / `rudolfs`, so keep those names when
you edit. Upstream changes are brought in as patch files under `patches/`
(see `patches/README.md`), not merged, because the fork has no link to the
upstream repo.

## Commands

These are the same checks CI runs (`.github/workflows/ci.yml`):

```bash
cargo check
cargo test --features redis            # every feature except `faulty`; see below
cargo +nightly fmt --all -- --check   # rustfmt.toml uses nightly-only options
cargo clippy --all-features           # lib.rs has #![deny(clippy::all)]
```

- Run one integration test file: `cargo test --test test_local`
- Run one test by name: `cargo test --test test_local <name>`
- Run the server locally: `cargo run -- --cache-dir cache --host localhost:8080 --max-cache-size 10GiB [--key <hex32>] s3 --bucket <bucket>`,
  or use `local --path <dir>` instead of `s3 ...`. Global flags go **before**
  the backend subcommand. `test.sh` is gitignored so it can hold a local run
  script.
- Docker images are static musl binaries in a `scratch` image, published for
  amd64 and arm64 by the manually triggered `docker` workflow (each built on a
  runner of its own architecture, then combined into one multi-arch tag). To
  build locally: `podman build --platform linux/arm64 .`

Cargo features: `dynamodb` and `otel` are on by default. `redis` and
`faulty` are opt-in. `faulty` injects random failures into the S3 and cache
byte streams to exercise the retry/verify layers. The S3 round-trip tests
can't pass with it on, so don't test with `--all-features`. Clippy still
uses `--all-features` so that `faulty` gets compiled.

### Integration tests

- The tests in `tests/` drive a real `git` + `git lfs` client (through `duct`
  in `tests/common.rs`) against a server started in-process. `git-lfs` must
  be installed. Every git call goes through the `git!` macro, which points
  `GIT_CONFIG_GLOBAL` at a generated config, so your own `~/.gitconfig`
  (e.g. `lfs.storage`) can't affect the tests.
- The S3, DynamoDB and Redis tests read their targets from `LFS_TEST_*` env
  vars
  (documented in `tests/common.rs`) and skip when those are unset.
  `LFS_TEST_REQUIRED=1` turns a skip into a failure, and CI sets it.
  Credentials go into an explicit `SdkConfig` (`S3ServerBuilder::sdk_config`,
  `DynamoLs::from_config`), never through `AWS_*` env vars or `set_var`.
- Every test bucket and table is named `lfs-rs-scratch-*`, so a mistyped
  name can't reach a real one, and test credentials can be scoped to that
  prefix in IAM. The harness rejects buckets without the prefix. It adds the
  prefix to table names itself, because the tests delete and recreate those
  tables.
- CI runs them against rustfs, DynamoDB Local and Redis. To do the same locally,
  start `tests/docker-compose.yaml` and export the variables listed at the
  top of that file. The tests create the bucket and tables themselves. The
  image digests are pinned in both that file and `.github/workflows/ci.yml`,
  so keep the two in sync.
- Transfer acceleration can't run against S3-compatible servers. It is
  covered by offline unit tests in `src/storage/s3.rs`, which check the
  presigned URLs' host and credential scope.

## Architecture

### Storage is a stack of decorators

`storage::Storage` (`src/storage/mod.rs`) is an async trait with methods
`get`/`put`/`size`/`delete`/`list` plus optional signed/public URL methods.
Each backend in `src/storage/` either implements it directly (`Disk`, `S3`)
or wraps another `Storage` (`Cached`, `Encrypted`, `Verify`, `Faulty`). The
builders in `src/lib.rs` (`S3ServerBuilder`, `LocalServerBuilder`) put the stack
together. There is no retry layer: S3 retries are the AWS SDK's (3 attempts for
transient errors), checked by the `retries` tests in `s3.rs`. For S3 the order
is:

```
Verify( Verify(Encrypted(Cached(Disk, S3))) | Verify(Cached(Disk, S3)) )
```

Rules about the order:
- `Verify` checks downloads' SHA256 against the OID, so it has to sit
  outside `Encrypted` because it needs plaintext. When a download fails
  verification, `Verify` deletes the object from the layer below. That is how
  corrupted cache entries get cleaned up. Uploads are checked by the upload
  handler in `app.rs`, which answers a mismatch with a 400.
- `Cached` keeps an in-memory LRU (`src/lru.rs`) of the disk cache. The LRU
  is rebuilt from `list()` at startup and pruned down to `--max-cache-size`.
  On a cache miss, `LFSObject::fanout` streams the object to the client and
  into the cache at the same time.
- `--cdn` turns off both the disk cache and encryption, because data no
  longer passes through the server.
- The S3 clients refresh credentials 35 minutes before they expire
  (`--s3-credential-refresh-buffer`), so that presigned URLs get their full
  15 minutes. The SDK refreshes somewhere between half the buffer and all of
  it before expiry, and `app.rs` checks at compile time that URLs last at most
  half. This only suits credential sources that hand out new credentials when
  asked early (Pod Identity, IRSA, SSO). See the comment on
  `DEFAULT_CREDENTIAL_REFRESH_BUFFER` in `s3.rs` for why EC2 instance and ECS
  task roles need `0`.
- git-lfs uploads each object in one PUT, and S3 takes at most 5 GiB per PUT.
  So uploads over 5 GiB are never presigned. They go through the server, which
  sends them to S3 as a multipart upload (100 MiB parts, reading the next part
  while one uploads, aborting on failure). Objects over a quarter of the disk
  cache bypass it both ways.
- `S3` has its own LRU for object sizes (`--s3-size-cache-entries`), which
  avoids repeated HEAD requests during batch calls. It switches to multipart
  upload for large objects.
- Different backends are combined with `futures::future::Either`. `Storage`
  is implemented for `Either` and `Arc<S>`.

### Request path

`spawn_server` in `src/lib.rs` creates one service chain for each
connection: `Logger` → `auth::Auth` → `app::App`.
On SIGTERM (what Kubernetes sends) or SIGINT it stops taking connections and
waits up to `--shutdown-timeout` (default 25s) for requests in flight, such as
an upload through the server, to finish.

- `App` (`src/app.rs`) routes by hand on
  `/api/{org}/{project}/{object/<oid> | objects/batch | objects/verify | locks/...}`.
  Each `{org}/{project}` pair becomes a `storage::Namespace`, which is part
  of every `StorageKey`.
- LFS wire types live in `src/lfs.rs`.
- `Auth` is a passthrough unless `--github-auth` is set. When it is set, the
  request's Authorization header is checked against the GitHub repo API for
  pull/push permission, and results go into a shared `LinkedHashMap` cache.
  Tests replace GitHub with a mock via `authentication_server(...)`, backed
  by `wiremock`.

### Locks

The `locks::LockStorage` trait (`src/locks/mod.rs`) has four
implementations: `NoneLs` (the default, where lock endpoints return not
implemented), `LocalLs` (filesystem), `DynamoLs`, and `RedisLs`. The last
two depend on Cargo features. `--lock-backend` selects one.
`LockStoreError` variants map to HTTP statuses in
`handle_lock_error_response` in `app.rs`.

### Observability

`src/init_tracing.rs` sets up logging for both builds. `main` creates the
subscriber and holds its guard until after the final error is logged. `exit`
skips destructors, so the guard is dropped explicitly first. `RUST_LOG`
(default `info`) sets the filter. `--log-level` / `RUDOLFS_LOG`, if given, sets
this crate's level, unless `RUST_LOG` names `lfs_rs` itself. ANSI colors are only
used on a terminal.

With the `otel` feature, traces and metrics are exported over OTLP/gRPC
(`OTEL_EXPORTER_OTLP_ENDPOINT` etc.), and handlers use `#[instrument]` behind
`cfg(feature = "otel")`. Keep `info` for what an operator needs. Health checks,
`ret` values and per-call detail belong at `debug`. Any otel-specific code you
add needs the same `cfg` gate so that `--no-default-features` still builds.

Activity is counted in `stats::STATS`, a process-wide set of atomic counters
(`src/stats.rs`). The same counters feed the periodic `stats` line at `info`
(`--stats-interval`, default 1h, deltas since the previous line) and the OTLP
metrics that `init_tracing` registers, exported every 60s. Request latency is
an OTel histogram in `logger.rs`. Tests that check counters must assert "at
least" on deltas, since other tests in the binary share them.

## Conventions

- Keep lines within 80 columns. rustfmt also wraps comments and strings.
- Source files begin with the MIT header in `.license_template`.
