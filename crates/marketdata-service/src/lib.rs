//! # `marketdata-service`
//!
//! Core crate for the take-home assignment: it consumes the `BookMessage`
//! stream emitted by [`feed_sim`] and exposes two APIs over gRPC —
//! request/response (latest snapshot) and pub/sub (push real-time updates).
//!
//! ## Public API shape
//!
//! ```ignore
//! let cfg = ServiceConfig::from_env()?;
//! let service = Service::new(cfg)?;     // assumes a tokio runtime context
//! service.run().await?;                  // blocks until ctrl_c or upstream EOF
//! ```
//!
//! The overall architecture (ingest → snapshot + bus → gRPC handler) and
//! design trade-offs are documented at the top of each sub-module and in
//! `crates/marketdata-service/README.md`.

#![warn(missing_docs)]

mod bus;
mod config;
mod grpc;
mod ingest;
mod snapshot;
mod upstream;

pub use config::{ServiceConfig, UpstreamConfig};
pub use grpc::pb;
pub use upstream::{MockHandle, MockUpstream, Upstream, make_book};

// ---------------------------------------------------------------------------
// Error alias
// ---------------------------------------------------------------------------

/// Unified service-layer error type.
///
/// `Box<dyn Error + Send + Sync + 'static>` is `Send + Sync`, so it can cross
/// `tokio::spawn` boundaries. `?` automatically converts from any concrete
/// error implementing `Error + Send + Sync + 'static`, giving us the
/// ergonomics of `anyhow::Error` without an extra dependency.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::bus::Bus;
use crate::grpc::{MarketDataServer, MarketDataService};
use crate::ingest::IngestHandle;
use crate::snapshot::Snapshot;
use crate::upstream::FeedSimUpstream;

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// Holder for the overall service lifecycle.
///
/// Owns the ingest thread handle, shared state (snapshot table, fan-out bus),
/// and the gRPC listen address.
///
/// # Runtime ownership pushed to the caller
///
/// `Service::new` **assumes a tokio runtime context** (constructing `Bus`
/// itself does not require a runtime, but one is required later for
/// `tonic::transport::Server::serve` and `tokio::spawn`). This crate does not
/// declare `#[tokio::main]` itself; runtime configuration is left to `main.rs`
/// or the caller, so the crate can be reused with different runtime
/// configurations (worker count, scheduler type).
///
/// # Two entry points: `run` vs `start`
///
/// | | [`Service::run`] | [`Service::start`] |
/// |---|---|---|
/// | Use case | Production binary | Integration tests |
/// | Blocking semantics | `.await`s until ctrl_c / natural EOF | Returns a [`RunningService`] handle immediately |
/// | `listen_addr` convention | Typically `0.0.0.0:50051` | Typically `127.0.0.1:0` (OS-assigned dynamic port) |
/// | Shutdown trigger | ctrl_c signal / ingest natural EOF | Explicit [`RunningService::shutdown`] call |
/// | Error reporting | Returned via `Result<(), BoxError>` | Pushed to the background server task, surfaced by `shutdown().await` |
///
/// Both paths share the `Service::new*` entry point, so construction
/// side effects (the `mds-ingest` std::thread has already been spawned) are
/// identical; only the server lifecycle management differs.
pub struct Service {
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    ingest: Option<IngestHandle>,
    listen_addr: SocketAddr,
    subscriber_queue_size: usize,
}

impl Service {
    /// Construct the service: start the ingest thread and wire up the
    /// snapshot table and bus.
    ///
    /// # Side-effect ordering (order-sensitive)
    ///
    /// 1. **Fail fast early**: call `cfg.validate()` first, so invalid
    ///    configuration does not trigger the expensive side effects below.
    /// 2. Construct [`FeedSimUpstream`] → **immediately starts the feed-sim
    ///    background thread** (which internally spawns a generator thread
    ///    that keeps pushing `BookMessage`s into a buffer).
    /// 3. Delegate to [`Service::new_with_upstream`] → **spawns another
    ///    std::thread named `mds-ingest`** (see `ingest::spawn`); from this
    ///    point on, ingest continuously drains upstream and writes to
    ///    snapshot + bus.
    ///
    /// Consequently, by the time [`Service::run`] / [`Service::start`] is
    /// awaited, ingest **has already been accumulating data**; the snapshot
    /// table is typically warm by the time the gRPC server comes online,
    /// avoiding the awkward case where the client's first request always
    /// returns `NotYet`.
    ///
    /// # Lifecycle safety net
    ///
    /// On `Service` drop: `IngestHandle::Drop` sets stop and calls
    /// `JoinHandle::join`; `FeedSimUpstream::Drop` stops and joins the
    /// feed-sim background thread (within 500ms). The double-Drop safety
    /// net guards against thread leaks.
    pub fn new(cfg: ServiceConfig) -> Result<Self, BoxError> {
        // Fail-fast: avoid spawning the feed-sim background thread when the
        // config is invalid. `validate` is cheap; `new_with_upstream` calls
        // it again (defensively), so the double call is free.
        cfg.validate()?;
        let upstream = FeedSimUpstream::new(cfg.upstream.clone())?;
        Self::new_with_upstream(cfg, upstream)
    }

    /// Test / custom-upstream entry point: inject any [`Upstream`]
    /// implementation.
    ///
    /// Difference from [`Service::new`]: does not construct
    /// `FeedSimUpstream`; the caller controls upstream rate and termination.
    /// Integration tests use [`MockUpstream`] via this path to avoid the
    /// nondeterminism of the real feed-sim background thread.
    ///
    /// Uses generic `<U: Upstream + 'static>` (static dispatch) rather than
    /// `Box<dyn Upstream>`: `Upstream::receive` is on the hot path and does
    /// not tolerate virtual call overhead. See the top of `upstream/mod.rs`
    /// for details.
    ///
    /// # Side effects
    ///
    /// Same as [`Service::new`]: **immediately** spawns a `std::thread`
    /// named `mds-ingest` (inside `ingest::spawn`); from then on ingest
    /// drains upstream. On `Service` drop, `IngestHandle::Drop` provides the
    /// stop + join safety net.
    pub fn new_with_upstream<U: Upstream + 'static>(
        cfg: ServiceConfig,
        upstream: U,
    ) -> Result<Self, BoxError> {
        cfg.validate()?;

        let snapshot = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(cfg.bus_channel_capacity));

        let ingest = ingest::spawn(
            upstream,
            snapshot.clone(),
            bus.clone(),
            cfg.poll_interval,
            cfg.progress_log_every,
        );

        Ok(Self {
            snapshot,
            bus,
            ingest: Some(ingest),
            listen_addr: cfg.listen_addr,
            subscriber_queue_size: cfg.subscriber_queue_size,
        })
    }

    /// Run ingest and the tonic gRPC server concurrently. Three exit paths
    /// converge here:
    ///
    /// 1. **Ctrl-C** → tonic `serve_with_shutdown` returns → set stop_token → ingest exits.
    /// 2. **tonic `serve` itself errors** (e.g. port conflict) → set stop_token → ingest exits → propagate the error.
    /// 3. **Ingest natural EOF** (upstream `SIM_MAX_MESSAGES` exhausted) → return immediately (process exit cancels serve).
    ///
    /// # Cancel-safety
    ///
    /// This `select!` is **single-shot** (not inside a loop), so each arm
    /// fires at most once. The usual cancel-safety concern (loops that
    /// cancel/resubmit and miss messages) does not apply — the winning arm's
    /// future has already been driven to completion, the losing arm's future
    /// is dropped, and no partial state is left behind.
    ///
    /// Per-arm cancel behavior:
    /// - `ingest_join` dropped → the spawn_blocking task **cannot be aborted**
    ///   (per tokio docs); its closure still runs `ingest_handle.join()` to
    ///   completion. `IngestHandle::Drop` provides the stop + join safety net
    ///   to prevent thread leaks.
    /// - `serve_fut` dropped → tonic follows the graceful drop path, but
    ///   **in-flight RPCs are aborted** (HTTP/2 RST_STREAM reaches the
    ///   client). Acceptable for demo / test scenarios; strict production
    ///   scenarios should follow path 1 with the refactor noted below.
    pub async fn run(mut self) -> Result<(), BoxError> {
        let ingest_handle = self
            .ingest
            .take()
            .ok_or_else(|| -> BoxError { "Service::run called twice".into() })?;

        // gRPC service: inject shared snapshot / bus.
        let svc = MarketDataService::new(
            self.snapshot.clone(),
            self.bus.clone(),
            self.subscriber_queue_size,
        );
        let addr = self.listen_addr;
        eprintln!("[server] listening on {addr}");

        // Ingest join must go onto the blocking pool — ingest is a
        // std::thread, and its `JoinHandle::join` is a synchronous blocking
        // call that cannot be awaited directly. The split: clone the stop
        // signal for `select!` above, spawn_blocking to wait for completion.
        let stop_token = ingest_handle.stop_token();
        let ingest_join = tokio::task::spawn_blocking(move || ingest_handle.join());

        // tonic `serve` is awaited directly; ctrl_c is handled via
        // `tokio::signal::ctrl_c`.
        //
        // **Current architectural limit**: the shutdown signal **only**
        // responds to ctrl_c. If ingest reaches natural EOF (path 3), there
        // is no shared shutdown channel to make serve exit gracefully — we
        // rely on process exit to cancel the server task. Future work: use
        // an `oneshot::channel` + `tokio::select! { ctrl_c, shutdown_rx }`
        // as the shutdown signal so path 3 can also trigger a true graceful
        // drain. Deliberately not done here: the refactor requires borrowing
        // `ingest_join` into `select!` (`tokio::pin!` + `&mut`), and the
        // complexity does not match the real-world benefit for demo / test
        // scenarios.
        let serve_fut = tonic::transport::Server::builder()
            .add_service(MarketDataServer::new(svc))
            .serve_with_shutdown(addr, async {
                let _ = tokio::signal::ctrl_c().await;
            });

        tokio::select! {
            // Path 3: ingest natural EOF (finite stream / max_messages exhausted).
            //
            // We deliberately **do not** wait for `serve_fut` to exit: once
            // ingest hits EOF we return immediately → main returns → process
            // exits → runtime drop aborts the server task. The cost: any
            // client currently streaming receives a broken transport (HTTP/2
            // RST_STREAM) rather than a graceful FIN. Acceptable in demo /
            // test scenarios; strict production scenarios should use the
            // shared shutdown channel refactor described above.
            join_res = ingest_join => {
                let stats = join_res
                    .map_err(|e| -> BoxError { format!("ingest join task panicked: {e}").into() })?;
                // Log fields are aligned with `ingest_loop`'s own
                // `[ingest] stopped: ...` line — when the reviewer reads both
                // logs, they should not be puzzled by "why does the service
                // layer have fewer fields". `snapshot.len()` is read at this
                // moment; there is a μs-scale gap between this and ingest
                // stopping, which does not affect final-consistency observation.
                eprintln!(
                    "[service] ingest finished: received={} snapshot.len={} gaps={}",
                    stats.received,
                    self.snapshot.len(),
                    stats.gaps,
                );
                Ok(())
            }
            // Paths 1 + 2: tonic serve exited (ctrl_c triggered graceful
            // shutdown, or serve itself errored, e.g. port conflict). Set
            // `stop_token` to notify ingest to exit.
            //
            // **We do not `ingest_join.await`**: `ingest_join` was moved into
            // the `select!` future; when this arm fires, the `ingest_join`
            // future is dropped — **but the closure inside spawn_blocking
            // still runs to completion** (spawn_blocking cannot be aborted).
            // So `ingest_handle.join()` is still called, we just lose the
            // returned `IngestStats`. The cost:
            //   - Log order may be jumbled: `[server] shut down gracefully`
            //     prints first, `[ingest] stopped` prints slightly later
            //     (printed by ingest_loop itself). Acceptable.
            //   - Final stats are unavailable here: the `[ingest] stopped:
            //     received=N` log still reaches stderr from ingest_loop, so
            //     we do not duplicate it in the service layer.
            // Fixing this would require `tokio::pin!(ingest_join)` plus
            // `select!` using `&mut ingest_join`. The benefit does not match
            // the cost, so we keep the simplified path.
            serve_res = serve_fut => {
                stop_token.store(true, std::sync::atomic::Ordering::Release);
                serve_res.map_err(|e| -> BoxError { format!("tonic serve failed: {e}").into() })?;
                eprintln!("[server] shut down gracefully");
                Ok(())
            }
        }
    }

    /// Number of FIGIs currently known in the snapshot table (demo / test use).
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// Integration-test entry point: bind a TcpListener, spawn the tonic
    /// server in the background, and return a [`RunningService`] handle
    /// immediately.
    ///
    /// Key differences from [`Service::run`]:
    /// - `run`: blocking `.await`, exits on ctrl_c / natural EOF. Production
    ///   binary path.
    /// - `start`: spawns in the background, returns `addr` and a shutdown
    ///   handle immediately. Integration-test path.
    ///
    /// Supports `listen_addr = 127.0.0.1:0` (OS-assigned dynamic port); use
    /// [`RunningService::addr`] to retrieve the actual port. Integration
    /// tests rely on this to avoid port conflicts.
    pub async fn start(mut self) -> Result<RunningService, BoxError> {
        let listen_addr = self.listen_addr;
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .map_err(|e| -> BoxError { format!("bind tcp listener on {listen_addr}: {e}").into() })?;
        let local_addr = listener.local_addr()?;

        let ingest = self
            .ingest
            .take()
            .ok_or_else(|| -> BoxError { "Service::start called twice".into() })?;
        let stop_token = ingest.stop_token();
        let snapshot = self.snapshot.clone();

        let svc = MarketDataService::new(
            self.snapshot.clone(),
            self.bus.clone(),
            self.subscriber_queue_size,
        );

        // The oneshot is the shutdown signal channel: either
        // `shutdown_tx.send(())` **or** dropping `shutdown_tx` — **either
        // one** wakes `shutdown_rx.await` (`Receiver::poll` returns
        // `Err(RecvError)` when the sender is dropped; the `let _` here
        // swallows it).
        //
        // Triple protection:
        //   ① Explicit [`RunningService::shutdown`].await → `tx.send(())` → normal path.
        //   ② Caller forgets to call shutdown → [`RunningService::Drop`] does `tx.send(())` as the safety net.
        //   ③ Extreme case where `tx` was already dropped without sending → `shutdown_rx` still wakes because the sender was dropped.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let join = tokio::spawn(async move {
            let serve_res = tonic::transport::Server::builder()
                .add_service(MarketDataServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await;

            // Stop ingest as well to avoid a thread leak (IngestHandle::drop
            // performs stop + join).
            stop_token.store(true, Ordering::Release);
            drop(ingest);

            serve_res.map_err(|e| -> BoxError { format!("tonic serve failed: {e}").into() })
        });

        Ok(RunningService {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
            snapshot,
        })
    }
}

// ---------------------------------------------------------------------------
// RunningService
// ---------------------------------------------------------------------------

/// Test-only handle for a running service returned by [`Service::start`].
///
/// Holds the server task's join handle and the graceful-shutdown signal.
/// Drop automatically sends the shutdown signal to prevent a server-task
/// leak, but it is **strongly recommended** to call `.shutdown().await`
/// explicitly to wait for the task to actually finish (otherwise the ingest
/// std::thread may not be reaped until after the runtime is dropped, which
/// triggers a cleanup race).
pub struct RunningService {
    local_addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), BoxError>>>,
    snapshot: Arc<Snapshot>,
}

impl RunningService {
    /// The actual listen address (including the OS-assigned dynamic port).
    pub fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Number of FIGIs currently known in the snapshot table. Used by
    /// integration tests to assert that ingest is still running.
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// Send the shutdown signal and await server-task completion.
    pub async fn shutdown(mut self) -> Result<(), BoxError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            join.await
                .map_err(|e| -> BoxError { format!("server task panicked: {e}").into() })?
        } else {
            Ok(())
        }
    }
}

impl Drop for RunningService {
    fn drop(&mut self) {
        // Defensive safety net: even when a test forgets to call
        // [`Self::shutdown`], do not let the server task / ingest thread leak.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // A synchronous `Drop` cannot await the server task's `JoinHandle`.
        // Lifecycle convergence relies on three layers of protection:
        //
        //   ① `shutdown_tx.send(())` → `shutdown_rx.await` inside the server
        //      task resolves → the task takes the normal exit path; its
        //      internal `drop(ingest)` triggers `IngestHandle::Drop` →
        //      stop + std::thread::join → ingest exits cleanly.
        //   ② If ① cannot complete in time (the runtime is being torn down
        //      immediately), the tokio runtime drop cancels all pending
        //      tasks → the server task is aborted → the `ingest:
        //      IngestHandle` in its scope is also dropped → set stop + join
        //      as the safety net.
        //   ③ In the extreme case where the runtime is already dead, the
        //      std::thread is reaped by the OS on process exit.
        //
        // It is **strongly recommended** that callers still explicitly call
        // `.shutdown().await`:
        //   - It surfaces the server task's `Result<(), BoxError>` return
        //     value (otherwise it is lost forever).
        //   - It guarantees log order: without an explicit await, the
        //     server-task exit log may appear after the test assertion,
        //     causing the reviewer to wonder "did this print after the test
        //     finished?".
    }
}

#[cfg(test)]
mod tests {
    //! Guards the early fail-fast behavior of `Service` construction.
    //!
    //! Runtime behavior of `run` / `start` is covered by the
    //! `tests/grpc_basic.rs` integration tests (NotYet/Found / Subscribe
    //! streaming / empty figi rejection / too-long figi rejection); this
    //! file only guards the **validate-priority on the construction path**:
    //! an invalid cfg must be rejected **before** any expensive side effect
    //! (spawning the `mds-ingest` std::thread), to avoid the ugly failure
    //! mode where "the thread is already spawned and then we fail".
    //!
    //! We do not add unit tests for `run` / `start` here: doing so would
    //! require mocking the upstream and running a real tonic server, which
    //! amounts to rewriting the integration flow with high duplication cost.

    use super::*;
    use crate::upstream::MockUpstream;

    fn cfg_with_zero_bus_capacity() -> ServiceConfig {
        ServiceConfig {
            bus_channel_capacity: 0,
            ..Default::default()
        }
    }

    /// Guards "an invalid cfg is rejected before any thread is spawned":
    /// the `cfg.validate()?` short-circuit at the `new_with_upstream` entry
    /// point rejects the request and **never** reaches `ingest::spawn`.
    ///
    /// Indirect proof: if validate had passed, `spawn` would have created an
    /// `mds-ingest` thread and immediately started draining MockUpstream.
    /// The `MockUpstream` constructed in this test never pushes a book and
    /// never closes; if ingest had really started, it would have entered a
    /// wait/poll loop — but **because validate fails this path is never
    /// triggered**, and the function returns Err immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn new_with_upstream_rejects_invalid_config_early() {
        let (up, _handle) = MockUpstream::new();
        let result = Service::new_with_upstream(cfg_with_zero_bus_capacity(), up);

        // `Result::expect_err` requires the Ok variant to implement `Debug`,
        // but `Service` does not (its internal `Option<IngestHandle>` /
        // `Arc<Bus>` etc. are not Debug). Fall back to `.err().expect(...)`
        // using `Option::expect` to sidestep the Debug requirement.
        let err = result
            .err()
            .expect("invalid cfg must be rejected, but new_with_upstream returned Ok(Service)");
        let msg = err.to_string();
        assert!(
            msg.contains("bus_channel_capacity"),
            "error should clearly identify which field is invalid, actual {msg:?}"
        );
    }
}
