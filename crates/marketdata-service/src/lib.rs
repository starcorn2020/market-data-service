//! # `marketdata-service`
//!
//! Take-home assignment 的核心 crate:接住 [`feed_sim`] 喷出的 `BookMessage`
//! 流, 对外同时提供 request/response (取最新快照) 与 pub/sub (推播即时更新)
//! 两种 API 通过 gRPC 暴露。
//!
//! ## 对外 API 形状
//!
//! ```ignore
//! let cfg = ServiceConfig::from_env()?;
//! let service = Service::new(cfg)?;     // 假设身处 tokio runtime 上下文
//! service.run().await?;                  // 阻塞到 ctrl_c 或上游 EOF
//! ```
//!
//! 整体架构 (ingest → snapshot + bus → gRPC handler) 与设计取舍详见
//! 各 sub-module 的顶部 doc, 以及 `crates/marketdata-service/README.md`。

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
// 错误别名
// ---------------------------------------------------------------------------

/// Service 层统一错误类型。
///
/// `Box<dyn Error + Send + Sync + 'static>` 是 `Send + Sync`，可跨 `tokio::spawn`
/// 边界；同时 `?` 自动从任何实现 `Error + Send + Sync + 'static` 的具体错误转换，
/// 替代 `anyhow::Error` 的 ergonomic 又不引入额外依赖。
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

/// 服务总体生命周期 holder。
///
/// 持有 ingest 线程句柄 + 共享状态 (snapshot 表、fan-out bus) + gRPC 监听
/// 地址。
///
/// # Runtime 主导权外推
///
/// `Service::new` **假设身处 tokio runtime 上下文** (构造 `Bus` 不需要 runtime,
/// 但留出 runtime 给 `tonic::transport::Server::serve` 与 `tokio::spawn`)。
/// service crate 自身不写 `#[tokio::main]`, runtime 配置权留给 `main.rs` 或
/// 调用方 —— 便于以不同 runtime 配置 (worker 数、scheduler 类型) 复用本 crate。
///
/// # 两个启动入口:`run` vs `start`
///
/// | | [`Service::run`] | [`Service::start`] |
/// |---|---|---|
/// | 用途 | 生产 binary | 集成测试 |
/// | 阻塞语义 | `.await` 到 ctrl_c / 自然 EOF 才返回 | 立即返回 [`RunningService`] 句柄 |
/// | `listen_addr` 用法 | 通常 `0.0.0.0:50051` | 通常 `127.0.0.1:0` (OS 分配动态 port) |
/// | Shutdown 触发 | ctrl_c 信号 / ingest 自然 EOF | [`RunningService::shutdown`] 显式调用 |
/// | 错误返回 | 直接走 `Result<(), BoxError>` | 推给 background server task, `shutdown().await` 时收 |
///
/// 二者共用 `Service::new*` 入口, 所以构造副作用 (`mds-ingest` std::thread 已
/// spawn) 是一致的;差别只在 server 生命周期管理。
pub struct Service {
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    ingest: Option<IngestHandle>,
    listen_addr: SocketAddr,
    subscriber_queue_size: usize,
}

impl Service {
    /// 构造服务：启动 ingest 线程，绑定 snapshot 表与 bus。
    ///
    /// # 副作用顺序(顺序敏感)
    ///
    /// 1. **早 fail-fast**:先 `cfg.validate()`,避免无效配置触发后续昂贵副作用。
    /// 2. 构造 [`FeedSimUpstream`] → **立即启动 feed-sim 背景执行緒**(其内部
    ///    会 spawn 一条 generator thread,持续往 buffer 推 `BookMessage`)。
    /// 3. 走 [`Service::new_with_upstream`] → **再 spawn 一条 std::thread `mds-ingest`**
    ///    (见 `ingest::spawn`),从此 ingest 持续 drain upstream 写 snapshot + bus。
    ///
    /// 因此 [`Service::run`] / [`Service::start`] 被 await 之前 ingest **已经在累积数据**;
    /// gRPC server 上线时 snapshot 表通常已经热身完毕(消除 client 第一笔必然 NotYet
    /// 的窘境)。
    ///
    /// # 生命周期兜底
    ///
    /// `Service` drop 时:`IngestHandle::Drop` 会 set stop + `JoinHandle::join`,
    /// `FeedSimUpstream::Drop` 会 stop + join feed-sim 背景执行緒(500ms 内)。
    /// 双重 Drop 兜底防止 thread leak。
    pub fn new(cfg: ServiceConfig) -> Result<Self, BoxError> {
        // 早 fail-fast:避免 cfg 无效时也启动 feed-sim 背景执行緒。`validate` 廉价,
        // `new_with_upstream` 内还会再调用一次(防御性),双重调用零成本。
        cfg.validate()?;
        let upstream = FeedSimUpstream::new(cfg.upstream.clone())?;
        Self::new_with_upstream(cfg, upstream)
    }

    /// 测试 / 自定义上游入口:注入任意 [`Upstream`] 实作。
    ///
    /// 与 [`Service::new`] 的区别:不构造 `FeedSimUpstream`, 让调用方控制
    /// 上游的速率与终止时机。集成测试用 [`MockUpstream`] 走这条路径, 避免
    /// 真 feed-sim 背景执行緒的不确定性。
    ///
    /// 走泛型 `<U: Upstream + 'static>` 而非 `Box<dyn Upstream>` (静态分派):
    /// `Upstream::receive` 是 hot path, 不容忍虚函数开销。详见
    /// `upstream/mod.rs` 顶部 doc。
    ///
    /// # 副作用
    ///
    /// 与 [`Service::new`] 相同:**立即** spawn 一条名为 `mds-ingest` 的
    /// `std::thread` (`ingest::spawn` 内部), 从此 ingest 开始 drain upstream。
    /// `Service` drop 时由 `IngestHandle::Drop` 兜底 stop + join。
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

    /// 同时跑 ingest 与 tonic gRPC server，三路合流退出条件：
    ///
    /// 1. **Ctrl-C** → tonic `serve_with_shutdown` 退出 → set stop_token → ingest 退出。
    /// 2. **tonic `serve` 自身报错**(端口冲突等) → set stop_token → ingest 退出 → 向上抛 err。
    /// 3. **Ingest 自然 EOF**(上游 `SIM_MAX_MESSAGES` 跑完) → 立即返回(serve 由 process exit 兜底)。
    ///
    /// # Cancel-safety
    ///
    /// 本 select! 是 **single-shot**(不在 loop 内),两臂总只 fire 一次。传统
    /// cancel-safety 顾虑(loop 内多次 cancel/resubmit 漏消息)不适用 —— 选定的
    /// 那臂的 future 已经驱动完成,落选臂的 future 被 drop,无 partial state 残留。
    ///
    /// 具体每臂 cancel 后的行为:
    /// - `ingest_join` 被 drop → spawn_blocking task **不能 abort**(tokio 文档明示),
    ///   其 closure 内 `ingest_handle.join()` 仍会跑完;`IngestHandle::Drop` 兜底
    ///   set stop + join 防 thread leak。
    /// - `serve_fut` 被 drop → tonic 走 graceful drop path,但**已 in-flight 的 RPC
    ///   会被 abort**(HTTP/2 RST_STREAM 到达 client)。Demo / 测试场景下可接受;
    ///   production 严格场景需走 path 1 改造,见下方 Future work 注释。
    pub async fn run(mut self) -> Result<(), BoxError> {
        let ingest_handle = self
            .ingest
            .take()
            .ok_or_else(|| -> BoxError { "Service::run called twice".into() })?;

        // gRPC service：注入共享 snapshot / bus。
        let svc = MarketDataService::new(
            self.snapshot.clone(),
            self.bus.clone(),
            self.subscriber_queue_size,
        );
        let addr = self.listen_addr;
        eprintln!("[server] listening on {addr}");

        // Ingest join 必须放进 blocking pool —— ingest 是 std::thread, 其
        // `JoinHandle::join` 是同步阻塞调用, 不能直接 await。
        // 拆法:clone stop signal 给上面 select! 用, spawn_blocking 等结束。
        let stop_token = ingest_handle.stop_token();
        let ingest_join = tokio::task::spawn_blocking(move || ingest_handle.join());

        // tonic serve 直接 await；ctrl_c 用 `tokio::signal::ctrl_c`。
        //
        // **当前架构限制**:shutdown signal **只**响应 ctrl_c。若 ingest 自然 EOF
        // (path 3),无法通过 shared shutdown channel 让 serve 也 graceful 退出 ——
        // 当前只能让 process exit 兜底 cancel server task。Future work:用
        // `oneshot::channel` + `tokio::select! { ctrl_c, shutdown_rx }` 作为 shutdown
        // signal,让 path 3 也能触发 serve 真正 graceful drain。本次刻意不做,
        // 因为重构需要把 `ingest_join` 借入 select! (`tokio::pin!` + `&mut`),复杂度
        // 与 demo / 测试场景的实际收益不匹配。
        let serve_fut = tonic::transport::Server::builder()
            .add_service(MarketDataServer::new(svc))
            .serve_with_shutdown(addr, async {
                let _ = tokio::signal::ctrl_c().await;
            });

        tokio::select! {
            // 路径 3:ingest 自然 EOF (finite stream / max_messages 跑完)。
            //
            // 当前**不**等 serve_fut 退出:ingest EOF 后立即返回 → main 函数
            // 返回 → process exit → runtime drop 把 serve task abort。代价:
            // 任何还在 stream 的 client 收到 broken transport (HTTP/2
            // RST_STREAM), 不是 graceful FIN。Demo / 测试场景可接受;production
            // 严格场景应通过上方注释里的 shared shutdown channel 重构。
            join_res = ingest_join => {
                let stats = join_res
                    .map_err(|e| -> BoxError { format!("ingest join task panicked: {e}").into() })?;
                // log 字段对齐 `ingest_loop` 自己的 `[ingest] stopped: ...` —— reviewer
                // 看两行 log 时不会困惑「service 层为何字段更少」。snapshot.len() 是
                // 当下读,与 ingest stop 之间有 μs 级延迟,但不影响最终一致性观察。
                eprintln!(
                    "[service] ingest finished: received={} snapshot.len={} gaps={}",
                    stats.received,
                    self.snapshot.len(),
                    stats.gaps,
                );
                Ok(())
            }
            // 路径 1 + 2:tonic serve 退出 (ctrl_c 触发 graceful shutdown, 或
            //              serve 自身错误如端口冲突)。set stop_token 通知 ingest
            //              退出。
            //
            // **不等 `ingest_join.await`**:`ingest_join` 在 select! 中被 move 进
            // future, 这条臂 fire 时 ingest_join future 被 drop, **但
            // spawn_blocking 内部的 closure 仍会跑完** (spawn_blocking 不可
            // abort)。所以 `ingest_handle.join()` 仍会被调用, 只是返回的
            // `IngestStats` 我们拿不到。代价:
            //   - log 顺序可能乱:`[server] shut down gracefully` 先打,
            //     `[ingest] stopped` 稍后打 (由 ingest_loop 自己 print)。可接受。
            //   - 拿不到 final stats:`[ingest] stopped: received=N` log 仍能在
            //     stderr 看到, service 层不重复打。
            // 修正需要 `tokio::pin!(ingest_join)` + select! 用 `&mut ingest_join`,
            // 收益与代价不匹配, 保持当前简化路径。
            serve_res = serve_fut => {
                stop_token.store(true, std::sync::atomic::Ordering::Release);
                serve_res.map_err(|e| -> BoxError { format!("tonic serve failed: {e}").into() })?;
                eprintln!("[server] shut down gracefully");
                Ok(())
            }
        }
    }

    /// 当前 snapshot 表中已知的 FIGI 数（demo / 测试用）。
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// 集成测试用启动入口：bind 一个 TcpListener、后台 spawn tonic server，
    /// 立即返回 [`RunningService`] 句柄。
    ///
    /// 与 [`Service::run`] 的核心区别：
    /// - `run`：阻塞 await，走 ctrl_c / 自然 EOF 退出。生产 binary 路径。
    /// - `start`：后台 spawn，立即返回 `addr` 与 shutdown handle。集成测试路径。
    ///
    /// 支持 `listen_addr = 127.0.0.1:0`（OS 分配动态端口），用 [`RunningService::addr`]
    /// 拿实际监听端口。集成测试用此避免端口冲突。
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

        // oneshot 作为 shutdown 信号通道:`shutdown_tx.send(())` **或** `shutdown_tx`
        // 被 drop,二者**任一**都会让 `shutdown_rx.await` 完成(`Receiver::poll` 在
        // sender drop 时返 `Err(RecvError)`,本处 `let _` 吞掉)。
        //
        // 双重保护:
        //   ① 显式 [`RunningService::shutdown`].await → tx.send(()) → 走 normal path;
        //   ② 调用方漏调 shutdown → [`RunningService::Drop`] 内 tx.send(()) 兜底;
        //   ③ 极端情况 tx 已被 drop 且没 send → shutdown_rx 仍因 sender drop 醒来。
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let join = tokio::spawn(async move {
            let serve_res = tonic::transport::Server::builder()
                .add_service(MarketDataServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await;

            // 让 ingest 也停下来，避免线程泄漏（IngestHandle::drop 会 stop+join）。
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

/// 测试用：由 [`Service::start`] 返回的运行中服务句柄。
///
/// 持有 server task 的 join handle 与 graceful shutdown 信号。Drop 时
/// 自动发送 shutdown 信号防止 server task 泄漏，但**强烈建议**显式
/// `.shutdown().await` 以等待 task 真正结束（否则 ingest std::thread
/// 可能在 runtime drop 之后才被回收，引发 cleanup race）。
pub struct RunningService {
    local_addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), BoxError>>>,
    snapshot: Arc<Snapshot>,
}

impl RunningService {
    /// 实际监听的地址（含 OS 分配的动态端口）。
    pub fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 当前 snapshot 表已知 FIGI 数。集成测试断言 ingest 仍在跑用。
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// 发送 shutdown 信号并 await server task 结束。
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
        // 防御性兜底:测试遗漏 [`Self::shutdown`] 时也不要让 server task / ingest
        // thread 泄漏。
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // 同步 Drop 不能 await server task 的 `JoinHandle`。生命周期收敛靠三层保护:
        //
        //   ① shutdown_tx.send(()) → server task 内 `shutdown_rx.await` 解决 →
        //      task 走 normal path 退出,内部 `drop(ingest)` 触发 `IngestHandle::Drop`
        //      → stop + std::thread::join → ingest 干净退出。
        //   ② 若 ① 来不及完成(runtime 立刻关停),tokio runtime drop 会 cancel
        //      所有未完成 task → server task abort → 其作用域内的 `ingest:
        //      IngestHandle` 也走 Drop → set stop + join 兜底。
        //   ③ 极端情况 runtime 已 dead,std::thread 由 OS 在 process exit 时回收。
        //
        // **强烈建议**调用方仍显式 `.shutdown().await`,理由:
        //   - 拿到 server task 的 `Result<(), BoxError>` 返回值(否则永远丢失)。
        //   - log 顺序确定:不显式 await 时,server task 退出 log 可能出现在测试
        //     断言之后,触发 reviewer「这是测试结束后才打印的吗」的困惑。
    }
}

#[cfg(test)]
mod tests {
    //! `Service` 构造期的早 fail-fast 行为守护。
    //!
    //! `run` / `start` 的运行时行为由 `tests/grpc_basic.rs` integration 测试
    //! 覆盖 (NotYet/Found / Subscribe 推流 / 空 figi 拒绝 / too-long figi
    //! 拒绝);本档只守**构造路径的 validate 优先级**:无效 cfg 必须**早**于
    //! 昂贵副作用 (spawn `mds-ingest` std::thread) 被拒绝, 避免"无效 cfg
    //! 已经 spawn thread 后才 fail" 的丑路径。
    //!
    //! 不在本档加 `run` / `start` 的 unit test:二者本质上需要 mock 上游 +
    //! 真实 tonic server, 等于重写 integration 流程, 重复成本高。

    use super::*;
    use crate::upstream::MockUpstream;

    fn cfg_with_zero_bus_capacity() -> ServiceConfig {
        ServiceConfig {
            bus_channel_capacity: 0,
            ..Default::default()
        }
    }

    /// 守 "无效 cfg 在 spawn thread 之前被拒绝":`new_with_upstream` 入口的
    /// `cfg.validate()?` 短路 reject, **根本不**走到 `ingest::spawn`。
    ///
    /// 间接证明:若 validate 通过, `spawn` 会创一条 `mds-ingest` 线程并立即
    /// 开始 drain MockUpstream;本测试构造的 `MockUpstream` 没 push 任何 book
    /// 也没 close, 若 ingest 真启动会进入 wait/poll 循环, 但**因为 validate
    /// 失败, 这条路径不会被触发**, 函数立刻返回 Err。
    #[tokio::test(flavor = "current_thread")]
    async fn new_with_upstream_rejects_invalid_config_early() {
        let (up, _handle) = MockUpstream::new();
        let result = Service::new_with_upstream(cfg_with_zero_bus_capacity(), up);

        // `Result::expect_err` 要求 Ok 一侧实现 `Debug`,但 `Service` 不实现
        // (内部 `Option<IngestHandle>` / `Arc<Bus>` 等不 Debug)。退化为
        // `.err().expect(...)` 用 Option 的 expect 绕开 Debug 需求。
        let err = result
            .err()
            .expect("无效 cfg 必拒,但 new_with_upstream 返了 Ok(Service)");
        let msg = err.to_string();
        assert!(
            msg.contains("bus_channel_capacity"),
            "error 应明确指出哪个字段无效,实际 {msg:?}"
        );
    }
}
