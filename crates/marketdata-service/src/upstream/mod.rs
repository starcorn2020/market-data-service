//! 上游 feed 抽象层。
//!
//! # I4 不变量在哪里被守护
//!
//! GUIDELINE §5 I4 要求"对外 API 不洩漏 `feed_sim::*`"。本 module 是
//! 整个 service crate 里**唯一**可以 `use feed_sim::*` 的地方——所有
//! 跨 module 的调用都走 [`Upstream`] trait，从而做到：
//!
//! 1. 任何依赖 `Upstream` 的代码（`ingest.rs` / 未来的 mock 测试）都看不见
//!    `FeedSubscriber`。
//! 2. 未来把 `feed-sim` 换成真实 iceoryx2，只需新增 `upstream/iceoryx2.rs`
//!    并实作同一个 trait，**ingest.rs 与 lib.rs 0 改动**。
//!
//! # D3 选项 A：静态分派
//!
//! [`crate::ingest::spawn`] 走泛型 `<U: Upstream>` 而不是 `Box<dyn Upstream>`。
//! 理由：
//!
//! - Ingest 是数据热路径（每秒上千次 `receive` / `wait`），不容忍虚函数开销。
//! - Phase 3 注入 mock 时直接 `spawn::<MockUpstream>(...)`，编译期单态化即可。

use std::time::Duration;

use marketdata_types::BookMessage;

mod feed_sim;
mod mock;

pub use feed_sim::FeedSimUpstream;
pub use mock::{MockHandle, MockUpstream, make_book};

/// Ingest 路径对上游 feed 的唯一依赖。
///
/// 实作者必须保证：
///
/// - `receive` 是**非阻塞**的 try-recv 语意（`Ok(None)` 表示当下无数据，**不**代表结束）。
/// - `wait(d)` 是唯一的关闭信号通道：`Err(())` = 上游彻底排空 + 关闭，
///   ingest loop 才会退出。
/// - `&self` 即可调用，使用者通过 `move` 进 ingest 线程独占。
pub trait Upstream: Send {
    /// 非阻塞拉一笔。`Ok(None)` = 当下 buffer 空；`Ok(Some(_))` = 拿到一笔。
    fn receive(&self) -> anyhow::Result<Option<BookMessage>>;

    /// 阻塞 `duration` 后返回；`Err(())` = 上游已排空且关闭 = 唯一合法的结束信号。
    fn wait(&self, duration: Duration) -> Result<(), ()>;

    /// 累计生成 / 入 buffer 的数量（供 sanity check）。
    fn total_generated(&self) -> u64;
}
