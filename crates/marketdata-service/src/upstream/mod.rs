//! 上游 feed 抽象层。
//!
//! 整个 service crate 里**唯一**可以 `use feed_sim::*` 的地方 —— 所有跨
//! module 的调用都走 [`Upstream`] trait, 把 vendor 类型封死在本 mod 内:
//!
//! 1. 任何依赖 `Upstream` 的代码 (`ingest.rs` / mock 测试) 都看不见
//!    `feed_sim::FeedSubscriber`。
//! 2. 未来把 `feed-sim` 换成真实 iceoryx2 (或别的上游), 只需新增
//!    `upstream/iceoryx2.rs` 实现 `Upstream` trait, 其它 mod 0 改动。

use std::time::Duration;

use marketdata_types::BookMessage;

use crate::BoxError;

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
    fn receive(&self) -> Result<Option<BookMessage>, BoxError>;

    /// 阻塞 `duration` 后返回；`Err(())` = 上游已排空且关闭 = 唯一合法的结束信号。
    //
    // `Result<(), ()>` 对齐 `feed_sim::FeedSubscriber::wait` 写死的契约 ——
    // `Err(())` 是 feed-sim 的唯一合法结束讯号(无 error variant 区分),改 custom
    // error type 会破坏 feed-sim 边界对应。clippy `result_unit_err` 此处忽略。
    #[allow(clippy::result_unit_err)]
    fn wait(&self, duration: Duration) -> Result<(), ()>;

    /// 累计生成 / 入 buffer 的数量（供 sanity check）。
    fn total_generated(&self) -> u64;
}
