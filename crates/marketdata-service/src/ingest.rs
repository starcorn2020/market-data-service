//! 单一执行緒 ingest 循环：拉 upstream → 写 snapshot → 广播 bus。
//!
//! # 为什么是 `std::thread` 而不是 `tokio::task`
//!
//! GUIDELINE §7.1 / §11：[`crate::upstream::Upstream`] 是同步阻塞 API
//! （`wait()` 内部 `thread::sleep`），放进 tokio worker 会占住一个 OS thread，
//! 干扰其它 async 任务。专用 OS thread 是正解。
//!
//! # 不变量
//!
//! - **I1**：ingest 永不被任何下游阻塞。`snapshot.put` 是 DashMap shard write lock
//!   （极短），`bus.publish` 是 broadcast::send（容量满自动丢最旧，非阻塞）。
//! - **顺序**：先 `snapshot.put` 后 `bus.publish` —— 订阅者收到 update 时，
//!   `GetSnapshot(figi)` 一定能读到至少同一笔（GUIDELINE §4.2 "先写快照，后广播"）。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::bus::Bus;
use crate::snapshot::Snapshot;
use crate::upstream::Upstream;

/// Ingest 线程的控制句柄。Drop 时自动通知 ingest 退出并 join。
pub struct IngestHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<IngestStats>>,
}

/// Ingest 退出时的累计统计。
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    /// 实际成功收到 / 写入 snapshot / 广播的笔数。
    pub received: u64,
    /// `gateway_seq` 不连续的**事件次数**(每次跳跃 = +1,无论跳几笔)。
    ///
    /// 粒度选择 `event count` 是 GUIDELINE §4.2 拍板的(「紀錄一筆 gap event」)。
    /// 「漏了几笔」「閾值」「復原」属 GUIDELINE §13 TODO,不在 deliverable 范围。
    pub gaps: u64,
}

impl IngestHandle {
    /// 通知 ingest 退出（非阻塞）。可重复调用。
    ///
    /// Phase 2 `Service::run` 在 tonic server 退出后 / ctrl-c 触发时调用，
    /// 对称地处理 EOF 与外部 shutdown。
    #[allow(dead_code)]
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// 取 stop 信号的 `Arc` clone，让 [`Service::run`](crate::Service::run)
    /// 在 `tokio::select!` 拿走 `IngestHandle` 之后仍能从外部触发 stop。
    ///
    /// 直接暴露 `Arc<AtomicBool>`（而非定义 `StopToken` newtype）的理由：
    /// - service crate 内部使用，不出 crate 边界，无需类型包装。
    /// - 调用方一律 `.store(true, Release)`，语义即"通知 ingest 退出"。
    pub fn stop_token(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// **等 ingest 自然退出**（不主动发 stop 信号）。
    ///
    /// 典型用法：上游有 `max_messages` cap，等它自然 EOF；或调用方先
    /// 手动 [`stop`](Self::stop) 再 `join`。直接 `join` 一个无 cap 的 ingest
    /// 会**永远阻塞**——这是有意的，让"非阻塞 stop"与"等结束"两个语义分离。
    pub fn join(mut self) -> IngestStats {
        self.thread
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for IngestHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

/// 启动 ingest 线程。
///
/// 走泛型 `U` 而非 `Box<dyn Upstream>`（GUIDELINE D3-A 静态分派）：
/// `Upstream::receive` 是每秒上千次的热路径，不容忍虚函数开销。
pub fn spawn<U>(
    upstream: U,
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    poll_interval: Duration,
    progress_log_every: u64,
) -> IngestHandle
where
    U: Upstream + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();

    let thread = thread::Builder::new()
        .name("mds-ingest".into())
        .spawn(move || ingest_loop(upstream, snapshot, bus, poll_interval, progress_log_every, stop_t))
        .expect("failed to spawn ingest thread");

    IngestHandle {
        stop,
        thread: Some(thread),
    }
}

fn ingest_loop<U: Upstream>(
    upstream: U,
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    poll_interval: Duration,
    progress_log_every: u64,
    stop: Arc<AtomicBool>,
) -> IngestStats {
    let mut stats = IngestStats::default();
    // GUIDELINE §4.2: `gateway_seq` 全流嚴格遞增；用作 gap 检测的唯一可靠依据。
    let mut last_seq: Option<u64> = None;

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        // 外层 wait：唯一合法的"上游结束"信号通道。
        if upstream.wait(poll_interval).is_err() {
            break;
        }

        // 内层 drain loop：把当下 buffer 一次掏空，再回外层 wait。
        // 缺这层 → 每 poll_interval 只取一笔，feed-sim buffer 必然溢位丢消息。
        loop {
            match upstream.receive() {
                Ok(Some(book)) => {
                    stats.received += 1;

                    // 考虑过乱序情况，在feed-sim的情况下不会产生错误
                    // 未来在实际情况下可能会需要调整
                    if let Some(prev) = last_seq
                        && book.gateway_seq != prev + 1
                    {
                        stats.gaps += 1;
                    }
                    last_seq = Some(book.gateway_seq);

                    // 先 snapshot，后 bus —— 顺序敏感。
                    snapshot.put(book);
                    bus.publish(book);

                    if progress_log_every > 0
                        && stats.received.is_multiple_of(progress_log_every)
                    {
                        eprintln!(
                            "[ingest] received={} snapshot.len={} gaps={} total_generated={}",
                            stats.received,
                            snapshot.len(),
                            stats.gaps,
                            upstream.total_generated(),
                        );
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // GUIDELINE §6：单条错误不应杀整个服务；log + 跳回外层 wait。
                    eprintln!("[ingest] receive error: {e:?}");
                    break;
                }
            }
        }
    }

    eprintln!(
        "[ingest] stopped: received={} snapshot.len={} gaps={} total_generated={}",
        stats.received,
        snapshot.len(),
        stats.gaps,
        upstream.total_generated(),
    );
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::{MockUpstream, make_book};

    /// 推入 N 笔连续 seq、close upstream、等 ingest 自然 EOF。
    /// 验证 ingest_loop 正确 drain + 写 snapshot + 不误报 gap。
    #[test]
    fn ingest_drains_finite_mock_and_populates_snapshot() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        for seq in 1..=30u64 {
            // 3 个 FIGI 轮替，验证 snapshot.len() == 3
            let figi = format!("F{:011}", seq % 3);
            handle_in.push(make_book(&figi, seq));
        }
        handle_in.close();

        let handle = spawn(
            up,
            snap.clone(),
            bus.clone(),
            Duration::from_millis(5),
            0,
        );
        let stats = handle.join();

        assert_eq!(stats.received, 30);
        assert_eq!(snap.len(), 3, "3 distinct FIGIs (seq % 3)");
        assert_eq!(stats.gaps, 0, "1..=30 has no gaps");
    }

    /// **T3（DEV_PROCESS §5.1）**：守 GUIDELINE §4.2 "gateway_seq 全流嚴格遞增"。
    ///
    /// 注入 seq=1, 2, 5 → ingest_loop 应在 5 处累进一次 `gaps`。
    #[test]
    fn gap_counter_increments_on_skipped_seq() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        handle_in.push(make_book("BBG000000001", 1));
        handle_in.push(make_book("BBG000000001", 2));
        // seq 跳到 5 —— 3 与 4 缺失，对应一次 gap event。
        handle_in.push(make_book("BBG000000001", 5));
        handle_in.close();

        let handle = spawn(
            up,
            snap.clone(),
            bus.clone(),
            Duration::from_millis(5),
            0,
        );
        let stats = handle.join();

        assert_eq!(stats.received, 3);
        assert_eq!(stats.gaps, 1, "skipping 3,4 between 2 and 5 = 1 gap event");
    }

    /// 守 ingest 顺序不变量："先 snapshot.put、后 bus.publish"。
    ///
    /// 推一笔 → ingest 结束 → assert snapshot 必含这笔。bus 未必有订阅者，
    /// 但 snapshot 必须先写好（否则 README §2 取不到最新）。
    #[test]
    fn snapshot_populated_before_join_returns() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        handle_in.push(make_book("BBG000000042", 42));
        handle_in.close();

        let handle = spawn(up, snap.clone(), bus.clone(), Duration::from_millis(5), 0);
        let _ = handle.join();

        let got = snap.get(&"BBG000000042".parse().unwrap()).unwrap();
        assert_eq!(got.gateway_seq, 42);
    }
}
