//! Per-FIGI 最新快照表。
//!
//! # 设计要点
//!
//! - 底层 `DashMap<Figi, BookMessage>`:shard-locked HashMap, 读写都不阻塞,
//!   契合 ingest (写多) + RPC (读少) 的非对称负载。
//! - `BookMessage` 是 `#[repr(C)] + Copy`, 整份 ~408 bytes:
//!   - 写:`insert` 直接整份覆盖, **不做** increment 合并 —— 上游契约保证
//!     每笔 `BookMessage` 已是 top-10 完整快照, 不需要在 service 这层合并。
//!   - 读:值拷贝出去 (不回 `&BookMessage`, 避免跨线程生命周期纠缠)。
//! - `Figi` 是 `[u8; 12] + Copy + Hash + Eq`, **直接当 key**, 不做成 `String`。
//!
//! # 为何选 `DashMap` 而非 `Arc<RwLock<HashMap>>`
//!
//! DashMap 把 map 切成 N 个 shard, 每个 shard 各自一把 RwLock;distinct FIGI
//! 通常落在不同 shard, **互不阻塞**。`Arc<RwLock<HashMap>>` 反而把所有访问
//! 串行化 —— ingest write 期间所有 RPC read 都要等, 破坏 "读路径也不被
//! 写路径阻塞" 的对外契约。
//!
//! `Arc<T>` 本身**不是锁**, 只是引用计数, hot path 上 `Arc::deref` 是零成本
//! 指针解引用 —— 概念上和 `Mutex<T>` / `RwLock<T>` 完全两回事。
//!
//! # 为何不抽 trait
//!
//! 与 `Upstream` 抽 trait (为 mock + 未来换上游铺路) 不同, `Snapshot` 是
//! **内部模组** (`mod snapshot;` private), 没有第二个实作需求, 也没有"测试
//! 要 mock" 的压力 —— DashMap 已经够轻量, unit test 直接构造真实 `Snapshot`
//! 实例即可。抽 trait 只会增加 vtable / 泛型噪声而无任何收益。
//!
//! 唯一对外泄漏的是 `Service::snapshot_len() -> usize` (demo / 测试用的
//! 标量), 完全在"不泄漏内部类型"的安全侧。

use dashmap::DashMap;
use marketdata_types::{BookMessage, Figi};

/// Per-FIGI 最新快照表。线程安全，可 `Arc` 共享给 ingest 与 RPC handler。
#[derive(Default)]
pub struct Snapshot {
    inner: DashMap<Figi, BookMessage>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// 整份覆盖写入。Ingest hot path 调用。
    ///
    /// # 整份覆盖语义
    ///
    /// `BookMessage` 已经是 top-10 完整快照 (upstream 契约), 本方法直接
    /// `insert`, **绝不做** order-by-order / increment 合并 —— 旧 entry 的
    /// 任何字段都不应残留。由 [`tests::put_overwrites_entire_book_not_merge`]
    /// 守护:若未来有人"优化"成"合并旧新 bids/asks", 该测试 fail。
    #[inline]
    pub fn put(&self, msg: BookMessage) {
        self.inner.insert(msg.figi, msg);
    }

    /// 取该 FIGI 的最新快照值。
    ///
    /// `None` 即题面要求的 "clearly-defined no data yet" 信号;gRPC handler 把
    /// 这个映射到 `SnapshotResponse::NotYet`。
    #[inline]
    pub fn get(&self, figi: &Figi) -> Option<BookMessage> {
        self.inner.get(figi).map(|e| *e.value())
    }

    /// 已知 FIGI 数。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 与 [`Self::len`] 配套 —— Rust API 惯例要求 `len` + `is_empty` 成对暴露
    /// (clippy `len_without_is_empty`)。当前 production 路径用 `len`, 但保留
    /// `is_empty` 以符合 idiom。
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! Snapshot 单元测试。
    //!
    //! 守的契约:
    //!
    //! | 测试 | 守的契约 |
    //! |---|---|
    //! | `put_then_get_returns_latest` | 同 FIGI 二次 put 后 get 返新 seq (基础 happy path) |
    //! | `get_returns_none_for_unknown_figi` | 未知 FIGI 返 `None` → wire 层 `NotYet` |
    //! | `put_overwrites_entire_book_not_merge` | **整份覆盖**:绝不做增量合并 |
    //! | `is_empty_reflects_population` | `is_empty` / `len` 在初始 + 推入后的行为一致 |
    //!
    //! **不**写并发 put 测试:DashMap 自身已被业界压测, 自己写一个 `tokio::join!`
    //! 多 task 测试本质是测 DashMap, 信号弱 + 复杂度高。若未来发现真实 race,
    //! 补 deterministic regression test 即可。

    use super::*;
    use marketdata_types::BookLevel;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    #[test]
    fn put_then_get_returns_latest() {
        let s = Snapshot::new();
        let mut m = BookMessage {
            figi: figi("BBG000000001"),
            gateway_seq: 1,
            ..Default::default()
        };
        s.put(m);
        m.gateway_seq = 42;
        s.put(m);

        let got = s.get(&figi("BBG000000001")).unwrap();
        assert_eq!(got.gateway_seq, 42);
    }

    #[test]
    fn get_returns_none_for_unknown_figi() {
        let s = Snapshot::new();
        assert!(s.get(&figi("BBG000000999")).is_none());
    }

    /// 守 "整份覆盖, 不做 increment 合并" 的契约 —— 来自题面 non-goal:
    /// "Building an L3 book from increments — `BookMessage` is already a top-10
    /// snapshot"。直接 `snapshots.insert(msg.figi, *msg)`。
    ///
    /// 旧 book `bid_count=2 / ask_count=1`, 新 book `bid_count=1 / ask_count=2`;
    /// 第二次 `put` 后 `get` 必须**完整反映新 book**, 旧 book 的任何字段都不
    /// 残留。若未来有人把 `put` 改为"合并旧新 bids/asks", 本测试 fail。
    #[test]
    fn put_overwrites_entire_book_not_merge() {
        let s = Snapshot::new();
        let f = figi("BBG000000001");

        let mut old = BookMessage {
            figi: f,
            gateway_seq: 1,
            bid_count: 2,
            ask_count: 1,
            ..Default::default()
        };
        old.bids[0] = BookLevel { price: 100.0, qty: 1.0, orders: 3 };
        old.bids[1] = BookLevel { price: 99.0, qty: 2.0, orders: 5 };
        old.asks[0] = BookLevel { price: 101.0, qty: 1.5, orders: 4 };
        s.put(old);

        let mut new = BookMessage {
            figi: f,
            gateway_seq: 2,
            bid_count: 1,
            ask_count: 2,
            ..Default::default()
        };
        new.bids[0] = BookLevel { price: 200.0, qty: 7.0, orders: 9 };
        new.asks[0] = BookLevel { price: 201.0, qty: 8.0, orders: 11 };
        new.asks[1] = BookLevel { price: 202.0, qty: 6.0, orders: 2 };
        s.put(new);

        let got = s.get(&f).expect("FIGI present after second put");
        assert_eq!(got.gateway_seq, 2, "seq 必须反映新 book");
        assert_eq!(
            got.bid_count, 1,
            "bid_count 必为 new(=1), 旧 bid_count=2 绝不残留"
        );
        assert_eq!(got.ask_count, 2, "ask_count 必为 new(=2)");
        assert_eq!(got.bids[0].price, 200.0, "首档 bid 必为 new 的 200.0");
        assert_eq!(got.asks[0].price, 201.0, "首档 ask 必为 new 的 201.0");
        assert_eq!(got.asks[1].price, 202.0, "次档 ask 必为 new 的 202.0");
        // 注:`bids[1]` 不验 —— `BookMessage` 是 `#[repr(C)] + Copy` 整份覆盖,
        // 数组 slot 由 default 重置, 但**有效范围**由 `bid_count` 控制
        // (`.bids()` / `.asks()` 切片只返回 count 范围内)。这里只验 "**有效**
        // 部分必为 new"。
    }

    /// 守 `is_empty` / `len` 在初始与推入后的行为一致。
    ///
    /// `is_empty` 是 clippy `len_without_is_empty` 强制的配对暴露, 无 production
    /// 调用者, 但作为 public API 仍需测试覆盖 —— 防止未来 wrapper bug 无回归保护。
    #[test]
    fn is_empty_reflects_population() {
        let s = Snapshot::new();
        assert!(s.is_empty(), "起始必为空");
        assert_eq!(s.len(), 0);

        let m = BookMessage {
            figi: figi("BBG000000001"),
            ..Default::default()
        };
        s.put(m);

        assert!(!s.is_empty(), "put 一笔后 is_empty 必为 false");
        assert_eq!(s.len(), 1);
    }
}
