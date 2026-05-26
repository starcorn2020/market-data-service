//! Per-FIGI 最新快照表。
//!
//! # 设计要点
//!
//! - 底层 `DashMap<Figi, BookMessage>`：shard-locked HashMap，读写都不阻塞，
//!   契合 ingest（写多）+ RPC（读少）的非对称负载。
//! - `BookMessage` 是 `#[repr(C)] + Copy`，整份 ~408 bytes：
//!   - 写：`insert` 直接整份覆盖，**不做** increment 合并（GUIDELINE N3 non-goal）。
//!   - 读：值拷贝出去（不回 `&BookMessage`，避免跨线程生命周期）。
//! - `Figi` 是 `[u8; 12] + Copy + Hash + Eq`，**直接当 key**，不要做成 `String`。

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
    #[inline]
    pub fn put(&self, msg: BookMessage) {
        self.inner.insert(msg.figi, msg);
    }

    /// 取该 FIGI 的最新快照值。
    ///
    /// `None` 即 README §3 的"clearly-defined no data yet"信号；
    /// Phase 2 的 `GetSnapshot` RPC 会把这个映射到 `SnapshotResponse::NotYet`。
    #[inline]
    pub fn get(&self, figi: &Figi) -> Option<BookMessage> {
        self.inner.get(figi).map(|e| *e.value())
    }

    /// 已知 FIGI 数。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 与 [`Self::len`] 配套；clippy 习惯成对暴露。当前未直接使用。
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    #[test]
    fn put_then_get_returns_latest() {
        let s = Snapshot::new();
        let mut m = BookMessage::default();
        m.figi = figi("BBG000000001");
        m.gateway_seq = 1;
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
}
