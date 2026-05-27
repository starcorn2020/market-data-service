//! Per-FIGI latest-snapshot table.
//!
//! # Design notes
//!
//! - Backed by `DashMap<Figi, BookMessage>`: a shard-locked HashMap; reads
//!   and writes do not block each other, matching the asymmetric load of
//!   ingest (write-heavy) + RPC (read-light).
//! - `BookMessage` is `#[repr(C)] + Copy`, ~408 bytes:
//!   - Write: `insert` performs a full overwrite; we **do not** merge
//!     increments — the upstream contract guarantees that each
//!     `BookMessage` is already a complete top-10 snapshot, no merging
//!     needed at the service layer.
//!   - Read: returns a value copy (not `&BookMessage`, to avoid
//!     cross-thread lifetime entanglement).
//! - `Figi` is `[u8; 12] + Copy + Hash + Eq` and is used **directly as the
//!   key**, not wrapped in a `String`.
//!
//! # Why `DashMap` over `Arc<RwLock<HashMap>>`
//!
//! DashMap splits the map into N shards, each guarded by its own RwLock;
//! distinct FIGIs usually land in different shards and **do not block each
//! other**. `Arc<RwLock<HashMap>>` instead serializes all access — during
//! an ingest write every RPC read has to wait, breaking the public
//! contract that "the read path is never blocked by the write path".
//!
//! `Arc<T>` is **not a lock**; it is just a reference count. On the hot
//! path `Arc::deref` is a zero-cost pointer deref — conceptually entirely
//! different from `Mutex<T>` / `RwLock<T>`.
//!
//! # Why no trait abstraction
//!
//! Unlike `Upstream` (which is behind a trait to support mocks + future
//! upstream swaps), `Snapshot` is an **internal module** (`mod snapshot;`
//! private). There is no second implementation requirement and no
//! "tests need to mock" pressure — DashMap is already lightweight enough,
//! and unit tests construct a real `Snapshot` instance directly. Adding a
//! trait would only introduce vtable / generics noise with no benefit.
//!
//! The only thing exposed publicly is `Service::snapshot_len() -> usize`
//! (a scalar, for demo / test purposes), which is safely on the "no
//! internal type leaks" side.

use dashmap::DashMap;
use marketdata_types::{BookMessage, Figi};

/// Per-FIGI latest-snapshot table. Thread-safe, shareable via `Arc`
/// between ingest and RPC handlers.
#[derive(Default)]
pub struct Snapshot {
    inner: DashMap<Figi, BookMessage>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Full-overwrite write. Called on the ingest hot path.
    ///
    /// # Full-overwrite semantics
    ///
    /// `BookMessage` is already a complete top-10 snapshot (per upstream
    /// contract); this method simply calls `insert` and **never** performs
    /// any order-by-order / incremental merge — no field from the previous
    /// entry should linger. Guarded by
    /// [`tests::put_overwrites_entire_book_not_merge`]: if anyone later
    /// "optimizes" this into a merge of old and new bids/asks, that test
    /// fails.
    #[inline]
    pub fn put(&self, msg: BookMessage) {
        self.inner.insert(msg.figi, msg);
    }

    /// Read the latest snapshot value for the given FIGI.
    ///
    /// `None` is the "clearly-defined no data yet" signal required by the
    /// assignment; the gRPC handler maps it to the `NotYet` variant inside
    /// each `SnapshotEntry` of the `GetSnapshots` response.
    #[inline]
    pub fn get(&self, figi: &Figi) -> Option<BookMessage> {
        self.inner.get(figi).map(|e| *e.value())
    }

    /// Number of known FIGIs.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Paired with [`Self::len`] — Rust API convention requires `len` and
    /// `is_empty` to be exposed together (clippy `len_without_is_empty`).
    /// The current production path uses `len`, but `is_empty` is kept to
    /// match the idiom.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! Snapshot unit tests.
    //!
    //! Contracts guarded:
    //!
    //! | Test | Contract |
    //! |---|---|
    //! | `put_then_get_returns_latest` | A second put on the same FIGI must surface the new seq on get (basic happy path) |
    //! | `get_returns_none_for_unknown_figi` | Unknown FIGI returns `None` → maps to `NotYet` on the wire |
    //! | `put_overwrites_entire_book_not_merge` | **Full overwrite**: never an incremental merge |
    //! | `is_empty_reflects_population` | `is_empty` / `len` behave consistently before and after insertion |
    //!
    //! We deliberately **do not** add a concurrent-put test: DashMap has
    //! been stress-tested by the wider community, so writing a
    //! `tokio::join!` multi-task test essentially tests DashMap itself —
    //! weak signal, high complexity. If a real race surfaces in the
    //! future, we will add a deterministic regression test then.

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

    /// Guards the "full overwrite, no incremental merge" contract — from
    /// the assignment's non-goal: "Building an L3 book from increments —
    /// `BookMessage` is already a top-10 snapshot". Implementation is
    /// simply `snapshots.insert(msg.figi, *msg)`.
    ///
    /// Old book: `bid_count=2 / ask_count=1`; new book: `bid_count=1 /
    /// ask_count=2`. After the second `put`, `get` must **fully reflect
    /// the new book**, with no field from the old book leaking through.
    /// If anyone later turns `put` into "merge old and new bids/asks",
    /// this test fails.
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
        assert_eq!(got.gateway_seq, 2, "seq must reflect the new book");
        assert_eq!(
            got.bid_count, 1,
            "bid_count must be new(=1); the old bid_count=2 must not leak through"
        );
        assert_eq!(got.ask_count, 2, "ask_count must be new(=2)");
        assert_eq!(got.bids[0].price, 200.0, "top bid must be new's 200.0");
        assert_eq!(got.asks[0].price, 201.0, "top ask must be new's 201.0");
        assert_eq!(got.asks[1].price, 202.0, "second-level ask must be new's 202.0");
        // Note: `bids[1]` is intentionally not asserted — `BookMessage`
        // is `#[repr(C)] + Copy` and overwritten in full; the array slots
        // are reset by default, but the **effective range** is controlled
        // by `bid_count` (the `.bids()` / `.asks()` slices only return the
        // first `count` entries). This only verifies that the **effective**
        // portion matches new.
    }

    /// Guards consistent behavior of `is_empty` / `len` before and after
    /// insertion.
    ///
    /// `is_empty` is the paired API enforced by clippy
    /// `len_without_is_empty`; it has no production callers, but as a
    /// public API it still needs test coverage — preventing future
    /// wrapper bugs from going unnoticed.
    #[test]
    fn is_empty_reflects_population() {
        let s = Snapshot::new();
        assert!(s.is_empty(), "initially must be empty");
        assert_eq!(s.len(), 0);

        let m = BookMessage {
            figi: figi("BBG000000001"),
            ..Default::default()
        };
        s.put(m);

        assert!(!s.is_empty(), "is_empty must be false after one put");
        assert_eq!(s.len(), 1);
    }
}
