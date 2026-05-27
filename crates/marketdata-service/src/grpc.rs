//! gRPC service implementation.
//!
//! Wraps [`crate::snapshot::Snapshot`] and [`crate::bus::Bus`] as a tonic
//! `MarketData` service.
//!
//! # Design notes
//!
//! - `GetSnapshots` is a unary RPC that takes a batch of FIGIs and returns
//!   one `SnapshotEntry` per FIGI in request order. Each entry maps to
//!   [`Snapshot::get`]: `None` → `NotYet`, `Some` → `Found(Book)` —
//!   exactly the "clearly-defined no data yet" required by the assignment.
//!   No cross-FIGI atomicity: per-entry reads against the underlying
//!   DashMap, semantically identical to issuing N parallel single-FIGI lookups.
//! - `Subscribe` is server-streaming. On the wire side, **`send().await`
//!   is strictly forbidden**: any `await` would let a slow client back-
//!   pressure into fan-in and then into ingest, violating the
//!   "ingest never blocks" invariant. This file uses `try_send`
//!   exclusively, dropping on full and incrementing `dropped_total`.
//! - The wire stage `try_send` and the fan-in stage share the same
//!   `dropped` counter (`Subscription::dropped_counter`), so
//!   `BookUpdate.dropped_total` is the cumulative end-to-end count of
//!   "fan-in drops" + "wire drops".
//! - The `BookMessage ↔ proto::Book` conversion is a pure mechanical
//!   mapping; it lives at the bottom of this file.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use marketdata_types::{BookMessage, Figi};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::bus::Bus;
use crate::snapshot::Snapshot;

// ---------------------------------------------------------------------------
// Generated proto module
// ---------------------------------------------------------------------------

/// `tonic-build` emits `$OUT_DIR/marketdata.v1.rs`, triggered by build.rs;
/// we inline-include it here so the IDE can find it.
///
/// `#![allow(missing_docs)]`: proto comments are turned into doc-comments
/// by tonic-build automatically, but the `derive(Message)` boilerplate
/// fields are not documented; the service crate enforces
/// `#![warn(missing_docs)]` at the top level, so generated code must be
/// exempted.
pub mod pb {
    #![allow(missing_docs)]
    tonic::include_proto!("marketdata.v1");
}

pub use pb::market_data_server::{MarketData, MarketDataServer};
use pb::snapshot_entry::Result as SnapshotResult;
use pb::{
    Book, BookUpdate, GetSnapshotsRequest, GetSnapshotsResponse, Level, NotYet, SnapshotEntry,
    SubscribeRequest,
};

// ---------------------------------------------------------------------------
// Service impl
// ---------------------------------------------------------------------------

/// The concrete type passed to
/// `tonic::Server::add_service(MarketDataServer::new(MarketDataService { ... }))`.
pub struct MarketDataService {
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    /// Capacity of the wire-side mpsc. Tonic server-streaming would
    /// otherwise propagate back-pressure to the producer; we sever that
    /// via `try_send`, and capacity determines how much short-term lag
    /// we tolerate from the client.
    subscriber_queue_size: usize,
}

impl MarketDataService {
    pub fn new(
        snapshot: Arc<Snapshot>,
        bus: Arc<Bus>,
        subscriber_queue_size: usize,
    ) -> Self {
        Self {
            snapshot,
            bus,
            subscriber_queue_size,
        }
    }
}

#[tonic::async_trait]
impl MarketData for MarketDataService {
    // `tonic::Status` is ~176 bytes; clippy's `result_large_err` flags
    // `Result<_, Status>` as too large. This is the standard tonic handler
    // error type and matches the signature on `subscribe` below.
    #[allow(clippy::result_large_err)]
    async fn get_snapshots(
        &self,
        request: Request<GetSnapshotsRequest>,
    ) -> Result<Response<GetSnapshotsResponse>, Status> {
        let figi_strs = request.into_inner().figis;
        if figi_strs.is_empty() {
            return Err(Status::invalid_argument(
                "get_snapshots with empty figi list",
            ));
        }

        // All-or-nothing validation, mirroring `subscribe`: any oversized
        // figi in the batch rejects the entire request. `Figi::from_str`
        // is `Infallible` upstream — input longer than 12 bytes is
        // silently truncated rather than rejected — so the wire layer
        // explicitly rejects overly long figis to avoid the confusing UX
        // where "BBG_LONG_FIGI" is sliced to 12 bytes and almost
        // certainly returns NotYet.
        let mut entries = Vec::with_capacity(figi_strs.len());
        for s in figi_strs {
            if s.len() > 12 {
                return Err(Status::invalid_argument(format!(
                    "figi too long ({} bytes, max 12): {s}",
                    s.len()
                )));
            }
            let figi: Figi = s
                .parse()
                .expect("Figi::from_str is Infallible; length already bounded above");
            let result = match self.snapshot.get(&figi) {
                Some(book) => SnapshotResult::Found(book_to_proto(&book)),
                None => SnapshotResult::NotYet(NotYet {}),
            };
            // Echo the original figi string so the client does not depend
            // on response order to match a result back to a request entry.
            entries.push(SnapshotEntry {
                figi: s,
                result: Some(result),
            });
        }

        Ok(Response::new(GetSnapshotsResponse { entries }))
    }

    type SubscribeStream = ReceiverStream<Result<BookUpdate, Status>>;

    // `tonic::Status` is ~176 bytes; clippy's `result_large_err` flags
    // `Result<_, Status>` as too large. This is the standard tonic
    // handler error type; switching to `Box<Status>` would break tonic's
    // wire contract, and figi-parse failures are a rare path, so size
    // optimization is pointless.
    #[allow(clippy::result_large_err)]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let figi_strs = request.into_inner().figis;
        if figi_strs.is_empty() {
            return Err(Status::invalid_argument("subscribe with empty figi list"));
        }

        // String → Figi conversion. If any single entry fails to parse we
        // reject the entire request (all-or-nothing) — this has clearer
        // semantics than "silently drop the bad figi and subscribe to the
        // rest", which would give the caller a partially-missing
        // subscription with an OK status code. `Figi::from_str` is
        // Infallible (silently truncates); the wire layer explicitly
        // rejects overly long figis to avoid that confusing UX.
        let figis: Vec<Figi> = figi_strs
            .into_iter()
            .map(|s| -> Result<Figi, Status> {
                if s.len() > 12 {
                    return Err(Status::invalid_argument(format!(
                        "figi too long ({} bytes, max 12): {s}",
                        s.len()
                    )));
                }
                Ok(s.parse::<Figi>()
                    .expect("Figi::from_str is Infallible; length already bounded above"))
            })
            .collect::<Result<_, _>>()?;

        // Open the subscription on the bus (this internally spawns N
        // fan-in tasks).
        let mut sub = self.bus.subscribe(&figis, self.subscriber_queue_size);

        // Shared dropped counter: the fan-in stage (bus.rs) and the wire
        // stage (the spawn below) both fetch_add into this same
        // AtomicU64. The cumulative value the client sees covers
        // "end-to-end drops".
        let dropped = sub.dropped_counter();

        // Wire-side mpsc: tonic's ReceiverStream pulls from this
        // Receiver. Capacity is independent of the fan-in side — fan-in
        // drops on full, wire drops on full, and both feed the same
        // `dropped` counter.
        let (out_tx, out_rx) =
            mpsc::channel::<Result<BookUpdate, Status>>(self.subscriber_queue_size);

        // wire-pump task: pulls one message from the fan-in mpsc and
        // hands it to the wire mpsc, which tonic then pushes to the
        // client.
        //
        // Two-armed select!:
        //   - Primary arm `sub.next()`: pushes on new updates;
        //   - Secondary arm `out_tx.closed()`: exits immediately when the
        //     client disconnects, preventing a potential task leak in
        //     the scenario where "the figi has no publish for a long
        //     time, so this task is parked on sub.next() and only sees
        //     Closed at the next publish" (cost is tiny, but the
        //     semantics are unclean).
        // Both arms are cancel-safe (`mpsc::Receiver::recv` /
        // `mpsc::Sender::closed` are documented as such by tokio); the
        // loop re-registers interest each iteration, and there is no
        // starvation.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    book = sub.next() => {
                        let Some(book) = book else { break };
                        let upd = BookUpdate {
                            book: Some(book_to_proto(&book)),
                            // `load(Relaxed)` may briefly lag the
                            // fan-in side's `fetch_add` — multiple
                            // fan_in_one tasks add concurrently while
                            // this single task loads. Benign:
                            // dropped_total is cumulative, so anything
                            // missed by this sample will appear in the
                            // next BookUpdate; the final cumulative
                            // value is correct.
                            dropped_total: dropped.load(Ordering::Relaxed),
                        };
                        // ★ Must use try_send; await is forbidden. Any
                        //   await would let a slow client back-pressure
                        //   into fan-in and then into ingest, violating
                        //   the "ingest never blocks" invariant — a hard
                        //   constraint of the entire service design.
                        match out_tx.try_send(Ok(upd)) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TrySendError::Closed(_)) => {
                                // Equivalent to the secondary arm
                                // out_tx.closed(); client is gone,
                                // exit as the safety net.
                                break;
                            }
                        }
                    }
                    _ = out_tx.closed() => break,
                }
            }
            // All three exit paths arrive here; drop semantics differ
            // by path:
            //   - Secondary arm out_tx.closed() / primary arm try_send
            //     Closed: out_tx is already closed, drop is redundant
            //     but harmless;
            //   - Primary arm sub.next() returning None (fan-in mpsc
            //     closed, e.g. bus has been dropped): out_tx is still
            //     open, so we **must** drop it for the client to see
            //     a stream end rather than hang.
            //
            // About the Status arm of `Result<BookUpdate, Status>`:
            // tonic server-streaming requires stream items to be
            // Result<T, Status>, so even though we only send Ok, we
            // cannot drop the Status arm from the signature. This file
            // does not actively emit a mid-stream Err — client
            // disconnects are signaled by the tonic transport layer
            // (HTTP/2 RST_STREAM) directly, no service intervention
            // needed; if a future graceful shutdown wants to push a
            // Status::Cancelled to all subscribers, this is the
            // injection point.
            drop(out_tx);
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

// ---------------------------------------------------------------------------
// Conversion: marketdata_types::BookMessage  <->  proto Book
// ---------------------------------------------------------------------------

/// Map the service-internal `BookMessage` to the wire `Book`.
///
/// Notes:
/// - `Figi` is `[u8; 12]` (NUL-padded); calling `as_str()` auto-trims.
/// - Only emit the active levels within `bid_count` / `ask_count`
///   (upstream contract: the remaining array slots are `BookLevel::default()`
///   placeholders and must not enter the wire). `BookLevel` is `Copy`,
///   but proto `Level` does not implement `From<BookLevel>` — a one-line
///   lambda does the conversion.
fn book_to_proto(msg: &BookMessage) -> Book {
    Book {
        figi: msg.figi.as_str().to_string(),
        gateway_seq: msg.gateway_seq,
        gateway_ts: msg.gateway_ts,
        bids: msg.bids().iter().map(level_to_proto).collect(),
        asks: msg.asks().iter().map(level_to_proto).collect(),
    }
}

fn level_to_proto(level: &marketdata_types::BookLevel) -> Level {
    Level {
        price: level.price,
        qty: level.qty,
        orders: level.orders as u32,
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! grpc.rs test layering:
    //!
    //! - **This file (unit)**: only tests the **pure conversion function**
    //!   `book_to_proto` — `BookMessage` is `Copy`, the conversion
    //!   involves no IO / spawn / runtime, and is well suited to
    //!   deterministic unit testing.
    //! - **`tests/grpc_basic.rs` (integration)**: covers handler behavior
    //!   on a real gRPC wire — `GetSnapshots` NotYet/Found switching and
    //!   batch shape, `Subscribe` streaming, empty figi / too-long figi
    //!   rejection.
    //! - **`tests/grpc_slow_consumer.rs` (`#[ignore]`, manual)**: stress
    //!   test for wire-layer slow-consumer isolation — proves that the
    //!   wire path also honors the "ingest never blocks" invariant.
    //!
    //! We deliberately do not add handler unit tests here: `MarketData::*`
    //! is `async fn` + `Request<T>`; unit-testing it either requires too
    //! many mocks (weak signal) or essentially rewrites the integration
    //! flow (high duplication). Let integration tests guard handler
    //! logic and let unit tests guard pure functions — cleaner division
    //! of labor.

    use super::*;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    fn sample_book() -> BookMessage {
        let mut m = BookMessage {
            figi: figi("BBG000000123"),
            gateway_seq: 42,
            gateway_ts: 1_700_000_000_000_000_000,
            bid_count: 2,
            ask_count: 1,
            ..Default::default()
        };
        m.bids[0] = marketdata_types::BookLevel {
            price: 100.5,
            qty: 1.0,
            orders: 3,
        };
        m.bids[1] = marketdata_types::BookLevel {
            price: 100.0,
            qty: 2.5,
            orders: 7,
        };
        m.asks[0] = marketdata_types::BookLevel {
            price: 101.0,
            qty: 0.5,
            orders: 1,
        };
        m
    }

    #[test]
    fn book_to_proto_maps_basic_fields() {
        let m = sample_book();
        let pb = book_to_proto(&m);

        assert_eq!(pb.figi, "BBG000000123");
        assert_eq!(pb.gateway_seq, 42);
        assert_eq!(pb.gateway_ts, 1_700_000_000_000_000_000);
        assert_eq!(pb.bids.len(), 2);
        assert_eq!(pb.asks.len(), 1);
        assert_eq!(pb.bids[0].price, 100.5);
        assert_eq!(pb.bids[0].qty, 1.0);
        assert_eq!(pb.bids[0].orders, 3);
        assert_eq!(pb.asks[0].price, 101.0);
    }

    #[test]
    fn book_to_proto_trims_to_active_levels_only() {
        // bid_count=2 but the bids array has 10 slots — the proto Book
        // must carry only 2.
        let m = sample_book();
        let pb = book_to_proto(&m);
        assert_eq!(pb.bids.len(), 2);
        // The trailing 8 `BookLevel::default()` slots must not reach the wire.
    }

    #[test]
    fn book_to_proto_empty_book_is_valid() {
        let m = BookMessage::default();
        let pb = book_to_proto(&m);
        assert_eq!(pb.figi, "");
        assert!(pb.bids.is_empty());
        assert!(pb.asks.is_empty());
    }
}
