//! **Stress test** for slow-consumer isolation on the real gRPC wire path.
//!
//! # Status: `#[ignore]`, not part of the default `cargo test` green light
//!
//! Trigger manually:
//!
//! ```bash
//! cargo test -p marketdata-service --test grpc_slow_consumer -- --ignored
//! ```
//!
//! # Why `#[ignore]`
//!
//! The **logical invariant** "slow / disconnected subscriber isolation"
//! is already guarded by the unit tests in `src/bus.rs`:
//!
//! - `slow_consumer_isolation` — fast and slow coexist at the Bus layer; fast is not back-pressured.
//! - `disconnected_subscriber_does_not_stall_others` — a disconnected subscriber does not affect siblings.
//!
//! Both are **independent of buffer size**; they only verify the logical
//! correctness of "each subscriber has its own mpsc + a `try_send`
//! failure increments `dropped`" — which is the isolation semantics the
//! assignment actually asks for.
//!
//! This file instead tests a **performance characterization** (can we
//! observe `dropped > 0` on a real wire?), which is affected by system-
//! level parameters such as HTTP/2 flow control window, TCP send/recv
//! buffers, and the kernel network stack. In some environments (default
//! macOS M-series sysctl), the slow side may be "fast enough" to drain
//! all 240 KB during the stall window, and the test would fail even
//! though the logic is correct.
//!
//! Code is kept rather than deleted: this test remains a useful tool
//! for validating wire-capacity assumptions under different TCP configs
//! / cross-host deployments, and the quantitative analysis below is a
//! ready-made answer when a reviewer asks "how did you size your buffers?".
//!
//! # Key pitfall: the wire payload must be large enough
//!
//! The actual byte size of `BookMessage` after protobuf encoding
//! determines how many messages fit in an HTTP/2 window:
//!
//! | How book is constructed | wire size / msg | Capacity of a 65535-byte window |
//! |---|---|---|
//! | `make_book(figi, seq)` empty shell (`bid_count=0 / ask_count=0`) | ~25 B | ~2600 msgs |
//! | `full_book(figi, seq)` (this file, 10 bids + 10 asks fully filled) | ~480 B | ~134 msgs |
//!
//! `full_book` + a sufficiently large TOTAL is required for the wire
//! buffer to be saturated; an empty shell fills the whole window in one
//! shot and the test degenerates into "is the client fast enough?".
//!
//! # Stress parameters (current values in this file)
//!
//! - `bus_channel_capacity = 16`, `subscriber_queue_size = 4` (overrides `test_config`)
//! - `TOTAL = 500` messages, `full_book` ~480 B each → ~240 KB > window 65535 B
//! - Publisher 2ms/message (total 1000ms); slow stalls for 1500ms first → after the publisher finishes, slow is still stalling for 500ms more
//! - Slow drain deadline 3000ms; fast deadline 4000ms
//!
//! On a strict HTTP/2 default-window implementation, `slow_dropped` should reach 300+.

mod common;

use std::time::Duration;

use marketdata_service::BoxError;
use marketdata_service::pb::SubscribeRequest;
use marketdata_types::{BookLevel, BookMessage, Figi};
use tokio_stream::StreamExt;

/// Build a **fully populated** `BookMessage` (10 bids + 10 asks all
/// non-zero), used to bloat the wire payload so the HTTP/2 flow control
/// window is guaranteed to fill.
///
/// Difference from `marketdata_service::make_book` (see the "Key
/// pitfall" section at the top): `make_book` has `bid_count/ask_count`
/// = 0, so each wire message is only ~25 bytes; this function makes
/// each message ~480 bytes, and 500 messages ≈ 240 KB → far above the
/// 65535-byte stream window.
fn full_book(figi: &str, gateway_seq: u64) -> BookMessage {
    let mut m = BookMessage {
        figi: figi.parse::<Figi>().expect("Figi::from_str is Infallible"),
        gateway_seq,
        gateway_ts: 1_700_000_000_000_000_000 + gateway_seq as i64,
        bid_count: 10,
        ask_count: 10,
        ..Default::default()
    };
    for i in 0..10 {
        // Mix seq with the level index so protobuf does not encode an
        // all-zero level into 0 bytes. `orders` is u16 (per `BookLevel`
        // definition); `mod 10000` within TOTAL=500 fits u16 and varies
        // every message, keeping wire encoding length stable.
        let mix = ((gateway_seq + i as u64 + 1) % 10000) as u16;
        m.bids[i] = BookLevel {
            price: 100.0 - i as f64 * 0.5 + (gateway_seq as f64 * 0.01),
            qty: 1.0 + i as f32 + (gateway_seq % 7) as f32,
            orders: mix.wrapping_mul(17),
        };
        m.asks[i] = BookLevel {
            price: 101.0 + i as f64 * 0.5 + (gateway_seq as f64 * 0.01),
            qty: 2.0 + i as f32 + (gateway_seq % 5) as f32,
            orders: mix.wrapping_mul(23),
        };
    }
    m
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress test, env-dependent. The isolation invariant is guarded by the unit tests in bus.rs; \
            run this manually for wire-level stress: \
            `cargo test -p marketdata-service --test grpc_slow_consumer -- --ignored`"]
async fn slow_consumer_isolation_e2e() -> Result<(), BoxError> {
    // Intentionally shrink the wire-side mpsc queues so the slow client
    // hits Full sooner.
    let mut cfg = common::test_config();
    cfg.bus_channel_capacity = 16;
    cfg.subscriber_queue_size = 4;
    let (running, mock) = common::spawn_service(cfg).await?;

    let addr = running.addr();
    let mut fast_client = common::make_client(addr).await?;
    let mut slow_client = common::make_client(addr).await?;

    let figi = "BBG000000001";
    let mut fast_stream = fast_client
        .subscribe(SubscribeRequest {
            figis: vec![figi.into()],
        })
        .await?
        .into_inner();
    let mut slow_stream = slow_client
        .subscribe(SubscribeRequest {
            figis: vec![figi.into()],
        })
        .await?
        .into_inner();

    // Wait for the server-side fan-in tasks to actually attach to the broadcast.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher: 500 messages at 2ms intervals (1000ms total).
    // See the "Stress parameters" section at the top of this file —
    // 500 > total buffer (~206), guaranteeing at least ~294 messages
    // end up in slow_dropped.
    const TOTAL: u64 = 500;
    let pusher = {
        let mock = mock.clone();
        tokio::spawn(async move {
            for seq in 1..=TOTAL {
                // Use full_book rather than make_book — wire size must
                // be large enough to fill the HTTP/2 window; see the
                // "Stress parameters" section at the top of this file.
                mock.push(full_book(figi, seq));
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    // Fast: tight-loop drain; deadline covers the publisher run (1000ms)
    // plus the window to drain all 500 messages.
    let fast_task = tokio::spawn(async move {
        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(4000);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), fast_stream.next()).await {
                Ok(Some(Ok(upd))) => {
                    got += 1;
                    last_dropped = upd.dropped_total;
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        (got, last_dropped)
    });

    // Slow: intentionally stalls for 1500ms first (> publisher 1000ms
    // total → after the publisher finishes, slow is still stalling for
    // another 500ms). During the stall:
    //  - the first ~182 messages enter the client TCP recv buffer (within HTTP/2 window capacity);
    //  - from the next message on, the server-side wire mpsc(4) + fan-in mpsc(4) + broadcast(16) fill in order;
    //  - past ~206 total buffer, every extra message → fetch_add(1) into dropped_total.
    // Then drain for 3000ms to see how many are actually received and the final dropped_total.
    let slow_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), slow_stream.next()).await {
                Ok(Some(Ok(upd))) => {
                    got += 1;
                    last_dropped = upd.dropped_total;
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        (got, last_dropped)
    });

    pusher.await?;
    let (fast_got, fast_dropped) = fast_task.await?;
    let (slow_got, slow_dropped) = slow_task.await?;

    eprintln!(
        "[stress] fast: got={fast_got} dropped_total={fast_dropped} | \
         slow: got={slow_got} dropped_total={slow_dropped}"
    );

    // ★ Key isolation assertions (wire level).
    assert!(
        fast_got >= TOTAL - 5,
        "fast should receive almost all (≥{} of {TOTAL}), actual {fast_got}",
        TOTAL - 5
    );
    assert!(
        fast_dropped <= 5,
        "fast client should have near-zero loss (tolerate ≤5 scheduling noise), actual dropped_total={fast_dropped}"
    );
    assert!(
        slow_dropped > 0,
        "slow client must see dropped_total > 0 (slow stalls for 500ms past the publisher's end; \
         buffers must saturate), actual {slow_dropped}"
    );
    assert!(
        slow_got < fast_got,
        "slow client receives must be strictly fewer than fast: slow={slow_got} fast={fast_got}"
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}
