//! Smoke tests for the basic gRPC paths.
//!
//! End-to-end guards on a real tonic server + client + `MockUpstream`:
//!
//! - The **"per-instrument latest-book snapshot"** contract (assignment §2):
//!   `not_yet_then_found` / `snapshot_returns_latest_seq`.
//! - The **"clearly-defined no data yet"** contract (assignment §3): same as above.
//! - **`GetSnapshots` batch shape**: `get_snapshots_batch_mixed_found_and_not_yet`
//!   demonstrates that one request can return a mix of `Found` and `NotYet`
//!   entries in request order, with each entry echoing its `figi`.
//! - **`Subscribe` streaming** + figi length validation (wire-side defensive layer in `grpc.rs`).

mod common;

use std::time::Duration;

use marketdata_service::BoxError;
use marketdata_service::make_book;
use marketdata_service::pb::{
    GetSnapshotsRequest, SubscribeRequest, snapshot_entry::Result as SnapResult,
};
use tokio_stream::StreamExt;

/// Convenience: send a single-FIGI GetSnapshots and return the sole entry's
/// `result`. Most tests below only care about one FIGI at a time; this
/// keeps their assertions readable.
async fn snapshot_one(
    client: &mut marketdata_service::pb::market_data_client::MarketDataClient<
        tonic::transport::Channel,
    >,
    figi: &str,
) -> Result<Option<SnapResult>, BoxError> {
    let mut resp = client
        .get_snapshots(GetSnapshotsRequest {
            figis: vec![figi.into()],
        })
        .await?
        .into_inner();
    assert_eq!(
        resp.entries.len(),
        1,
        "single-FIGI request must return exactly one entry, got {}",
        resp.entries.len()
    );
    let entry = resp.entries.remove(0);
    assert_eq!(entry.figi, figi, "entry must echo the requested figi");
    Ok(entry.result)
}

/// Guards the "clearly-defined no data yet" contract: after starting the
/// service with **no data pushed**, an immediate GetSnapshots must return
/// `NotYet`. After pushing one message → another GetSnapshots must return
/// `Found` with a matching seq.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_yet_then_found() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    // ① No data pushed → NotYet.
    match snapshot_one(&mut client, "BBG000000001").await? {
        Some(SnapResult::NotYet(_)) => {}
        other => panic!("expected NotYet before any push, got {other:?}"),
    }

    // ② Push one message and wait until ingest writes it to the snapshot table.
    mock.push(make_book("BBG000000001", 42));
    common::wait_for_snapshot_len(&running, 1, Duration::from_millis(500)).await?;

    // ③ Query again → Found(seq=42).
    match snapshot_one(&mut client, "BBG000000001").await? {
        Some(SnapResult::Found(book)) => {
            assert_eq!(
                book.gateway_seq, 42,
                "GetSnapshots must return the most recently written seq"
            );
            assert_eq!(book.figi, "BBG000000001");
        }
        other => panic!("expected Found after push, got {other:?}"),
    }

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Guards the "per-instrument latest-book snapshot" contract: after
/// pushing several messages with increasing seq to the same FIGI,
/// GetSnapshots must return the **largest** seq.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_returns_latest_seq() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    for seq in 1..=10u64 {
        mock.push(make_book("BBG000000001", seq));
    }
    common::wait_for_snapshot_len(&running, 1, Duration::from_millis(500)).await?;

    match snapshot_one(&mut client, "BBG000000001").await? {
        Some(SnapResult::Found(book)) => {
            assert_eq!(book.gateway_seq, 10, "snapshot should retain seq=10 (latest)");
        }
        other => panic!("expected Found, got {other:?}"),
    }

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Guards the new batch shape: one `GetSnapshots` call covering 3 FIGIs
/// where only one has data must return 3 entries in request order, with
/// the populated one as `Found` and the other two as `NotYet`. Verifies:
///
/// 1. **Per-entry independence**: the populated FIGI does not bleed into
///    the others (`NotYet` is per FIGI, not for the whole request).
/// 2. **Order preservation**: response entries are aligned with request order.
/// 3. **Echo**: each entry's `figi` matches the corresponding request entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_snapshots_batch_mixed_found_and_not_yet() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    // Populate only the middle FIGI.
    mock.push(make_book("BBG000000002", 7));
    common::wait_for_snapshot_len(&running, 1, Duration::from_millis(500)).await?;

    let resp = client
        .get_snapshots(GetSnapshotsRequest {
            figis: vec![
                "BBG000000001".into(),
                "BBG000000002".into(),
                "BBG000000003".into(),
            ],
        })
        .await?
        .into_inner();

    assert_eq!(resp.entries.len(), 3, "must return one entry per requested FIGI");

    // Entry 0: NotYet for BBG000000001
    assert_eq!(resp.entries[0].figi, "BBG000000001");
    assert!(matches!(resp.entries[0].result, Some(SnapResult::NotYet(_))));

    // Entry 1: Found for BBG000000002 with seq=7
    assert_eq!(resp.entries[1].figi, "BBG000000002");
    match &resp.entries[1].result {
        Some(SnapResult::Found(book)) => {
            assert_eq!(book.gateway_seq, 7);
            assert_eq!(book.figi, "BBG000000002");
        }
        other => panic!("expected Found for BBG000000002, got {other:?}"),
    }

    // Entry 2: NotYet for BBG000000003
    assert_eq!(resp.entries[2].figi, "BBG000000003");
    assert!(matches!(resp.entries[2].result, Some(SnapResult::NotYet(_))));

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// An empty FIGI list to GetSnapshots must return `InvalidArgument`,
/// mirroring `subscribe`'s empty-list rejection (explicit check in `grpc.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_snapshots_empty_figis_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .get_snapshots(GetSnapshotsRequest { figis: vec![] })
        .await
        .expect_err("empty FIGI list must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Guards the Subscribe path: after subscribing and pushing data, the
/// client should receive one BookUpdate with the correct seq.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_streams_pushed_updates() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let mut stream = client
        .subscribe(SubscribeRequest {
            figis: vec!["BBG000000001".into()],
        })
        .await?
        .into_inner();

    // Wait for server-side subscribe to complete (bus.subscribe spawns
    // fan-in tasks internally).
    tokio::time::sleep(Duration::from_millis(80)).await;

    mock.push(make_book("BBG000000001", 7));

    let upd = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await?
        .ok_or_else(|| -> BoxError { "stream closed without item".into() })??;
    let book = upd.book.expect("BookUpdate.book required");
    assert_eq!(book.gateway_seq, 7);
    assert_eq!(book.figi, "BBG000000001");
    assert_eq!(upd.dropped_total, 0, "first message must not have any dropped");

    drop(stream);
    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// An empty FIGI list to Subscribe must return `InvalidArgument`
/// (explicit check in grpc.rs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_empty_figis_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .subscribe(SubscribeRequest { figis: vec![] })
        .await
        .expect_err("empty FIGI list must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Guards the figi length validation in `grpc.rs::get_snapshots`:
/// `Figi::from_str` is itself Infallible (silently truncates); the wire
/// layer proactively rejects overly long figis before parsing, avoiding
/// the confusing UX where a client sending `"BBG_LONG_FIGI"` is sliced
/// down to 12 bytes and almost always returns `NotYet`. Any oversized
/// entry in the batch rejects the entire request (all-or-nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_snapshots_too_long_figi_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .get_snapshots(GetSnapshotsRequest {
            figis: vec![
                "BBG000000001".into(),            // valid 12 bytes
                "BBG_TOO_LONG_FIGI_13PLUS".into() // 24 bytes, triggers rejection
            ],
        })
        .await
        .expect_err("a request containing an overly long figi must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("too long"),
        "error message should explicitly say \"too long\", actual {:?}",
        err.message()
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Same as above, guards figi length validation on the `subscribe`
/// path. Any one overly long entry rejects the entire subscribe
/// (all-or-nothing semantics).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_too_long_figi_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .subscribe(SubscribeRequest {
            figis: vec![
                "BBG000000001".into(), // valid 12 bytes
                "BBG_TOO_LONG_FIGI_13PLUS".into(), // 24 bytes, triggers rejection
            ],
        })
        .await
        .expect_err("a subscribe containing an overly long figi must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("too long"),
        "error message should explicitly say \"too long\", actual {:?}",
        err.message()
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}
