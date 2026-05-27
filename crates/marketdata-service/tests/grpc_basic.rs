//! gRPC 基础路径冒烟测试。
//!
//! 在真实 tonic server + client + `MockUpstream` 上端到端守:
//!
//! - **"per-instrument latest-book snapshot"** 契约 (题面 §2):
//!   `not_yet_then_found` / `snapshot_returns_latest_seq`。
//! - **"clearly-defined no data yet"** 契约 (题面 §3):同上。
//! - **`Subscribe` 推流** + figi 长度校验 (`grpc.rs` wire 防御层)。

mod common;

use std::time::Duration;

use marketdata_service::BoxError;
use marketdata_service::make_book;
use marketdata_service::pb::{GetSnapshotRequest, SubscribeRequest, snapshot_response::Result as SnapResult};
use tokio_stream::StreamExt;

/// 守 "clearly-defined no data yet" 契约:启动 service 后**不推任何数据**,
/// 立刻 GetSnapshot → 必须返 `NotYet`。然后推一笔 → 再 GetSnapshot → 必须返
/// `Found` 且 seq 匹配。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_yet_then_found() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    // ① 未推数据 → NotYet。
    let resp = client
        .get_snapshot(GetSnapshotRequest {
            figi: "BBG000000001".into(),
        })
        .await?
        .into_inner();
    match resp.result {
        Some(SnapResult::NotYet(_)) => {} // ✓
        other => panic!("expected NotYet before any push, got {other:?}"),
    }

    // ② 推一笔，等 ingest 把它放进 snapshot 表。
    mock.push(make_book("BBG000000001", 42));
    common::wait_for_snapshot_len(&running, 1, Duration::from_millis(500)).await?;

    // ③ 再 query → Found(seq=42)。
    let resp = client
        .get_snapshot(GetSnapshotRequest {
            figi: "BBG000000001".into(),
        })
        .await?
        .into_inner();
    match resp.result {
        Some(SnapResult::Found(book)) => {
            assert_eq!(book.gateway_seq, 42, "GetSnapshot 必返回最新写入的 seq");
            assert_eq!(book.figi, "BBG000000001");
        }
        other => panic!("expected Found after push, got {other:?}"),
    }

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// 守 "per-instrument latest-book snapshot" 契约:同 FIGI 连续推多笔递增 seq
/// 后, GetSnapshot 必须返回**最大** seq。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_returns_latest_seq() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    for seq in 1..=10u64 {
        mock.push(make_book("BBG000000001", seq));
    }
    common::wait_for_snapshot_len(&running, 1, Duration::from_millis(500)).await?;

    let resp = client
        .get_snapshot(GetSnapshotRequest {
            figi: "BBG000000001".into(),
        })
        .await?
        .into_inner();
    match resp.result {
        Some(SnapResult::Found(book)) => {
            assert_eq!(book.gateway_seq, 10, "snapshot 应保留 seq=10（最新）");
        }
        other => panic!("expected Found, got {other:?}"),
    }

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// 守 Subscribe 路径：订阅后推数据，client 应该收到一笔含正确 seq 的 BookUpdate。
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

    // 等 server 端 subscribe 完成（bus.subscribe 内部 spawn fan-in tasks）。
    tokio::time::sleep(Duration::from_millis(80)).await;

    mock.push(make_book("BBG000000001", 7));

    let upd = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await?
        .ok_or_else(|| -> BoxError { "stream closed without item".into() })??;
    let book = upd.book.expect("BookUpdate.book required");
    assert_eq!(book.gateway_seq, 7);
    assert_eq!(book.figi, "BBG000000001");
    assert_eq!(upd.dropped_total, 0, "首笔不应有 dropped");

    drop(stream);
    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// Subscribe 空 FIGI 列表必须返回 `InvalidArgument`（grpc.rs 内显式 check）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_empty_figis_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .subscribe(SubscribeRequest { figis: vec![] })
        .await
        .expect_err("空 FIGI 必拒");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// 守 `grpc.rs::get_snapshot` 的 figi 长度校验:`Figi::from_str` 本身是
/// Infallible (silently 截断), wire 层在 parse 前主动拒绝过长 figi, 避免
/// 客户端送 `"BBG_LONG_FIGI"` 被切成前 12 byte 后大概率返 `NotYet` 的诡异 UX。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_snapshot_too_long_figi_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .get_snapshot(GetSnapshotRequest {
            figi: "BBG_TOO_LONG_FIGI_13PLUS".into(), // 24 bytes > 12
        })
        .await
        .expect_err("过长 figi 必拒");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("too long"),
        "error message 应明确说「too long」,实际 {:?}",
        err.message()
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}

/// 同上,守 `subscribe` 路径上的 figi 长度校验。任一条过长即整个 subscribe 拒绝
/// (all-or-nothing 语义)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_too_long_figi_rejected() -> Result<(), BoxError> {
    let (running, mock) = common::spawn_default_service().await?;
    let mut client = common::make_client(running.addr()).await?;

    let err = client
        .subscribe(SubscribeRequest {
            figis: vec![
                "BBG000000001".into(), // 合法 12 byte
                "BBG_TOO_LONG_FIGI_13PLUS".into(), // 24 bytes,触发拒绝
            ],
        })
        .await
        .expect_err("含过长 figi 的 subscribe 必拒");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("too long"),
        "error message 应明确说「too long」,实际 {:?}",
        err.message()
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}
