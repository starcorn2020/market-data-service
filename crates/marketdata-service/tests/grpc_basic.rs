//! gRPC 基础路径冒烟测试。
//!
//! 守 README §2 / §3 的"最新快照" + "clearly-defined no data yet" 契约。

mod common;

use std::time::Duration;

use marketdata_service::BoxError;
use marketdata_service::make_book;
use marketdata_service::pb::{GetSnapshotRequest, SubscribeRequest, snapshot_response::Result as SnapResult};
use tokio_stream::StreamExt;

/// **T4（DEV_PROCESS §5.1）**：守 README §3 "clearly-defined no data yet"。
///
/// 启动 service 后 **不推任何数据**，立刻 GetSnapshot → 必须返 `NotYet`。
/// 然后推一笔 → 再 GetSnapshot → 必须返 `Found` 且 seq 匹配。
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

/// 守 README §2 "per-instrument latest-book snapshot"：
/// 同 FIGI 连续推多笔递增 seq 后，GetSnapshot 必须返回**最大** seq。
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
