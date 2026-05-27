# 開發進度交接 (DEV_PROCESS)

> **用途**：給新 chat 視窗接續用的 handover doc。讀完本檔 + `AI_DEV_GUILDELINE.md` 就能無痛接手。
>
> **檔案職責劃分**：
> - `AI_DEV_GUILDELINE.md` → 設計憲法（不變量、Non-goals、最終定案的架構）。**只讀**。
> - `crates/feed-sim/src/Congfig.md`（typo 待修）→ feed-sim 參數總表。**只讀**。
> - `DEV_PROCESS.md`（本檔）→ 動態進度、過程心得、下一步 todo。**每完成一個 phase 更新**。

---

## 0. 當前狀態速覽

| 項目 | 狀態 |
|---|---|
| **Phase** | Phase 1 ✅ / Phase 2 ✅ / Phase 3 ⏳ 待開始 |
| **最後一次 `cargo test --workspace`** | 38/38 全綠（service 14 + feed-sim 19 + types 5），無警告 |
| **最後一次 `cargo build --release --workspace`** | 0 警告 0 錯誤 |
| **最後一次 demo** | server: `MDS_LISTEN=0.0.0.0:50051 SIM_INSTRUMENTS=10 SIM_RATE_HZ=200 cargo run -p marketdata-service` → `[server] listening on 0.0.0.0:50051`；client: `cargo run --bin client` → `Found(seq=5921, bids=5, asks=5)` + 3s 推流 178 筆 / `dropped_total=0` |
| **Rust toolchain** | rustc 1.95.0，edition 2024，resolver 3 |
| **Workspace deps** | `anyhow` / `tokio (full)` / `dashmap 6` / `tonic 0.12` / `prost 0.13` / `tokio-stream 0.1` / `tonic-build 0.12` (build) / `protoc-bin-vendored 3` (build) |

---

## 1. 已拍板的設計決策（D1–D7）

新視窗如果想重新討論這些，先回到 `AI_DEV_GUILDELINE.md` 看是否衝突；無衝突再考慮翻案。

| # | 主題 | 拍板選擇 | 體現位置 |
|---|---|---|---|
| **D1** | 依賴管理 | 全部走 `[workspace.dependencies]`（包含 `tonic-build` / `protoc-bin-vendored`） | `Cargo.toml` |
| **D2** | `.proto` 位置 | **根目錄** `proto/marketdata.proto`（公開合約） | `proto/marketdata.proto` ✅ |
| **D3** | `Upstream` trait | **立刻抽 + 泛型靜態分派**（拒絕 `Box<dyn>`） | `src/upstream/mod.rs`、`ingest::spawn<U>` |
| **D4** | `Bus::subscribe` 回傳 | **mpsc**（Bus 內部 fan-in 合併 N 個 broadcast） | `src/bus.rs` |
| **D5** | `ServiceConfig` 對 feed-sim | **嚴格隔離**：自家 `UpstreamConfig` + `From` 映射 | `src/config.rs` |
| **D6** | Runtime 主導權 | **外推**：`Service::new` 假設身處 tokio 上下文 | `src/main.rs` 持有 `#[tokio::main]` |
| **D7** | Tracing | **不引入**（Phase 1/2 用 `eprintln!`；Phase 4 視 deliverable 需要再評） | `ingest.rs` / `lib.rs` / `grpc.rs` |
| **D8** | `protoc` 取得 | **`protoc-bin-vendored` 在 `build.rs` 注入 PROTOC env**；reviewer 不需手動裝 protobuf-compiler | `build.rs` |

---

## 2. Phase 1 + Phase 2 已交付的程式碼

```
market-data-service/
├── proto/
│   └── marketdata.proto             # ✅ Phase 2 新建：MarketData service + Book/BookUpdate
└── crates/marketdata-service/
    ├── Cargo.toml                   # 已加 tonic / prost / tokio-stream / [[bin]] client / default-run
    ├── build.rs                     # ✅ Phase 2 新建：tonic_build + 注入 vendored protoc
    └── src/
        ├── lib.rs                   # Service::run 改用 tokio::select!（ingest / tonic / ctrl_c 合流）
        ├── main.rs                  # #[tokio::main] 薄包裝
        ├── config.rs                # 增 listen_addr 字段 + MDS_LISTEN env
        ├── snapshot.rs              # DashMap<Figi, BookMessage>，put/get
        ├── bus.rs                   # per-FIGI broadcast + Subscription{ rx, dropped }
        ├── grpc.rs                  # ✅ Phase 2 新建：MarketData trait impl + BookMessage↔proto 映射
        ├── ingest.rs                # std::thread loop，IngestHandle{ stop / stop_token / join }
        ├── upstream/
        │   ├── mod.rs               # trait Upstream
        │   └── feed_sim.rs          # FeedSimUpstream adapter（唯一 use feed_sim::* 處）
        └── bin/
            └── client.rs            # ✅ Phase 2 新建：sample client，demo GetSnapshot + Subscribe
```

### 2.1 對外 API 形狀（穩定）

```rust
pub struct Service { /* ... */ }
impl Service {
    pub fn new(cfg: ServiceConfig) -> anyhow::Result<Self>;     // 假設身處 tokio runtime
    pub async fn run(self) -> anyhow::Result<()>;                // 同時跑 ingest + tonic + ctrl_c
    pub fn snapshot_len(&self) -> usize;
}

// proto 生成型別重新導出，給 src/bin/client.rs 與整合測試使用：
pub mod pb { /* GetSnapshotRequest / SnapshotResponse / SubscribeRequest / BookUpdate / Book / Level / MarketDataClient */ }
```

### 2.2 內部不變量（編譯期 / runtime 保證）

| 不變量 | 守護點 |
|---|---|
| **I1** ingest 永不被下游阻塞 | `Bus::publish` 是 `broadcast::Sender::send`（容量滿丟最舊，非阻塞）；`Snapshot::put` 是 DashMap shard write（極短）。 |
| **I2** 慢/斷的訂閱者不影響其他訂閱者 | 每訂閱者獨立 fan-in task；`try_send(Full)` 累進 `dropped_total` 不阻塞。 |
| **I3** feed-sim 只能透過 `Upstream` trait 用 | `upstream/feed_sim.rs` 是整個 crate 唯一 `use feed_sim::*` 的文件。 |
| **I4** 對外不洩漏 `feed_sim::*` | `From<UpstreamConfig> for SubscriberConfig` 是密封點；`Service::new` 簽名只見 `ServiceConfig`。 |
| **順序** put 先於 publish | `ingest::ingest_loop` 內固定先 `snapshot.put(book)` 後 `bus.publish(book)`。訂閱者收到推播時 `GetSnapshot` 一定能讀到至少同筆。 |

### 2.3 環境變數（Phase 1 主場景）

| Env | 作用 | 預設 |
|---|---|---|
| `SIM_INSTRUMENTS` | 模擬 FIGI 數 | 100 |
| `SIM_RATE_HZ` | 總速率 msg/s | 1000 |
| `SIM_MAX_MESSAGES` | 訊息上限（觸發 EOF） | 無限 |
| `SIM_SEED` / `SIM_START_SEQ` / `SIM_BUFFER_SIZE` / `SIM_DEPTH` / `SIM_PACING` | 同 feed-sim README | 各自預設 |
| `MDS_POLL_INTERVAL_MS` | ingest `wait()` poll 間隔 | 50 |
| `MDS_BUS_CAPACITY` | 每 FIGI broadcast 容量 | 1024 |
| `MDS_SUBSCRIBER_QUEUE` | 每訂閱者 mpsc 容量 | 1024 |
| `MDS_PROGRESS_EVERY` | ingest 進度 log 每 N 筆 | 100 |
| `MDS_LISTEN` | gRPC server 監聽地址 | `0.0.0.0:50051` |
| `MDS_CLIENT_TARGET` | client 連線 endpoint | `http://127.0.0.1:50051` |
| `MDS_CLIENT_FIGI` | client `GetSnapshot` 查詢的 FIGI | `BBG000000001` |
| `MDS_CLIENT_FIGIS` | client `Subscribe` 的 FIGI 列表（逗號分隔） | 同上 |
| `MDS_CLIENT_SECS` | client `Subscribe` 持續秒數 | 3 |

---

## 3. Phase 1 過程中的關鍵心得（避坑備忘）

### 3.1 `IngestHandle::stop` 與 `::join` 必須語義分離

**第一版錯誤寫法**：`join()` 內部自動 `stop()`，意圖"等就要關"。後果：測試 / 真實場景下調用者想等上游自然 EOF 也會立刻被踢停，ingest 一筆都跑不到就退出。

**修正後契約**：

- `stop()` → 顯式非阻塞信號，可重複調用。
- `join()` → 純等線程結束，**不**主動發 stop。無 `max_messages` cap 時會永遠阻塞（有意設計）。
- `Drop` → 強制 `stop + join`，防止 thread leak。

Phase 2 加 ctrl-c handler 時保留此契約：handler 內 `handle.stop(); handle.join()`。

### 3.2 `Bus::publish` 走 `get-only` 路徑

`publish` 內部 `senders.get(&figi)`，**不**用 `entry().or_insert_with`。無訂閱者時零分配，是 I1 的核心。代價：訂閱前已發布的訊息不會補（合理，sub 是 from-now 語義）。

### 3.3 `dropped_total` 計數器共用設計

`Subscription::dropped_counter()` 暴露 `Arc<AtomicU64>`，Phase 2 grpc handler 在 wire `try_send` Full 時也 `fetch_add` 同一個 counter。這樣 client 看到的 `dropped_total` 涵蓋"fan-in 階段丟" + "wire 階段丟"，符合 GUIDELINE §4.3.3 累積值語義。

### 3.4 fan-in 採用 N task 而非 `select_all`

每個 broadcast::Receiver 一個 tokio task，`mpsc::Sender` clone 給每個 task。實作最簡單。Phase 3 壓測 100+ 訂閱者後若觀察到 task 過多，再評估換 `tokio_stream::StreamMap`。

### 3.5 `FeedSimUpstream: !Sync`

`FeedSubscriber` 內部持有 `std::sync::mpsc::Receiver`（`!Sync`），所以 `FeedSimUpstream` 也是 `!Sync`。**這是 feature 不是 bug**——契合 GUIDELINE §3.5 「整個 service 只能有一個 ingest 點」。`Upstream` trait 只 require `Send`（不 require `Sync`），編譯期確保 ingest 單線程獨佔。

### 3.6 `is_multiple_of` 是 stable

Rust 1.95 + edition 2024 環境下 `u64::is_multiple_of(n)` 已 stable。`ingest.rs` 用了；若 toolchain 降版要改回 `% n == 0`。

---

## 4. Phase 2：gRPC 接通 ✅ 已完成

**目標完成定義**：`cargo run --bin client` 同時 demo `GetSnapshot(figi)` 與 `Subscribe([figi…])`，跨主機（LAN）可連通。**已通過**。

### 4.0 實際交付差異備忘（與原計畫對照）

| 計畫 | 實際 | 理由 |
|---|---|---|
| 要求 reviewer 自行裝 `protoc` | **改用 `protoc-bin-vendored`**，`build.rs` 注入 PROTOC env | 交付體驗：zero 系統依賴，本機/reviewer 不用 `scoop install protobuf`；同時保留 README 路線備用 |
| `Service` 持 `cfg: ServiceConfig` | **只持 `listen_addr` + `subscriber_queue_size`** | 兩個字段就夠 `run()` 使用，避免整份 cfg 駐留 |
| `ingest_join` 用 `spawn_blocking(handle.join)` | 同左，**並暴露 `IngestHandle::stop_token() -> Arc<AtomicBool>`** | `tokio::select!` 拿走 `IngestHandle` 之後仍能從 select 的另一臂觸發 stop |
| `proto/marketdata.proto` 中 `dropped_total` 註解抄自 GUIDELINE | 同左 + tonic-build 自動轉成 doc-comment | 生成代碼帶 `///` 註解，IDE hover 即可看到 |

### 4.1 步驟（依序）

1. **建立 `proto/marketdata.proto`**：照 `AI_DEV_GUILDELINE.md` §4.3.1 抄。放**根目錄** `proto/`（D2）。
2. **加 workspace deps**：`tonic = "0.12"` / `prost = "0.13"` / `tokio-stream = "0.1"`；`[workspace.dependencies]` 加 `tonic-build = "0.12"`。
3. **建立 `crates/marketdata-service/build.rs`**：`tonic_build::compile_protos("../../proto/marketdata.proto")`（注意相對路徑從 service crate 出發）。
4. **service Cargo.toml** 加 `tonic` / `prost` / `tokio-stream` dependency + `tonic-build` build-dependency。
5. **新增 `src/grpc.rs`**：
   - 實作 `MarketData` service trait（unary `GetSnapshot` + server-streaming `Subscribe`）。
   - `GetSnapshot`：`service.snapshot().get(&figi)` → 映射 `Some(book)` → `SnapshotResponse::Found(book.into_proto())`、`None` → `NotYet`。
   - `Subscribe`：**照抄 GUIDELINE §4.3.4 樣板**。重點：
     - `out_tx` 走 bounded `mpsc::channel(cfg.subscriber_queue_size)`。
     - 取 `Subscription::dropped_counter()` 給 wire 端 task 共享 `Arc<AtomicU64>`。
     - `try_send` 時 `Full` 走 `dropped.fetch_add(1)`、`Closed` 走 break。**嚴禁** `send().await`。
   - 補 `BookMessage` ↔ proto `Book` 的轉換（純機械映射）。
6. **修改 `src/lib.rs`**：
   - `Service` 新增 `cfg: ServiceConfig` 與 `listen_addr: SocketAddr` 字段（從 cfg 拿）。
   - `Service::run` 改成 `tokio::select!` 合流 `ingest_join + tonic_serve + ctrl_c`：

     ```rust
     tokio::select! {
         _ = ctrl_c => { ingest_handle.stop(); /* serve will drop */ }
         res = tonic::transport::Server::builder().add_service(...).serve(addr) => { res? }
         stats = spawn_blocking(move || ingest_handle.join()) => { /* feed-sim EOF 場景 */ }
     }
     ```

   - 新增 `ServiceConfig::listen_addr: SocketAddr`（預設 `0.0.0.0:50051`，env `MDS_LISTEN`）。
7. **新增 `src/bin/client.rs`**（README §6 demo）：
   - 先 `GetSnapshot(figi)` 印一行。
   - 再 `Subscribe([figi…])` 跑 N 秒 print 收到數量與最終 `dropped_total`。
   - 用 `tonic::transport::Channel::from_static("http://127.0.0.1:50051").connect().await`。
   - 加 `MDS_CLIENT_TARGET` env 給跨主機測試用。

### 4.2 Phase 2 踩過的坑（保留給後續 phase 警惕）

| 坑 | 對策 | 狀態 |
|---|---|---|
| `protoc` 未安裝導致 `build.rs` 失敗 | **`protoc-bin-vendored` 自動 PROTOC env**，zero 系統依賴 | ✅ 消除 |
| `tonic-build` 把生成檔放 `OUT_DIR`，IDE 找不到 | `src/grpc.rs` 內 `pub mod pb { tonic::include_proto!("marketdata.v1"); }`；`cargo check` 觸發生成 | ✅ |
| Stream handler 不小心 `tx.send().await` | 嚴格 `try_send` + match 三分支；code review grep `\.send\(.*\)\.await` 零命中 | ✅ |
| `Subscription.next().await` 借 `&mut self` 跟 `&Subscription` 衝突 | handler 直接持 `Subscription` 值並 `&mut`，不再 clone | ✅ |
| client 跨主機連不通 | `ServiceConfig::default().listen_addr = 0.0.0.0:50051`；`MDS_LISTEN` env 可覆蓋 | ✅ |
| 兩個 `[[bin]]` 讓 `cargo run -p` 不知跑哪個 | `default-run = "marketdata-service"` | ✅ |
| 生成的 `pb` 模組觸發 `#![warn(missing_docs)]` | `pub mod pb { #![allow(missing_docs)] ... }` 局部豁免 | ✅ |

### 4.3 Phase 2 退出條件

- [x] `cargo build --workspace` 無警告
- [x] `cargo build --release --workspace` 無警告
- [x] `cargo test --workspace` 全綠（38/38，含新增 3 個 grpc 編解碼測試）
- [x] `cargo run -p marketdata-service` 監聽 `0.0.0.0:50051`，stderr 看到 `[server] listening on 0.0.0.0:50051`
- [x] `cargo run --bin client` 跑通：`Found(seq=5921, bids=5, asks=5)` + 3s 推流 178 筆 / `dropped_total=0`

---

## 5. Phase 3：不變量驗證測試（評分重頭戲）

**目標完成定義**：reviewer 看到測試名稱就信服 I1 / I2。

### 5.1 必寫測試清單

| 測試 | 守的不變量 | 骨架 |
|---|---|---|
| `slow_consumer_isolation` | I2 | 起 2 個 Subscribe；A 正常 recv，B `sleep(50ms)` between recv；驗證 A 吞吐量穩定、A `dropped=0`、B `dropped>0`。 |
| `disconnect_does_not_stall_ingest` | I1 | 起 1 訂閱者→主動 drop client→`Service::snapshot_len()` 持續成長 / `ingest_stats.received` 持續成長。 |
| `snapshot_returns_latest` | README §2 | 連續 put 100 筆同 FIGI，遞增 seq；`GetSnapshot` 回傳的 `gateway_seq` 必須是最大值。 |
| `not_yet_response` | README §3 | 對沒收過的 FIGI 查詢，必須回 `NotYet`。 |
| `gap_counter_increments_on_skipped_seq` | §4.2 gap 偵測 | 用 mock Upstream 注入 `seq=1, 2, 5`，驗證 `IngestStats::gaps == 1`。 |

### 5.2 為什麼 Phase 3 是評分重頭戲

GUIDELINE §10 對照表：reviewer 看 README 第 4 條「slow / disconnected subscriber must not affect the others」時，會直接搜你的測試名稱。沒有對應測試 = 不變量無證據。

---

## 6. Phase 4：交付打磨

### 6.1 寫 `crates/marketdata-service/README.md`

照 `AI_DEV_GUILDELINE.md` §14.1–14.3。**不要複製** `AI_DEV_GUILDELINE.md`。

關鍵段落：

- **設計決策** 8 條（§14.1 表格）。
- **Build / Test / Run** 三段，含 `protoc` 安裝。
- **跨主機 demo**：`MDS_LISTEN=0.0.0.0:50051` + `MDS_CLIENT_TARGET=http://<lan-ip>:50051` 一行命令。
- **Future work**（§14.3 對齊 Non-goals）。

### 6.2 文件 / 命名清理

- [ ] `crates/feed-sim/src/Congfig.md` → `Config.md`（typo）；同步更新 `AI_DEV_GUILDELINE.md` §1 / §3 引用。
- [ ] Phase 3 結束後考慮是否加 tracing（D7 重新評估點）。若加，**只在 main.rs init**，library 內部仍用 `tracing::info!` macros（switch cost 極低）。

### 6.3 最終 sanity check

- [ ] `cargo build --release --workspace` 零警告。
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`（如裝了 clippy）。
- [ ] zip 排除 `target/` / `.git/`（如有）。
- [ ] README 確認 reviewer 不需要任何「先問你」的步驟。

---

## 7. 給新視窗 AI 的接手 prompt 範本

```
我剛開新視窗。請按以下順序讀檔再回應：

1. 讀 AI_DEV_GUILDELINE.md（設計憲法，最高優先級）
2. 讀 DEV_PROCESS.md（本檔，知道現在進度）
3. 讀 crates/feed-sim/src/Congfig.md（feed-sim 參數表）

我接下來想推進 [Phase 2 / Phase 3 / Phase 4]。
先告訴我你打算動哪些檔案，我確認後再開始實作。
```
