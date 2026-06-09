# 低延遲優化指南（Improve Guideline）

> 針對 `market-data-service` 的**端到端延遲**整理可漸進落地的效能優化方向，涵蓋：**Tokio Runtime 調優**（不替換）、線程調度、內部通道、序列化、編譯配置，以及 **gRPC / Protobuf Wire 層**（公共傳輸層維持 gRPC，不替換）。
> 審閱基準：`crates/marketdata-service/src/{ingest,bus,grpc}.rs`、`proto/marketdata.proto`、`crates/marketdata-types/src/lib.rs`。

---

## 現狀診斷

### 已做好的部分（保留，勿破壞）

| 區域 | 現狀 | 評價 |
|------|------|------|
| 內部資料模型 | `BookMessage` 為 `#[repr(C)]` + `Copy`，固定深度 `[BookLevel; 10]` | ✅ 與 SBE / raw wire 天然對齊 |
| Ingest 熱路徑 | `std::thread` + `Bus::publish` 非阻塞 | ✅ 符合 I1 不變量 |
| Subscribe 背壓 | `try_send` + 累積 `dropped_total`，嚴禁 `send().await` | ✅ 慢消費者隔離正確 |
| 架構分層 | `Upstream` trait 隔離 `feed-sim` | ✅ 未來可換 iceoryx2 / kernel bypass |
| 公共傳輸層 | tonic gRPC over HTTP/2 | ✅ 生態相容性最佳，**本指南不替換 gRPC** |

### 端到端延遲熱點地圖

目前資料從上游到客戶端的完整路徑：

```text
feed-sim thread
    │  Upstream::receive
    ▼
mds-ingest (std::thread)                    ← Core 候選：ingest 專核
    │  snapshot.put (DashMap shard lock, 極短)
    │  bus.publish (broadcast::send)
    ▼
DashMap<Figi, broadcast::Sender>
    │  fan_in_one × N (tokio::spawn)        ← 二級瓶頸：調度抖動
    │  broadcast::Receiver → mpsc try_send
    ▼
mpsc #1 (per-subscriber fan-in)
    │  wire-pump (tokio::spawn)             ← 二級瓶頸：多一層 task + mpsc
    │  book_to_proto()                      ← 一級瓶頸：heap alloc
    │  mpsc try_send
    ▼
mpsc #2 (tonic ReceiverStream)
    │  prost encode + HTTP/2 frame          ← 一級瓶頸：序列化 + 協議開銷
    ▼
NIC → Client
```

| 層級 | 程式碼位置 | 典型延遲貢獻 | 優化維度 |
|------|-----------|-------------|---------|
| **L1 調度** | `bus.rs` fan-in、`grpc.rs` wire-pump | **尾部抖動（P99/P999）主因** | 維度 6–7（綁核 + `current_thread`） |
| **L2 序列化** | `book_to_proto()`、`prost::encode` | 每條 ~1–5 µs + heap | 維度 1–2 |
| **L3 傳輸** | tonic HTTP/2（HPACK、flow control） | 每幀 ~10–50 µs（localhost 較低） | 維度 3（在 gRPC 內優化） |

> **分級結論**：
> - **P50 延遲**：一級瓶頸在 Wire 轉換層（`book_to_proto` + prost encode）。
> - **P99/P999 尾部抖動**：二級瓶頸在 `tokio::sync::broadcast` + `mpsc` 多層通道，以及 `fan_in_one` / `wire-pump` 的 work-stealing 調度（見維度 6）。
> - **公共 API 維持 gRPC**：L3 的 HTTP/2 固有開銷透過 P1–P3 壓縮 encode 路徑來對沖，而非替換傳輸協議。

### 主要瓶頸（Wire 轉換層）

內部 `BookMessage` 是 stack `Copy`（約 408 bytes），但 `book_to_proto` 每筆推送至少觸發：

1. `figi.as_str().to_string()` → **Heap #1**（`String`）
2. `bids.iter().map(...).collect()` → **Heap #2**（`Vec<Level>`）
3. `asks.iter().map(...).collect()` → **Heap #3**（`Vec<Level>`）
4. tonic/prost encode → **Heap #4**（序列化緩衝區）
5. 客戶端 decode → 對稱的 `String` + `Vec` 分配

**結論**：`Snapshot` 與 `Bus::publish` 本身設計正確；**encode 鏈**是 P50 主因，**通道 + 調度**是尾部抖動主因。兩者應分開優化。

### 二級瓶頸（通道與調度層）

`bus.rs` 與 `grpc.rs` 目前堆疊了三層異步通道：

```text
broadcast::Sender::send          (ingest 熱路徑，有鎖 + 引用計數)
    → fan_in_one tokio task      (broadcast::recv().await)
    → mpsc #1 try_send           (內部 mutex)
    → wire-pump tokio task       (sub.next().await)
    → mpsc #2 try_send           (tonic ReceiverStream)
    → tonic encode + HTTP/2
```

**為什麼造成尾部抖動：**

1. **`tokio::sync::mpsc` 內部有 mutex**：高並發 `try_send` / `recv` 產生鎖競爭。
2. **`broadcast` 同樣有同步開銷**：每個 subscriber 的 `fan_in_one` 是獨立 tokio task，調度順序不可預測（`bus.rs` 測試 `multi_figi_fan_in_merges_streams` 已明確承認順序與調度相關）。
3. **跨邊界成本**：`std::thread`（ingest）→ `tokio task`（fan-in）→ `tokio task`（wire-pump）至少 **2 次 wake-up + 上下文切換**。
4. **`BookMessage` 是 Copy（~408 bytes）**：每層通道傳遞都是完整結構體拷貝；三層 ≈ 三次 408B memcpy。

> 此二級瓶頸**不影響 I1 不變量**（ingest 仍不阻塞），但會拉高 Subscribe 的 P99。優化方向見**維度 6**，最終出口仍接 tonic gRPC。

---

## 優先級行動清單

按投入產出比排序；建議按 Phase 漸進落地，每步保持測試通過。**公共傳輸層維持 gRPC，不列入替換項。**

| 優先級 | 項目 | 改動範圍 | 預期收益 |
|--------|------|----------|----------|
| P0 | Release 編譯配置 | `Cargo.toml` | 零程式碼改動，整體 5–15% 延遲改善 |
| P0.5 | CPU 綁核（`core_affinity`） | `main.rs`、`ingest.rs` | P99 ↓ 30–50%，低風險 |
| P1 | Proto 定長整數 + `bytes figi` | `.proto` + `grpc.rs` | 減少 varint bit-shift，消除 UTF-8 驗證 |
| P2 | 手動 encode / 預分配 `BytesMut` | `grpc.rs` | 消除每筆 3 次 heap alloc |
| P3 | 自訂 tonic `Codec` | 新模組 + `grpc.rs` | 跳過 `proto::Book` 中間結構 |
| P3.5 | 內部通道替換（`rtrb` SPSC） | `bus.rs`、`grpc.rs` | P99 ↓ 50–70%，出口仍為 gRPC |
| P3.6 | Tokio 雙 Runtime + `current_thread` 熱路徑 | `main.rs`、`grpc.rs` | 消除 work-stealing，P99 ↓ 20–40% |
| P3.7 | Tokio feature 裁剪 + Runtime 調參 | `Cargo.toml`、`main.rs` | 減少 binary / 編譯開銷，邊際延遲改善 |
| P4 | 架構分流：gRPC 公共路徑 + 私有快路徑 | 新 sidecar（可選） | 特約節點 tick-to-trade；**不改公共 gRPC** |
| ~~P5~~ | ~~Monoio / io_uring 替換~~ | — | **本專案不採用**（見附錄 A） |

---

## 維度 1：Protobuf 欄位編碼優化（Avoid Varints）

### 現狀問題

```protobuf
// proto/marketdata.proto（現狀）
message Book {
    string figi         = 1;  // length-delimited + UTF-8 驗證 + String alloc
    uint64 gateway_seq  = 2;  // Varint（大序號時 5–10 bytes + 迴圈解析）
    int64  gateway_ts   = 3;  // ZigZag Varint
    repeated Level bids = 4;
    repeated Level asks = 5;
}

message Level {
    double price  = 1;  // ✅ fixed64，已是定長
    float  qty    = 2;  // ✅ fixed32，已是定長
    uint32 orders = 3;  // Varint
}

message BookUpdate {
    uint64 dropped_total = 2;  // Varint
}
```

| 欄位 | Wire 格式 | 反序列化成本 |
|------|------|------|
| `uint64` / `int64` | Varint / ZigZag | 迴圈 bit-shift |
| `uint32 orders` | Varint | 小值 1 byte，仍需解析迴圈 |
| `string figi` | length-delimited | UTF-8 驗證 + `String` 分配 |
| `double` / `float` | fixed64 / fixed32 | ✅ 單次 `read_unaligned` |

> `gateway_seq` 單調遞增時，Varint 從 1 byte 膨脹到 5+ bytes；`fixed64` 永遠 8 bytes，大序號場景反而 **更小且更快**。

### 建議 Schema（P1）

```protobuf
message Book {
    bytes   figi         = 1;   // 固定 12 bytes，對齊內部 Figi [u8; 12]
    fixed64 gateway_seq  = 2;   // 定長 8 bytes，O(1) 讀取
    sfixed64 gateway_ts  = 3;   // 定長 8 bytes，有符號直接映射
    repeated Level bids  = 4;
    repeated Level asks  = 5;
}

message Level {
    fixed64 price_mantissa = 1; // 定點數 price * 1e8（HFT 實務，避免 f64 語意歧義）
    fixed32 qty_bits       = 2; // f32 bit pattern，或改用 fixed32 qty_mantissa
    fixed32 orders         = 3;
}

message BookUpdate {
    Book    book           = 1;
    fixed64 dropped_total  = 2;
}

// 請求側 figi 可保留 string（低頻），或改 bytes 以對稱
message SnapshotEntry {
    bytes figi = 1;
    // ...
}
```

### prost 生成碼差異（概念）

```rust
// Varint decode（uint64）— while-loop + bit-shift，~50–200ns
fn decode_varint(buf: &[u8]) -> (u64, usize) { /* ... */ }

// fixed64 decode — 單次 unaligned load，~5–10ns
#[inline(always)]
fn decode_fixed64(buf: &[u8]) -> (u64, usize) {
    let val = u64::from_le_bytes(buf[..8].try_into().unwrap());
    (val, 8)
}
```

### 定點價格轉換（若採用 `price_mantissa`）

```rust
const PRICE_SCALE: f64 = 1e8;

#[inline(always)]
fn price_to_wire(p: f64) -> i64 {
    (p * PRICE_SCALE).round() as i64
}

#[inline(always)]
fn price_from_wire(v: i64) -> f64 {
    v as f64 / PRICE_SCALE
}
```

### build.rs 補充（P1）

```rust
tonic_build::configure()
    .build_server(true)
    .build_client(true)
    .bytes([".marketdata.v1.Book.figi"])  // figi 生成 prost::bytes::Bytes
    .compile_protos(&[proto], &["../../proto"])?;
```

### 落地檢查清單

- [ ] 更新 `proto/marketdata.proto` 欄位型別
- [ ] 調整 `book_to_proto` / client 解析邏輯
- [ ] 更新 `grpc.rs` 單元測試與 `tests/grpc_basic.rs` 整合測試
- [ ] 確認 sample client（`src/bin/client.rs`）相容新 wire 格式

---

## 維度 2：記憶體分配優化（Zero-Heap Allocation）

### 現狀熱路徑（`grpc.rs`）

```rust
fn book_to_proto(msg: &BookMessage) -> Book {
    Book {
        figi: msg.figi.as_str().to_string(),           // ❌ Heap
        gateway_seq: msg.gateway_seq,
        gateway_ts: msg.gateway_ts,
        bids: msg.bids().iter().map(level_to_proto).collect(), // ❌ Heap
        asks: msg.asks().iter().map(level_to_proto).collect(), // ❌ Heap
    }
}
```

以 `MAX_BOOK_DEPTH = 10` 估算，每筆 `BookUpdate` 在 server 端至少 **3 次 heap 分配**，client 端 decode 再對稱分配。

### 方案 A：Thread-local 預分配 `BytesMut`（P2，推薦漸進式）

```rust
use bytes::{BufMut, BytesMut};
use std::cell::RefCell;

thread_local! {
    static ENCODE_BUF: RefCell<BytesMut> =
        RefCell::new(BytesMut::with_capacity(512));
}

/// 從 Copy 的 BookMessage 直接 encode，不建 proto::Book 中間結構。
#[inline(always)]
fn encode_book_update(msg: &BookMessage, dropped_total: u64, buf: &mut BytesMut) {
    buf.clear();
    // 手動寫 BookUpdate field tags + payloads
    // Field 1: Book (length-delimited)
    // Field 2: fixed64 dropped_total
    encode_book(msg, buf);
}

#[inline(always)]
fn encode_book(msg: &BookMessage, buf: &mut BytesMut) {
    // figi: bytes, 12 bytes 直接 put_slice(&msg.figi.0)
    // gateway_seq: fixed64
    // gateway_ts: sfixed64
    // bids/asks: repeated Level
}
```

在 `subscribe` wire-pump 中使用：

```rust
ENCODE_BUF.with(|cell| {
    let mut buf = cell.borrow_mut();
    encode_book_update(&book, dropped.load(Ordering::Relaxed), &mut buf);
    let bytes = buf.clone().freeze(); // Bytes refcount，非深拷貝
    // 需配合自訂 Encoder 送出 bytes
});
```

### 方案 B：bumpalo Arena（適合單連線生命週期）

每個 `Subscribe` 連線持有一個 `Bump` arena；`book_to_proto` 的 `Vec<Level>` 改在 arena 內分配，斷線時 `drop` 整批釋放（O(1) reset）。

適用場景：仍需保留 `proto::Book` 結構、尚未落地手動 encode 時的過渡方案。

### 方案 C：自訂 tonic Codec（P3，終極目標）

跳過 `proto::Book`，讓 `Codec::Encode = BookMessage`（或 `Bytes`），在 `Encoder::encode` 內直接呼叫方案 A 的手動 encode。

```rust
use tonic::codec::{Codec, Encoder, EncodeBuf};

pub struct BookMessageCodec;

impl Codec for BookMessageCodec {
    type Encode = BookMessage;
    type Decode = SubscribeRequest;
    // encoder / decoder 實作
}
```

### 落地檢查清單

- [ ] 在 `Subscribe` 熱路徑消除 `book_to_proto` 的 `String` + `Vec` 分配
- [ ] 評估 `GetSnapshots` 是否可用同一套 encode 邏輯（batch場景頻率低，優先級次於 Subscribe）
- [ ] 用 `dhat` 或 `heaptrack` 驗證每筆 message 的 alloc次數降至 0–1

---

## 維度 3：傳輸層與零拷貝（Zero-Copy，維持 gRPC）

> **架構裁量**：公共傳輸層固定為 **gRPC over HTTP/2**。本維度目標是壓縮「進入 tonic 之前」的拷貝與分配，使 HTTP/2 固有開銷（HPACK ~5–15 µs、frame 組裝 ~5–10 µs）在總延遲中佔比下降，而非替換協議。

### gRPC/HTTP/2 固有開銷（接受，不優化掉）

| 環節 | 典型成本 | 可否在維持 gRPC 下消除 |
|------|---------|----------------------|
| HPACK 頭部壓縮 | ~5–15 µs | ❌ tonic 內建 |
| HTTP/2 DATA frame 封裝 | ~5–10 µs/frame | ❌ |
| Flow control window | 變動 | ❌ |
| TLS（若啟用） | ~50–200 µs | ⚠️ 本作業 plaintext，生產可 session resumption |
| prost encode（P2–P3 後） | **~150 ns** | ✅ 可優化 |

localhost 實測參考：gRPC Subscribe P50 ~50 µs、P99 ~500 µs；完成 P0–P3 後，encode 開銷從總量 30–40% 降至 <10%，HTTP/2 成為剩餘下限。

### 現狀資料流中的拷貝點

```text
BookMessage (stack, Copy)
    │ book_to_proto()          ← 拷貝 #1：欄位複製到 heap 結構
    ▼
proto::Book (heap: String, Vec)
    │ prost::Message::encode   ← 拷貝 #2：序列化到新 buffer
    ▼
BytesMut (tonic internal)
    │ HTTP/2 frame             ← 可能零拷貝（Bytes 切片 refcount）
    ▼
NIC
```

### `bytes::Bytes` 零拷貝要點

- `figi` 改為 `bytes` 後，decode 端可用 `Bytes::slice` 持有原始 buffer 引用（僅增加 refcount）
- Server 端預序列化 `Bytes` 可直接送入 stream，避免二次分配
- `BookMessage` 內部已是 `Figi([u8; 12])`，encode 時 `put_slice(&msg.figi.0)` 無需字串轉換

```rust
// decode 端零拷貝 figi 範例
fn parse_figi_zero_copy(buf: &bytes::Bytes) -> &[u8] {
    &buf[..FIGI_LEN]  // slice into original buffer
}
```

### Kernel Bypass 架構建議（P4，生產級 HFT）

gRPC/HTTP2 本身有 HPACK、flow control、frame 組裝開銷（通常 10–50µs+），**不適合作 tick-to-trade 路徑**。

```text
┌─────────────┐     DMA      ┌──────────────┐
│   NIC       │ ──────────►  │  HugePage    │
│ (Solarflare │              │  Ring Buffer │
│  / Mellanox)│              └──────┬───────┘
└─────────────┘                     │
                              AF_XDP / DPDK
                                    │
                    ┌───────────────▼───────────────┐
                    │  Rust userspace               │
                    │  (xsk-rs / dpdk-rs / io_uring)│
                    │  直接 parse BookMessage       │
                    └───────────────┬───────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │  Bus（現有設計 ✅）            │
                    │  BookMessage: Copy, 0 alloc   │
                    └───────────────┬───────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │ gRPC（慢路徑）       │ UDP Multicast（快路徑）│
              │ 監控 / 非交易客戶端   │ 固定長度 binary frame  │
              └─────────────────────┴─────────────────────┘
```

### 🔴 關於「不針對全域公共傳輸層做 UDP 特別優化」的架構裁量基準

在本系統中，**我們明確拒絕將全域或公共行情推播服務直接替換為 UDP 或 QUIC**。這並非技術上無法實作，而是基於以下極為現實的**業務與網路場景約束**：

1. **區塊鏈網路的分散式拓撲複雜性**：本專案的節點（Clients）廣泛分布於全球公網、不同的雲端環境（AWS/GCP/阿里雲）甚至是各地的自建機房與家用環境。**許多 ISP 的網路防護策略、NAT 網關 or 企業級防火牆（Firewall Rules），對於高頻率的自訂 UDP 流量或非標準 QUIC 流量，會採取極為激進的「限速（Rate-limiting）」甚至「直接丟包（Drop）」策略**。強推全域 UDP 將導致嚴重的連線穩定度問題，並引發災難性的社群技術支援成本。
2. **外部使用者（下游生態）基數龐大且多樣**：使用該 Market Data 的外部生態夥伴、DApp 開發者及第三方監控工具陣容龐大，他們底層的技術棧（Go, Java, Python, Node.js 等）對基於 TCP 的標準 **gRPC (HTTP/2)** 支援最為成熟與友善。若貿然改為 UDP，會強迫所有下游使用者重構其網路驅動層，大幅破壞相容性並拉高生態接入門檻。

#### 💡 針對特殊網路優化需求的替代策略

若部分特定核心使用者（如造市商、驗證節點 Core Validator）對網路延遲提出極致要求，**絕不應改動公共廣播層的 Wire 協定**，而應導向以下分流方案：

* **在地化 / 邊緣運算架構（Co-location / Sidecar）**：引導高頻需求使用者與數據源部署在同一台主機或同一個局部區域網內，直接利用 Phase 4 預留的 `iceoryx2` 共享記憶體（Shared Memory）或本機 IPC 進行傳輸，直接達到 **0 網路開銷**。
* **專用私有轉發器（Private UDP Relay / Gateway）**：保留全域 gRPC (TCP) 作為標準大眾傳輸路徑。僅針對簽約、已手動打通特定網路防火牆、具備專線環境的特約高頻交易節點，單獨開闢一條私有的 **UDP Multicast 快路徑**（如上圖右側所示），將公共流量與極致延遲流量在架構上完全解耦。

---

## 維度 4：編譯器與 Codegen 極致優化

### 現狀

workspace `Cargo.toml` **無 `[profile.release]` 配置**，release build 使用 Cargo 預設值，未開啟 LTO。

### 建議配置（P0，立即落地）

在 workspace root `Cargo.toml` 加入：

```toml
[profile.release]
opt-level = 3
lto = "fat"              # 跨 crate 全程序優化，壓低 tonic 熱路徑尾延遲
codegen-units = 1        # 允許 LLVM 更激進 inline
panic = "abort"          # 移除 unwind tables，減少 code size + 分支
strip = "symbols"        # 可選：減少 binary size
overflow-checks = false  # release 關閉（需自行審計 safety）

[profile.release-with-debug]
inherits = "release"
debug = true             # perf / flamegraph 用
```

### `#[inline(always)]` 放置策略

僅對每 tick 呼叫的機械映射函式：

```rust
// grpc.rs
#[inline(always)]
fn level_to_proto(level: &marketdata_types::BookLevel) -> Level { /* ... */ }

// bus.rs — publish 已是 #[inline]，可升級
#[inline(always)]
pub fn publish(&self, book: BookMessage) { /* ... */ }
```

### PGO（Profile-Guided Optimization）

對 tonic codec 分支（frame 大小分派、定長 vs 變長路徑）的 tail latency 通常有 **5–15%** 改善。

```bash
# 1. Instrumented build
RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" \
  cargo build --release -p marketdata-service

# 2. 跑 representative workload（feed-sim 壓測）
./target/release/marketdata-service &
./target/release/feed-sim  # 依實際參數調整

# 3. 合併 profile 重新編譯
llvm-profdata merge -o /tmp/pgo-data/merged.profdata /tmp/pgo-data
RUSTFLAGS="-Cprofile-use=/tmp/pgo-data/merged.profdata" \
  cargo build --release -p marketdata-service
```

### tokio feature 裁剪（次要）

Phase 2 後可將 `tokio = { features = ["full"] }` 裁剪為實際使用的 feature set（`macros`, `rt-multi-thread`, `sync`, `net`, `io-util` 等），減少 binary size 與編譯時間，對 runtime 延遲影響較小。

### 落地檢查清單

- [ ] 加入 `[profile.release]` 配置
- [ ] release build 跑 `tests/grpc_basic.rs` 確認行為不變
- [ ] 用 `perf` / `flamegraph` 建立 baseline，PGO 後對比 tail latency（p99 / p999）

---

## 維度 5：終極架構替代（FlatBuffers / SBE / Raw Wire）

### 為何考慮替換 Protobuf？

Protobuf 的 **field tag 線性掃描**（tag → wire type → skip/decode）在固定 schema、固定深度場景下是純開銷。內部 `BookMessage` 已經是 flat memory layout，Wire層理想上應直接 mirror 它。

### 方案對比

| 方案 | Encode | Decode | Heap Alloc | Schema 演進 | 適用場景 |
|------|--------|--------|------------|-------------|----------|
| Protobuf（現狀） | ~500ns | ~800ns | 每 msg 3+ 次 | ✅ 優秀 | 通用 API、向後相容 |
| Protobuf（fixed + manual encode） | ~150ns | ~300ns | 0（server） | ✅ 優秀 | **本專案 P1–P3 目標** |
| FlatBuffers | ~200ns | **~10ns** | 0 | ⚠️ 需遷移 | 需 schema 演進的零拷貝 |
| SBE | ~50ns | **~5ns** | 0 | ❌ 固定 schema | 交易所級固定協議 |
| Raw `BookMessage` memcpy | **~30ns** | **~5ns** | 0 | ❌ 無版本 | UDP multicast 交易路徑 |

### SBE 範例（固定 schema）

```xml
<composite name="level">
  <type name="price"  primitiveType="double"/>
  <type name="qty"    primitiveType="float"/>
  <type name="orders" primitiveType="uint16"/>
  <type name="pad"    primitiveType="uint16"/>
</composite>

<sbe:message name="BookMessage" id="1">
  <field name="gatewaySeq" type="uint64"/>
  <field name="gatewayTs"  type="int64"/>
  <field name="figi"       type="char" length="12"/>
  <field name="bidCount"   type="uint8"/>
  <field name="askCount"   type="uint8"/>
  </sbe:message>
```

```rust
// Decode：pointer cast，~5ns
#[repr(C, align(8))]
struct SbeBookHeader {
    gateway_seq: u64,
    gateway_ts: i64,
    figi: [u8; 12],
    bid_count: u8,
    ask_count: u8,
}

// Safety: buf 對齊且長度足夠
#[inline(always)]
unsafe fn bids_from_buf(header: &SbeBookHeader, buf: &[u8]) -> &[BookLevel] {
    let offset = std::mem::size_of::<SbeBookHeader>();
    let ptr = buf.as_ptr().add(offset) as *const BookLevel;
    std::slice::from_raw_parts(ptr, header.bid_count as usize)
}
```

### FlatBuffers 範例（需 schema 演進的零拷貝）

```rust
// Decode：所有欄位是 &buf 上的 slice，無 heap
fn decode_book_fbs(buf: &[u8]) -> flatbuffers::Vector<'_, BookLevel<'_>> {
    let book = flatbuffers::root::<Book>(buf).unwrap();
    book.bids().unwrap()  // ~2ns per field read
}
```

### Raw `BookMessage` Wire（UDP 快路徑）

內部型別已具備條件：

```rust
// marketdata-types/src/lib.rs
#[derive(Clone, Copy)]
#[repr(C)]
pub struct BookMessage { /* fixed arrays, no pointers */ }

// Wire frame = 直接 memcpy
#[inline(always)]
fn wire_send(book: &BookMessage, buf: &mut [u8]) {
    let len = std::mem::size_of::<BookMessage>();
    buf[..len].copy_from_slice(
        unsafe {
            std::slice::from_raw_parts(
                book as *const _ as *const u8,
                len,
            )
        }
    );
}
```

> 注意：raw wire 需自行處理版本號、endianness、對齊；僅適合封閉系統內網。

### Bincode 定位（gRPC 場景下不推薦）

| 方案 | Server Encode | Client Decode | 零拷貝 | 與 gRPC 相容 |
|------|--------------|---------------|--------|-------------|
| Protobuf（P1–P3 優化後） | ~150ns | ~300ns | Server 端可達 0 alloc | ✅ 原生 |
| Bincode | ~1 µs | ~1 µs（完整反序列化 = 408B 拷貝） | ❌ | ❌ 需自訂 Codec，無跨語言 |
| FlatBuffers | ~200ns | **~10ns** | ✅ decode 零拷貝 | ⚠️ 需自訂 Codec |

Bincode 適合 Rust↔Rust 封閉 IPC，不適合作為 gRPC 公共 API 的序列化格式。在**維持 gRPC + Protobuf** 的前提下，P1–P3 的投入產出比更高。

---

## 維度 6：線程與調度優化（Tail Latency）

> **目標**：壓低 P99/P999 尾部抖動，**不改變 gRPC 公共出口**。優化 `ingest → bus → wire-pump` 的內部通道與 CPU 拓撲。

### 6.1 CPU 綁核（P0.5，低風險高收益）

#### 痛點

`main.rs` 使用預設 `#[tokio::main]`，所有線程由 OS 自由調度：

- **Cache Miss**：ingest 的 L1/L2 cache 被 tokio worker 搶佔後需重新 warm。
- **NUMA 效應**（多 socket 機器）：跨 node 訪問延遲可達 2×。
- **尾部抖動**：負載高峰時 ingest 與 gRPC worker 可能被排到同一核心。

目前活躍線程：`feed-sim` 生成、`mds-ingest`、N 個 tokio worker + `fan_in_one` / `wire-pump` tasks。

#### 建議核心分配（假設 4+ 核心）

```text
Core 0: feed-sim generator（上游，可選綁核）
Core 1: mds-ingest（最熱路徑，必綁）
Core 2: 合併後的 wire 消費線程 / 高優先級 tokio worker（gRPC streaming）
Core 3+: 其餘 tokio worker（unary RPC、accept 等）
```

#### 依賴與初始化

```toml
# Cargo.toml
core_affinity = "0.8"
```

```rust
use core_affinity::{get_core_ids, set_for_current, CoreId};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TOKIO_CORE: AtomicUsize = AtomicUsize::new(0);

pub struct CpuTopology {
    pub ingest_core: CoreId,
    pub wire_core: CoreId,
    pub tokio_cores: Vec<CoreId>,
}

impl CpuTopology {
    pub fn detect() -> Self {
        let cores = get_core_ids().expect("no CPU cores");
        assert!(cores.len() >= 3, "need ≥3 cores for pinning");
        Self {
            ingest_core: cores[1],
            wire_core: cores[2],
            tokio_cores: cores[3..].to_vec(),
        }
    }
}

/// ingest::spawn 內，線程入口第一行
fn ingest_loop_entry(core: CoreId) {
    set_for_current(core);
    // ... 原有 ingest_loop 邏輯
}

/// 替換 #[tokio::main]，在 main.rs
fn build_pinned_runtime(tokio_cores: &[CoreId]) -> tokio::runtime::Runtime {
    let cores = tokio_cores.to_vec();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores.len().max(1))
        .on_thread_start(move || {
            let idx = NEXT_TOKIO_CORE.fetch_add(1, Ordering::Relaxed);
            if let Some(core) = cores.get(idx) {
                set_for_current(*core);
            }
        })
        .thread_name("tokio-worker")
        .enable_all()
        .build()
        .expect("build runtime")
}
```

#### Linux 可選系統調優

```bash
# 隔離核心（需重啟，生產環境）
# /etc/default/grub: isolcpus=1,2,3

# 關閉 frequency scaling，減少抖動
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

#### 落地檢查清單

- [ ] `ingest::spawn` 線程入口呼叫 `set_for_current(ingest_core)`
- [ ] `main.rs` 改用自定義 pinned tokio runtime
- [ ] 文件化最低核心數需求（建議 ≥ 4）
- [ ] 綁核後重跑 `tests/grpc_basic.rs` 與 `slow_consumer` 測試

---

### 6.2 內部通道重構：Ring Buffer 替換 `tokio::sync::mpsc`（P3.5）

> **範圍限定**：僅替換 `bus → wire-pump` 之間的 **mpsc #1**；**mpsc #2（tonic ReceiverStream）與 HTTP/2 出口保留**。

#### 痛點回顧

| 現有元件 | 問題 |
|---------|------|
| `tokio::sync::broadcast` | 鎖 + 引用計數；`fan_in_one` 需 `.await` |
| `tokio::sync::mpsc`（#1） | 內部 mutex；`try_send` 高頻競爭 |
| `fan_in_one` + `wire-pump` | 兩個 tokio task，調度不可預測 |

#### Crate 選型

| Crate | 模式 | 特點 | 本專案適用 |
|-------|------|------|-----------|
| [`rtrb`](https://crates.io/crates/rtrb) | SPSC | Real-Time Ring Buffer，wait-free push/pop | ✅ per-subscriber 首選 |
| [`ringbuf`](https://crates.io/crates/ringbuf) | SPSC/MPSC | API 簡潔，純 Rust | ✅ 漸進式替代 |
| [`disruptor`](https://crates.io/crates/disruptor) | 1→N fan-out | LMAX Disruptor，`BusySpin` 等待 | ⚠️ 多 subscriber 同 FIGI 時考慮 |

#### 建議拓撲（維持 gRPC 出口）

```text
ingest (std::thread, pinned core)
    │  snapshot.put
    │  LowLatencyBus::publish → rtrb::Producer::push (wait-free)
    ▼
per-subscriber SPSC Ring Buffer（容量 1024/2048，2 的冪）
    │
    ├─ 方案 A：wire-pump tokio task 改為 poll/pop（仍接 tonic mpsc #2）
    └─ 方案 B：專用 mds-wire std::thread busy-spin → encode → tonic try_send
    ▼
mpsc #2 → tonic gRPC（不變）
```

#### 核心程式碼（`bus.rs` 漸進替換）

```toml
# Cargo.toml
rtrb = "0.3"
```

```rust
use rtrb::{Producer, Consumer, RingBuffer};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use marketdata_types::{BookMessage, Figi};

pub struct LowLatencyBus {
    subscribers: dashmap::DashMap<Figi, Vec<SubscriberSlot>>,
    ring_capacity: usize,  // 建議 1024 / 2048 / 4096
}

struct SubscriberSlot {
    producer: Producer<BookMessage>,
    dropped: Arc<AtomicU64>,
}

impl LowLatencyBus {
    /// Ingest 熱路徑：wait-free push，滿則 drop（語義與現有 try_send 一致）
    #[inline(always)]
    pub fn publish(&self, book: BookMessage) {
        if let Some(slots) = self.subscribers.get(&book.figi) {
            for slot in slots.iter() {
                if slot.producer.push(book).is_err() {
                    slot.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// subscribe 返回 Consumer；wire 端負責 pop
    pub fn subscribe(&self, figi: Figi) -> (Consumer<BookMessage>, Arc<AtomicU64>) {
        let (prod, cons) = RingBuffer::new(self.ring_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        self.subscribers
            .entry(figi)
            .or_insert_with(Vec::new)
            .push(SubscriberSlot { producer: prod, dropped: dropped.clone() });
        (cons, dropped)
    }
}
```

#### `grpc.rs` wire-pump 改造（仍接 tonic）

```rust
// 替換 sub.next().await → Consumer::pop()，減少一層 mpsc #1
tokio::task::spawn(async move {
    loop {
        tokio::select! {
            // 輪詢所有 figi 的 Consumer（或用 dedicated thread + block_on）
            _ = async {
                for cons in consumers.iter_mut() {
                    while let Ok(book) = cons.pop() {
                        ENCODE_BUF.with(|cell| {
                            let mut buf = cell.borrow_mut();
                            encode_book_update(&book, dropped.load(Relaxed), &mut buf);
                            // 仍送入 tonic 的 mpsc #2
                            let _ = out_tx.try_send(Ok(wire_bytes_to_book_update(&buf)));
                        });
                    }
                }
            } => {}
            _ = out_tx.closed() => break,
        }
        // hybrid：空轉時 yield，避免 100% CPU
        tokio::task::yield_now().await;
    }
});
```

> **注意**：`fan_in_one` 可整體刪除——ingest 直接 `push` 到 per-subscriber ring，`dropped_total` 語義不變。`try_send` + 慢消費者隔離契約保持。

#### 與現有設計的對應改動

| 現有 | 替換為 | 檔案 | gRPC 影響 |
|------|--------|------|----------|
| `broadcast::send` | `rtrb::Producer::push` | `bus.rs` | 無 |
| `fan_in_one` tokio task | 刪除 | `bus.rs` | 無 |
| `mpsc #1` | `rtrb` SPSC | `bus.rs` | 無 |
| `wire-pump` | poll `Consumer::pop` 或專用 thread | `grpc.rs` | 無 |
| `mpsc #2` + tonic | **保留** | `grpc.rs` | 公共 API 不變 |

#### 落地檢查清單

- [ ] `Bus` 漸進替換為 `LowLatencyBus`（或新增 feature flag `low-latency-bus`）
- [ ] 保留 `dropped_total` 累積語義與 `try_send` 契約
- [ ] 重跑 `bus.rs` 單元測試（`slow_consumer_isolation` 等）
- [ ] 重跑 `tests/grpc_slow_consumer.rs` 驗證 gRPC 路徑仍隔離慢消費者
- [ ] `ring_capacity` 對齊現有 `MDS_BUS_CAPACITY` / `MDS_SUBSCRIBER_QUEUE` env

---

## 維度 7：Tokio 極致調優（保留 Tokio，不替換 Runtime）

> **架構裁量（本專案定案）**：**不替換 Tokio**。Runtime 層所有優化均在 Tokio 生態內完成，目標是壓榨 Tokio 極限而非引入 Monoio / smol 等替代方案（不採用理由見**附錄 A**）。

### 7.0 為何不替換 Tokio

| 因素 | 說明 |
|------|------|
| **tonic 硬依賴** | `tonic::transport::Server`、`tokio-stream` 的 `TcpListenerStream` 均要求 Tokio runtime；替換 = 重寫整個 gRPC 服務層 |
| **生態綁定深度** | workspace 中 `tokio`、`tokio-stream`、`tonic` 及整合測試的 `#[tokio::test]` 形成閉環；其他 crate / 下游服務是否也依賴 Tokio **尚未盤點，貿然替換風險不可控** |
| **混合 runtime 成本** | Tokio + Monoio 雙 runtime 共存需處理 `Send` 邊界、雙重 event loop、除錯複雜度；收益遠小於 P0.5–P3.6 |
| **已有替代路徑** | ingest 已是 `std::thread`；配合 `rtrb` + `current_thread` / 專用 `std::thread` wire，**80–90% 的 Monoio 收益可在 Tokio 架構內達成** |

```text
本專案 Runtime 分層定案：

  std::thread     → ingest（mds-ingest）、可選 mds-wire
  current_thread  → Subscribe wire-pump（無 work-stealing）
  multi_thread    → tonic gRPC server（accept / unary / HTTP/2 I/O）
  + core_affinity → 各層釘核（維度 6.1）
```

---

### 7.1 Tokio 延遲問題根因：Work-stealing

`multi_thread` runtime（`#[tokio::main]` 預設）的調度模型：

```text
每個 worker 有 local queue
    → local queue 空時，steal 其他 worker 的任務（全局隊列 + 跨 worker 鎖）
    → 任務可在 worker 間遷移 → L1/L2 cache 失效 → P99 尾部抖動
```

| 現有 tokio task | 所在 runtime | 被 steal 風險 |
|----------------|-------------|--------------|
| `fan_in_one` × N | `multi_thread`（`bus::subscribe` 內 `tokio::spawn`） | ⚠️ 高 |
| `wire-pump` | `multi_thread`（`grpc.rs` 內 `tokio::spawn`） | ⚠️ 高 |
| tonic gRPC handler | `multi_thread` | ✅ 可接受（I/O 密集） |
| `spawn_blocking`（ingest join） | blocking pool | ✅ 低頻 |

**Tokio 內建解法**：對熱路徑任務使用 **`current_thread` flavor** 或 **`spawn_local` + `LocalSet`**，從根本上拔除 work-stealing。

---

### 7.2 `multi_thread` vs `current_thread` 對照

| 特性 | `multi_thread` | `current_thread` |
|------|---------------|-----------------|
| Worker 數 | N（預設 = CPU 數） | **1** |
| Work-stealing | ✅ 有（尾部抖動來源） | ❌ **無** |
| `tokio::spawn` | 可跨 thread | 任務固定在單 thread |
| 適用場景 | gRPC accept、多連線 I/O | **單連線 wire-pump、熱路徑 encode** |
| 與 tonic 相容 | ✅ 原生 | ✅ 可共存（雙 runtime） |

> `current_thread` 本質上就是 **Thread-per-task-queue**：任務不遷移，配合 `core_affinity` 後等價於「單核單隊列」模型，**無需替換 runtime**。

---

### 7.3 方案 A：`current_thread` 獨立 Runtime（P3.6，推薦）

每個 Subscribe 連線的 wire-pump（或合併為一個共享 wire runtime）跑在 **專屬 `current_thread` runtime** 上：

```rust
use core_affinity::set_for_current;

/// 在專屬 OS thread 上跑 current_thread runtime（無 work-stealing）
fn spawn_wire_runtime_thread(
    wire_core: core_affinity::CoreId,
    wire_rx: crossbeam_channel::Receiver<WireJob>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("mds-wire-rt".into())
        .spawn(move || {
            set_for_current(wire_core);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("wire runtime");
            rt.block_on(async move {
                while let Ok(job) = wire_rx.recv() {
                    wire_pump_loop(job).await;
                }
            });
        })
        .expect("spawn wire runtime thread")
}
```

**`grpc.rs` Subscribe handler 改造思路**：

```rust
async fn subscribe(&self, request: Request<SubscribeRequest>) -> ... {
    // ... figi 驗證、建立 rtrb consumers ...
    let (out_tx, out_rx) = mpsc::channel(self.subscriber_queue_size);

    // ★ 派發到 current_thread wire runtime（無 work-stealing）
    self.wire_dispatch.send(WireJob { consumers, out_tx, dropped }).ok();

    Ok(Response::new(ReceiverStream::new(out_rx)))
}

async fn wire_pump_loop(job: WireJob) {
    loop {
        tokio::select! {
            _ = async {
                for cons in job.consumers.iter_mut() {
                    while let Ok(book) = cons.pop() {
                        let _ = job.out_tx.try_send(Ok(encode_book_update(&book, ...)));
                    }
                }
            } => {}
            _ = job.out_tx.closed() => break,
        }
        tokio::task::yield_now().await;
    }
}
```

**測試**：wire 路徑單元測試建議貼近生產 flavor：

```rust
#[tokio::test(flavor = "current_thread")]
async fn wire_pump_drops_on_full_mpsc() { /* ... */ }
```

---

### 7.4 方案 B：雙 Runtime 分離（P3.6，生產推薦組裝）

```text
┌─────────────────────────────────────────────────────────────┐
│ main 進程                                                    │
│                                                              │
│  std::thread [Core 1]                                        │
│    └─ mds-ingest → snapshot + bus.publish(rtrb)              │
│                                                              │
│  std::thread [Core 2] — wire runtime thread                  │
│    └─ current_thread runtime                                 │
│         └─ wire-pump(s) → encode → tonic mpsc try_send       │
│                                                              │
│  tokio multi_thread [Core 3–N] — gRPC runtime               │
│    ├─ tonic::Server::serve（accept / HTTP/2 / unary）        │
│    ├─ Subscribe handler（僅建立 rtrb + 派發 WireJob）         │
│    └─ spawn_blocking（ingest join）                          │
└─────────────────────────────────────────────────────────────┘
```

```rust
// main.rs 組裝範例
fn main() -> Result<(), BoxError> {
    let topo = CpuTopology::detect();
    let (wire_tx, wire_rx) = crossbeam_channel::unbounded::<WireJob>();
    spawn_wire_runtime_thread(topo.wire_core, wire_rx);

    let grpc_rt = build_pinned_runtime(&topo.tokio_cores);
    grpc_rt.block_on(async {
        let service = Service::new_with_wire_dispatch(cfg, wire_tx)?;
        service.run().await
    })
}
```

**關鍵約束**：

- `tokio::sync::mpsc::Sender::try_send` 是 `Sync` 的，wire runtime **可直接 try_send 到 tonic 的 `out_tx`**。
- `rtrb::Consumer` **不是 `Send`** → 必須在建立 consumer 的 wire thread 內消費，不能丟給 `multi_thread` pool。

---

### 7.5 方案 C：熱路徑完全脫離 Tokio 任務（與 P3.5 互補）

```rust
// std::thread 內，無 tokio runtime
fn wire_thread_main(
    mut consumers: Vec<rtrb::Consumer<BookMessage>>,
    out_tx: tokio::sync::mpsc::Sender<Result<BookUpdate, Status>>,
    dropped: Arc<AtomicU64>,
) {
    set_for_current(wire_core);
    loop {
        let mut got = false;
        for cons in consumers.iter_mut() {
            while let Ok(book) = cons.pop() {
                got = true;
                ENCODE_BUF.with(|buf| { /* P2 手動 encode */ });
                if out_tx.try_send(Ok(upd)).is_err() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if !got { std::hint::spin_loop(); }
    }
}
```

| 方案 | work-stealing | tokio 開銷 | 複雜度 | 推薦場景 |
|------|--------------|-----------|--------|---------|
| A `current_thread` runtime | ❌ | 低 | 中 | 需 `select!` / 優雅斷線 |
| B 雙 Runtime 分離 | ❌（熱路徑） | 中 | 中 | **生產預設** |
| C `std::thread` busy-spin | ❌ | **零** | 低 | 極致 P99、可接受 CPU 佔用 |

---

### 7.6 Core Affinity + Tokio 組合（交叉引用 6.1）

| 執行緒 / Runtime | 綁定核心 | 設置時機 |
|---------------|---------|---------|
| `mds-ingest` | `topo.ingest_core` | `ingest::spawn` 線程入口 |
| wire runtime / `mds-wire` | `topo.wire_core` | wire thread 入口 |
| gRPC `multi_thread` workers | `topo.tokio_cores[]` | `on_thread_start` |
| feed-sim generator | Core 0（可選） | upstream 初始化 |

> `current_thread` runtime 跑在專屬 `std::thread` 上時，在 **`thread::spawn` 入口`** 呼叫 `set_for_current`，而非在 `block_on` 內部。

---

### 7.7 Runtime Builder 調參（P3.7）

```rust
tokio::runtime::Builder::new_multi_thread()
    .worker_threads(topo.tokio_cores.len())  // 精確匹配，不過度超配
    .max_blocking_threads(2)                 // 僅 ingest join，預設 512 過大
    .thread_stack_size(2 * 1024 * 1024)
    .thread_name("tokio-grpc")
    .on_thread_start(/* 綁核 */)
    .enable_io()
    .enable_time()
    .build()?
```

| Env | 建議預設 | 用途 |
|-----|---------|------|
| `TOKIO_WORKER_THREADS` | `tokio_cores.len()` | gRPC worker 數 |
| `TOKIO_MAX_BLOCKING` | `2` | blocking pool 上限 |
| `MDS_WIRE_SPIN` | `hybrid` | 方案 C：`spin` / `yield` / `hybrid` |

#### `spawn` 使用規範

| 操作 | 熱路徑 | 冷路徑 |
|------|--------|--------|
| `tokio::spawn` 到 global pool | ❌ wire-pump、fan-in | ✅ tonic I/O handler |
| `spawn_local` + `LocalSet` | ✅ 若堅持單 runtime | — |
| `std::thread::spawn` | ✅ ingest、mds-wire | — |
| `spawn_blocking` | ❌ tick 路徑 | ✅ ingest `join` |

---

### 7.8 Tokio Feature 裁剪（P3.7，交叉引用維度 4）

```toml
tokio = { version = "1", features = [
    "rt-multi-thread", "macros", "sync", "net", "io-util", "time", "signal",
] }
tokio-stream = { version = "0.1", features = ["net"] }
```

> 對 runtime 延遲影響小，主要收益是 binary size 與依賴邊界審計。

---

### 7.9 協作式調度與 `tokio::select!` 注意事項

| 模式 | 風險 | 建議 |
|------|------|------|
| 長迴圈 `pop()` 無 yield | 阻塞 `current_thread` 的 I/O 驅動 | 每 N 次 pop 後 `yield_now().await` |
| `select!` 雙 arm 公平輪詢 | 無優先級 | 熱路徑 arm 放前面；或拆到獨立 runtime |

---

### 7.10 逐步遷移順序

```text
Step 1: P0 + P0.5 — release profile + 綁核現有 multi_thread
Step 2: P3.5 — rtrb 替換 broadcast + mpsc #1，刪除 fan_in_one
Step 3: P3.6 — wire-pump 遷到 current_thread 或 std::thread（方案 B/C）
Step 4: P3.7 — feature 裁剪 + worker_threads 調參
Step 5: P1 → P2 → P3 — encode 優化（可與 Step 2–4 並行）
```

### 7.11 落地檢查清單

- [ ] **不引入** Monoio / smol / async-std
- [ ] `main.rs` 拆分 gRPC runtime 與 wire runtime（方案 B）
- [ ] `grpc.rs` Subscribe 不再 `tokio::spawn(wire-pump)` 到 global pool
- [ ] `rtrb::Consumer` 不跨 thread 傳遞
- [ ] `try_send` 契約不變
- [ ] 綁核覆蓋 ingest + wire + gRPC 三層
- [ ] 裁剪 tokio / tokio-stream features
- [ ] 重跑 `grpc_basic` + `grpc_slow_consumer` + `bus.rs` 全套測試

---

## 附錄 A：Monoio / io_uring（本專案不採用）

> 僅作技術對照，**不納入實施路線圖**。

### 不採用原因（定案）

1. **tonic 不可替代地依賴 Tokio** — gRPC 層仍需要 Tokio，雙 runtime 維護負擔大。
2. **生態風險** — workspace 與下游 crate 的 Tokio 依賴尚未完整盤點。
3. **收益遞減** — P0.5 + P3.5 + P3.6 完成後，io_uring 僅剩 ~1 µs 量級 syscall 收益。
4. **P3.6 已覆蓋核心訴求** — `current_thread` + `core_affinity` 在 Tokio 內等價實現「無 work-stealing + 釘核」。

| 能力 | Monoio | 本專案 Tokio 對應 |
|------|--------|------------------|
| 無 work-stealing | thread-per-core | `current_thread` runtime（7.3） |
| 核心綁定 | 手動 | `core_affinity`（6.1） |
| io_uring I/O | 原生 | ❌ gRPC 仍走 epoll（tonic 內建） |

---

## 建議實施路線圖

```text
Phase 0（本週，零/低風險）
  ├─ P0:   Cargo.toml release profile（LTO、codegen-units=1）
  └─ P0.5: CPU 綁核（core_affinity + pinned tokio runtime）
           └─ 預期：P99 ↓ 30–50%，測試零改動

Phase 1（1–2 天，gRPC Wire 層）
  └─ P1: proto fixed 整數 + bytes figi
     └─ 更新 grpc.rs 轉換 + grpc_basic 測試

Phase 2（3–5 天，gRPC Wire 層）
  └─ P2: Subscribe 路徑手動 encode + thread-local BytesMut
     └─ 基準測試：alloc 次數、encode 延遲（P50 主因）

Phase 3（1–2 週，Tokio + 內部通道，gRPC 出口不變）
  ├─ P3:   自訂 tonic Codec（跳過 proto::Book 中間結構）
  ├─ P3.5: bus 內部 mpsc/broadcast → rtrb SPSC，刪除 fan_in_one
  ├─ P3.6: Tokio 雙 Runtime — wire-pump 遷到 current_thread（方案 B）
  │        └─ 或 std::thread busy-spin（方案 C，極致 P99）
  ├─ P3.7: Tokio feature 裁剪 + Runtime Builder 調參
  └─ PGO 壓測與 p99 驗證

Phase 4（架構級，按需，不改公共 gRPC）
  ├─ 私有 sidecar：UDP/raw 或 iceoryx2（僅白名單節點）
  └─ iceoryx2 / AF_XDP 替換 feed-sim Upstream

（不採用）Monoio / io_uring 替換 — 見附錄 A
```

### 風險與收益矩陣

| 階段 | 改動 | P50 改善 | P99 改善 | 風險 | 破壞 gRPC？ |
|------|------|---------|---------|------|------------|
| P0 | Release profile | 5–15% | 5–10% | 極低 | 否 |
| P0.5 | CPU 綁核 | 10–20% | **30–50%** | 低 | 否 |
| P1 | Proto fixed 欄位 | 15–25% | 10–15% | 低（wire 破壞性變更） | 否（仍 gRPC） |
| P2 | 手動 encode | **30–50%** | 20–30% | 中 | 否 |
| P3 | 自訂 Codec | 10–20% | 10–15% | 中 | 否 |
| P3.5 | rtrb 替換 mpsc | 5–10% | **40–60%** | 中 | 否 |
| P3.6 | `current_thread` 熱路徑 | 5–10% | **20–40%** | 中 | 否 |
| P3.7 | Tokio 調參 / feature 裁剪 | 邊際 | 5–10% | 低 | 否 |
| P4 | 私有快路徑 sidecar | — | 特約節點 <1µs | 高 | 否（分流） |

---

## 量測與驗證

優化前後應記錄以下指標，**P50 與 P99 分開量測**（兩者優化手段不同）：

| 指標 | 工具 | 目標 | 主要對應 Phase |
|------|------|------|---------------|
| 每筆 BookUpdate heap alloc 次數 | `dhat-heap` / `heaptrack` | Subscribe 熱路徑 → 0 | P2–P3 |
| encode 延遲（p50 / p99 / p999） | `criterion` / `perf stat` | p50 下降 > 30% | P1–P3 |
| **尾部抖動（p99 / p999）** | `hdrhistogram` / 自建壓測 | p99 下降 > 50% | P0.5、P3.5、P3.6 |
| work-stealing 次數 | `perf stat` / tokio trace | P3.6 後趨近 0（熱路徑） | P3.6 |
| end-to-end Subscribe 延遲 | 自建 client timestamp | 依業務 SLA | 全 Phase |
| CPU cycles per message | `perf record` | 對比 Phase 0 baseline | P0、PGO |
| 上下文切換次數 | `perf stat -e context-switches` | P3.5 後顯著下降 | P3.5 |
| cache-misses | `perf stat -e cache-misses` | P0.5 後下降 | P0.5 |

```bash
# 範例：release + flamegraph（建立 Phase 0 baseline）
cargo build --release -p marketdata-service
perf record -g ./target/release/marketdata-service
# 跑 feed-sim + client subscribe 後分析
perf script | inferno-collapse-perf | inferno-flamegraph > flame.svg

# 範例：綁核前後對比尾部延遲
SIM_RATE_HZ=5000 cargo run --release -p marketdata-service &
cargo run --release --bin client  # 記錄 p50/p99
# 啟用 P0.5 後重複，對比 hdrhistogram
```

---

## 參考檔案

| 檔案 | 角色 |
|------|------|
| `proto/marketdata.proto` | Wire schema，P1 首要修改點 |
| `crates/marketdata-service/src/grpc.rs` | `book_to_proto`、Subscribe wire-pump（P2–P3、P3.5） |
| `crates/marketdata-service/src/bus.rs` | fan-out / fan-in（P3.5 rtrb 替換點） |
| `crates/marketdata-service/src/ingest.rs` | ingest 線程（P0.5 綁核點） |
| `crates/marketdata-service/src/main.rs` | Tokio 雙 Runtime 組裝（P0.5 綁核、P3.6 分離） |
| `crates/marketdata-service/src/lib.rs` | `Service::run` 生命週期 |
| `crates/marketdata-service/build.rs` | prost 生成配置 |
| `crates/marketdata-types/src/lib.rs` | 內部 `BookMessage` 佈局基準 |
| `crates/marketdata-service/README.md` | 架構總覽與 env 變數 |
| `Cargo.toml` | Release profile（P0） |

---

## 不建議的優化（避免過度工程）

- **不要** 在 `Bus::publish` 或 ingest 路徑加 `async`/`.await` — 破壞 I1
- **不要** 把 `Subscribe` 改回 `send().await` — 慢客戶端會回壓 ingest
- **不要** 為了消除 `Vec` 而把 `repeated` 改成 10 個獨立欄位（`bid_0`…`bid_9`）— proto 可讀性極差，收益不如手動 encode
- **不要** 在 assignment / 3-day scope 內一次性切換到 SBE — 先完成 P0–P2 投入產出比更高
- **不要** 盲目將全域公共服務層改為 UDP/QUIC 或 Raw TCP — 公共 API **維持 gRPC**；極致延遲需求走 P4 私有 sidecar 或 co-location IPC
- **不要** 替換 Tokio 為 Monoio / smol / async-std — tonic 生態綁定太深，本專案已定案在 Tokio 內壓榨（見維度 7、附錄 A）
- **不要** 把 wire-pump `tokio::spawn` 到 global `multi_thread` pool — 應遷到 `current_thread` runtime 或 `std::thread`（P3.6）
- **不要** 把 `rtrb::Consumer` 跨 thread 傳給 `tokio::spawn` — SPSC 不實作 `Send`，違反 Rust 並發安全
- **不要** 用 `disruptor` 替換整個 `Bus` 若僅有少量 subscriber — `rtrb` SPSC 更簡單；disruptor 適合 1→N 高頻 fan-out
- **不要** 在 wire-pump 路徑引入額外 tokio task — 每多一層 task 就多一次 work-stealing 抖動
- **不要** 對熱路徑使用 `tokio = { features = ["full"] }` 的隱式預設 — 完成 P3.7 feature 審計