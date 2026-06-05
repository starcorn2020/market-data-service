# 低延遲優化指南（Improve Guideline）

> 針對 `market-data-service` 的 gRPC / Protobuf Wire 層與編譯配置，整理可漸進落地的效能優化方向。
> 審閱基準：`proto/marketdata.proto`、`crates/marketdata-service/src/grpc.rs`、`crates/marketdata-types/src/lib.rs`。

---

## 現狀診斷

### 已做好的部分（保留，勿破壞）

| 區域 | 現狀 | 評價 |
|------|------|------|
| 內部資料模型 | `BookMessage` 為 `#[repr(C)]` + `Copy`，固定深度 `[BookLevel; 10]` | ✅ 與 SBE / raw wire 天然對齊 |
| Ingest 熱路徑 | `std::thread` + `Bus::publish` 非阻塞 | ✅ 符合 I1 不變量 |
| Subscribe 背壓 | `try_send` + 累積 `dropped_total`，嚴禁 `send().await` | ✅ 慢消費者隔離正確 |
| 架構分層 | `Upstream` trait 隔離 `feed-sim` | ✅ 未來可換 iceoryx2 / kernel bypass |

### 主要瓶頸（Wire 轉換層）

內部 `BookMessage` 是 stack `Copy`（約 400 bytes），但 `book_to_proto` 每筆推送至少觸發：

1. `figi.as_str().to_string()` → **Heap #1**（`String`）
2. `bids.iter().map(...).collect()` → **Heap #2**（`Vec<Level>`）
3. `asks.iter().map(...).collect()` → **Heap #3**（`Vec<Level>`）
4. tonic/prost encode → **Heap #4**（序列化緩衝區）
5. 客戶端 decode → 對稱的 `String` + `Vec` 分配

**結論**：瓶頸不在 `Bus` 或 `Snapshot`，而在 **`BookMessage → proto::Book → prost encode` 這條鏈**。

---

## 優先級行動清單

按投入產出比排序；建議按 Phase 漸進落地，每步保持測試通過。

| 優先級 | 項目 | 改動範圍 | 預期收益 |
|--------|------|----------|----------|
| P0 | Release 編譯配置 | `Cargo.toml` | 零程式碼改動，整體 5–15% 延遲改善 |
| P1 | Proto 定長整數 + `bytes figi` | `.proto` + `grpc.rs` | 減少 varint bit-shift，消除 UTF-8 驗證 |
| P2 | 手動 encode / 預分配 `BytesMut` | `grpc.rs` | 消除每筆 3 次 heap alloc |
| P3 | 自訂 tonic `Codec` | 新模組 + `grpc.rs` | 跳過 `proto::Book` 中間結構 |
| P4 | 架構分流：gRPC 監控 + UDP/raw 交易 | 新傳輸層 | 局部專線路徑 tick-to-trade < 1µs |

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

## 維度 3：傳輸層與零拷貝（Zero-Copy）

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

---

## 建議實施路線圖

```text
Phase 0（本週）
  └─ P0: Cargo.toml release profile

Phase 1（1–2 天）
  └─ P1: proto fixed 整數 + bytes figi
     └─ 更新 grpc.rs 轉換 + 測試

Phase 2（3–5 天）
  └─ P2: Subscribe 路徑手動 encode + thread-local BytesMut
     └─ 基準測試：alloc 次數、encode 延遲

Phase 3（可選）
  └─ P3: 自訂 tonic Codec
  └─ PGO 壓測與 p99 驗證

Phase 4（架構級，按需）
  └─ UDP/raw wire 交易路徑（僅限對接特定防火牆白名單節點）
  └─ iceoryx2 / AF_XDP ingest 替換
```

---

## 量測與驗證

優化前後應記錄以下指標：

| 指標 | 工具 | 目標 |
|------|------|------|
| 每筆 BookUpdate heap alloc 次數 | `dhat-heap` / `heaptrack` | Subscribe 熱路徑 → 0 |
| encode 延遲（p50 / p99 / p999） | `criterion` / `perf stat` | p99 下降 > 30% |
| end-to-end Subscribe 延遲 | 自建 client timestamp | 依業務 SLA |
| CPU cycles per message | `perf record` | 對比 Phase 0 baseline |

```bash
# 範例：release + flamegraph
cargo build --release -p marketdata-service
perf record -g ./target/release/marketdata-service
# 跑 feed-sim + client subscribe 後分析
perf script | inferno-collapse-perf | inferno-flamegraph > flame.svg
```

---

## 參考檔案

| 檔案 | 角色 |
|------|------|
| `proto/marketdata.proto` | Wire schema，P1 首要修改點 |
| `crates/marketdata-service/src/grpc.rs` | `book_to_proto`、Subscribe wire-pump |
| `crates/marketdata-service/build.rs` | prost 生成配置 |
| `crates/marketdata-types/src/lib.rs` | 內部 `BookMessage` 佈局基準 |
| `crates/marketdata-service/src/bus.rs` | Ingest fan-out（已優化，勿引入阻塞） |
| `Cargo.toml` | Release profile（P0） |

---

## 不建議的優化（避免過度工程）

- **不要** 在 `Bus::publish` 或 ingest 路徑加 `async`/`.await` — 破壞 I1
- **不要** 把 `Subscribe` 改回 `send().await` — 慢客戶端會回壓 ingest
- **不要** 為了消除 `Vec` 而把 `repeated` 改成 10 個獨立欄位（`bid_0`…`bid_9`）— proto 可讀性極差，收益不如手動 encode
- **不要** 在 assignment / 3-day scope 內一次性切換到 SBE — 先完成 P0–P2 投入產出比更高
- **不要** 盲目將全域公共服務層改為 UDP/QUIC — 區塊鏈的分散式公網環境極其複雜且使用者眾多。在公共傳輸層強推 UDP 會遭遇嚴重的網路防火牆阻擋與相容性崩潰，屬於本末倒置。網路效能優化應優先導向在地化 IPC、共享記憶體（iceoryx2）或針對特定白名單節點的專用私有快路徑。
```