# `marketdata-service`

Market data middleware: consumes the `BookMessage` stream from `feed-sim` and
exposes both a request/response API (latest snapshot) and a pub/sub API
(real-time updates). Slow or disconnected subscribers do not affect ingest or
other subscribers.

---

## Architecture

```
  FeedSubscriber (feed-sim, black box)
            │  Upstream::receive  (trait abstraction, the only contact point)
            ▼
  ingest_loop  (std::thread, single-threaded)
    │  put snapshot  ─→  Snapshot (Arc<DashMap<Figi, BookMessage>>)
    └  publish bus   ─→  Bus (Arc<DashMap<Figi, broadcast::Sender>>)
                              │  subscribe → fan-in to bounded mpsc
                              ▼
                    tonic gRPC server
                      - unary         GetSnapshots
                      - server-stream Subscribe
```

Core guarantees:

- **Ingest hot path never blocks**: a single `std::thread`; fan-out uses `try_send` only, with **no `.await` and no held locks**.
- **Full subscriber isolation**: each subscriber owns an independent mpsc; when its ring buffer fills, messages are dropped and a `dropped_total` counter accumulates so the client can detect loss. Slow or disconnected subscribers never back-pressure ingest or other subscribers.
- **Vendor types do not leak**: `feed-sim` is touched only through the private `upstream::Upstream` trait; the public API does not expose `feed_sim::*` (swapping in iceoryx2 in the future requires changing a single file).

Where each of the six requirements lives: `ingest.rs` (§1) / `snapshot.rs` (§2) /
proto `SnapshotEntry` with `oneof { Found, NotYet }` per FIGI (§3) /
`bus.rs` + `grpc.rs` (§4) /
`0.0.0.0:50051` gRPC over HTTP/2 (§5) / `src/bin/client.rs` (§6).

Both RPCs accept a batch of FIGIs — `GetSnapshots` returns one
`SnapshotEntry` per requested FIGI (in request order), `Subscribe` fans
each of them into the same outgoing stream.

---

## Key design trade-offs

- **`DashMap` over `Arc<RwLock<HashMap>>`**: per-shard locking lets ingest write FIGI-A and an RPC read FIGI-B concurrently. A single `RwLock` would serialize all reads and writes.
- **Per-FIGI broadcast channel over a single global channel**: slow-consumer isolation becomes natural, and subscribers never have to deserialize traffic for FIGIs they do not care about.
- **Drop on full + cumulative `dropped_total`, not subscriber eviction**: reconnecting a gRPC stream is expensive; dropping individual messages is far lighter. The wire schema carries a cumulative counter, so the client can diff successive values to know exactly how much it missed.
- **Ingest uses `std::thread` rather than `tokio::task`**: `FeedSubscriber` is synchronous, blocking, and busy-polls; running it on a tokio worker would consume an entire core.
- **`Upstream` trait + generic static dispatch (not `Box<dyn>`)**: swapping in iceoryx2 only touches `upstream/feed_sim.rs`, with zero changes elsewhere. It also lets integration tests inject a `MockUpstream` for deterministic verification.
- **`BoxError = Box<dyn Error + Send + Sync>` instead of `anyhow`**: the boundary error type is already sufficient, with no extra dependency.

---

## Build / Test / Run

> Run from the workspace root. Commands use PowerShell; for bash, replace `$env:NAME="value"` with `NAME=value cmd`. `protoc` is injected automatically by `build.rs`, no system dependency required.

### Build + Test

```powershell
cargo build --release --workspace
cargo test --workspace
# Expected: 63 passed + 3 ignored
#   - service: 30 unit + 8 grpc_basic + 1 ignored (grpc_slow_consumer wire stress)
#   - feed-sim: 17 unit + 2 integration; types: 5
#   - doctests: 1 passed + 2 ignored
```

### Demo (server + client)

Use two terminals. Start the client only after the server prints `[server] listening on ...`:

```powershell
# Terminal A — Server
$env:SIM_INSTRUMENTS="10"; $env:SIM_RATE_HZ="1000"
cargo run -p marketdata-service

# Terminal B — Client (auto-exits after 3 seconds)
cargo run --bin client
```

Acceptance signal: the client prints `GetSnapshots(BBG000000001) -> Found(seq=..., bids=5, asks=5)` and the Subscribe loop ends with `dropped_total=0`.

- To see the full wire payload: `$env:MDS_CLIENT_VERBOSE="1"` (additionally dumps the full `Book` / `BookUpdate` proto structure).
- Cross-host: start the server on host A, then on host B set `$env:MDS_CLIENT_TARGET="http://<host-a-lan-ip>:50051"`.

### Demo (server + `grpcurl`, no Rust client needed)

Useful for reviewers who want to poke the wire directly without building the
sample client. Server reflection is intentionally not enabled (keeps the
binary lean and the wire surface explicit), so `grpcurl` must be pointed at
the proto file via `-proto`.

Install `grpcurl` (one-time, pick whichever fits your machine):

```powershell
# Windows (Scoop)
scoop install grpcurl
# macOS / Linux (Homebrew)
brew install grpcurl
# Linux (no native package on most distros): download the latest tarball
# from https://github.com/fullstorydev/grpcurl/releases and extract into a
# directory on $PATH, e.g.:
curl -sSL https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_x86_64.tar.gz | sudo tar -xz -C /usr/local/bin grpcurl
# Any platform with Go toolchain
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

If you do not have Go yet, install it first (only needed for the last line above):

```powershell
# Windows (winget / Scoop / Chocolatey — pick one)
winget install GoLang.Go
scoop install go
choco install golang

# macOS
brew install go

# Linux (Debian / Ubuntu)
sudo apt install -y golang-go
# Linux (Fedora / RHEL)
sudo dnf install -y golang
# Linux (Arch)
sudo pacman -S go

# Any OS — official tarball
# https://go.dev/dl/
```

With the server already running (`cargo run -p marketdata-service`), from
the workspace root:

```powershell
# Unary: GetSnapshots (batch — pass any number of FIGIs)
grpcurl -plaintext `
    -import-path proto -proto marketdata.proto `
    -d '{\"figis\": [\"BBG000000001\", \"BBG000000002\"]}' `
    127.0.0.1:50051 marketdata.v1.MarketData/GetSnapshots

# Server-streaming: Subscribe (Ctrl-C to stop)
grpcurl -plaintext `
    -import-path proto -proto marketdata.proto `
    -d '{\"figis\": [\"BBG000000001\"]}' `
    127.0.0.1:50051 marketdata.v1.MarketData/Subscribe
```

Expected `GetSnapshots` response shape (one `SnapshotEntry` per requested
FIGI, in request order; each entry echoes its `figi`):

```json
{
  "entries": [
    {
      "figi": "BBG000000001",
      "found": {
        "figi": "BBG000000001",
        "gatewaySeq": "123",
        "gatewayTs": "1700000000000000000",
        "bids": [ { "price": 100.5, "qty": 1.0, "orders": 3 }, ... ],
        "asks": [ { "price": 101.0, "qty": 0.5, "orders": 1 }, ... ]
      }
    },
    {
      "figi": "BBG000000002",
      "notYet": {}
    }
  ]
}
```

`{ "notYet": {} }` is exactly the "clearly-defined no data yet" signal
carried by `SnapshotEntry.oneof` — distinct from "this FIGI does not
exist" (the server cannot tell those apart in 3-day scope).

For bash, drop the backticks and escape the inner quotes the usual way
(`-d '{"figis": ["BBG000000001"]}'`).

---

## Env vars


| Env                                    | Default                  | Purpose                                                                  |
| -------------------------------------- | ------------------------ | ------------------------------------------------------------------------ |
| `MDS_LISTEN`                           | `0.0.0.0:50051`          | gRPC server listen address                                               |
| `MDS_BUS_CAPACITY`                     | `1024`                   | Per-FIGI broadcast channel capacity                                      |
| `MDS_SUBSCRIBER_QUEUE`                 | `1024`                   | Per-subscriber mpsc capacity                                             |
| `MDS_POLL_INTERVAL_MS`                 | `50`                     | Ingest `wait()` poll interval                                            |
| `SIM_INSTRUMENTS`                      | `100`                    | Number of simulated FIGIs                                                |
| `SIM_RATE_HZ`                          | `1000`                   | Aggregate rate (msg/s)                                                   |
| `SIM_MAX_MESSAGES`                     | (unlimited)              | Message cap that triggers EOF                                            |
| `MDS_CLIENT_TARGET`                    | `http://127.0.0.1:50051` | Client connection endpoint                                               |
| `MDS_CLIENT_FIGI` / `MDS_CLIENT_FIGIS` | `BBG000000001`           | FIGI for the client to query / subscribe (the latter is comma-separated) |
| `MDS_CLIENT_SECS`                      | `3`                      | Client `Subscribe` duration in seconds                                   |
| `MDS_CLIENT_VERBOSE`                   | (unset)                  | Setting any value enables pretty-printing of the wire payload            |


feed-sim's own env vars (`SIM_SEED` / `SIM_DEPTH` / `SIM_PACING` / `SIM_BUFFER_SIZE` /
`SIM_START_SEQ`) are documented under `crates/feed-sim/`.

---

## Non-goals (aligned with the assignment's Out of scope)

Deliberately omitted:

- Persistence / auth / TLS (snapshot is purely in-memory, gRPC is plaintext)
- L3 book reconstruction (`Snapshot::put` performs a full overwrite)
- HA / failover / multi-region (single process, single endpoint)

## Future work

- Real iceoryx2 replacement for `feed-sim` — the `Upstream` trait paves the way; only one file changes.
- Production graceful shutdown — `Service::run` would add a shared shutdown channel on the ingest-EOF branch so in-flight RPCs can drain gracefully.
- Cross-host automated testing (docker compose / SSH tunnel) — currently verified by manual commands.

