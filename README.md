# Take-Home Assignment: `market-data-service`

## The problem

A feed gateway parses exchange protocols and emits a continuous stream
of order-book updates — one `BookMessage` per update, across many
instruments. Downstream of you are mixed clients: notebooks that want
occasional snapshots, dashboards subscribed to a few instruments,
trading systems subscribed to many. **Build the middleware that lets
each get what they need without stepping on each other.**

## What to build

A Rust crate `market-data-service` that:

1. Consumes `BookMessage`s from the provided source.
2. Maintains a per-instrument latest-book snapshot, keyed by `Figi`.
3. **Request/response API** — client asks for the latest snapshot of a
   given `Figi`, gets one back (or a clearly-defined "no data yet").
4. **Pub/sub API** — client subscribes to one or more `Figi`s and
   receives book updates as they arrive. A slow or disconnected
   subscriber must not affect the others or the ingest path.
5. Works for clients on the **same host and on a remote machine**.
   Same wire protocol or different — your call, justify it.
6. A sample client demonstrating both APIs end-to-end.

The design decisions are the point: transport, fan-out, snapshot
representation, what "subscribe" means on the wire. Write them down.

## What's provided

| Path | What it is |
|---|---|
| `crates/marketdata-types/` | Shared types. You need `BookMessage` and what it references (`BookLevel`, `Figi`, `BookFlags`, `ConditionFlags`, `ExchangeHeader`). |
| `crates/feed-sim/` | Simulated upstream. Black box; same subscriber-style API as the production iceoryx2 source. See `crates/feed-sim/README.md`. |

```sh
cargo build
cargo run --example print_messages -p feed-sim   # smoke-test the simulator
cargo test
```

Add your crate with `cargo new --lib crates/market-data-service` and
register it under `[workspace].members` in the root `Cargo.toml`.

## Non-goals

Out of scope — do not implement:

- The transport from feed gateway to source (hidden on purpose).
- Persistence, auth, TLS.
- Building an L3 book from increments — `BookMessage` is already a
  top-10 snapshot.
- HA, failover, multi-region.

We may discuss these later, but they shouldn't be in your code.

## Logistics

- Spend roughly **3 days**.
- Deliver as a zip archive.
- Include build and run instructions in the README.
- We'll schedule a follow-up to run your demo and dig into the design.

Good luck.
