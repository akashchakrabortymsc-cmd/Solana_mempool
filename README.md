# solana-mempool

Real-time transaction monitor for the Solana network, written in async Rust.

Subscribes to the validator stream at `processed` commitment — the earliest observable moment before confirmation — decodes every pending transaction, scores it for MEV signals, persists to SQLite, and exposes a live Server-Sent Events feed for downstream consumers.

---

## What it does

- Taps the Solana WebSocket at `processed` commitment via `logsSubscribe`, seeing transactions before they are confirmed
- Enriches each transaction with real fee data (lamports, compute units) via `getTransaction` RPC with a concurrency-limited semaphore
- Deduplicates the stream using a concurrent `DashMap` set (Solana sends duplicate notifications)
- Computes rolling fee percentiles (p25 / p50 / p75 / p95) over a 10,000-transaction sliding window
- Detects MEV candidates: Jito bundle tip accounts, fee-surge outliers (>5× p95 baseline), and liquidation-protocol interactions (Marginfi, Kamino)
- Persists all transactions to SQLite via `sqlx` with async non-blocking writes
- Exposes Prometheus metrics at `/metrics` — consumable directly by Grafana
- Streams live transaction events to the frontend via Server-Sent Events at `/stream`
- Reconnects automatically on WebSocket drop using exponential backoff

---

## Architecture

```
Solana RPC WebSocket
        │
        ▼  logsSubscribe (processed commitment)
   rpc.rs  ──────────────────────────────────────────►  tokio::mpsc channel
                                                               │
                                                               ▼
                                                        processor.rs
                                                     ┌─────────────────┐
                                                     │  decode logs     │
                                                     │  enrich fees     │
                                                     │  deduplicate     │
                                                     │  score MEV       │
                                                     └────────┬────────┘
                                          ┌──────────────────┼──────────────────┐
                                          ▼                  ▼                  ▼
                                     storage.rs          metrics.rs       broadcast::channel
                                     SQLite              Prometheus            │
                                                         /metrics              ▼
                                                                           api.rs
                                                                      /stream  (SSE)
                                                                      /fee-stats
                                                                      /recent
                                                                      /health
```

---

## API

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness check — returns `200 OK` |
| `/fee-stats` | GET | Current fee percentiles (p25/p50/p75/p95) and sample size |
| `/recent` | GET | Last 100 transactions, newest first |
| `/stream` | GET | Server-Sent Events stream — one JSON event per transaction |
| `/metrics` | GET | Prometheus metrics |

### Example `/fee-stats` response

```json
{
  "p25_lamports": 5000,
  "p50_lamports": 25000,
  "p75_lamports": 100000,
  "p95_lamports": 750000,
  "sample_size": 10000
}
```

### Example `/stream` event

```json
{
  "signature": "4xK7...",
  "slot": 312847291,
  "fee_lamports": 980000,
  "compute_units_consumed": 142000,
  "program_ids": ["T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt"],
  "received_at_ms": 1718520041823,
  "mev_score": 0.9,
  "mev_signal": "JitoBundle"
}
```

---

## MEV signal scoring

Each transaction receives a `mev_score` between `0.0` (clean) and `1.0` (high-confidence MEV candidate).

| Signal | Score | Detection logic |
|---|---|---|
| `JitoBundle` | 0.90 | Transaction touches the Jito tip program (`T1pyy...`) |
| `LiquidationTarget` | 0.85 | High fee AND interacts with Marginfi or Kamino |
| `HighFeeSurge` | 0.60 | Fee exceeds 5× the rolling p95 baseline |
| `LiquidationTarget` | 0.50 | Liquidation protocol interaction, normal fee |
| `None` | 0.00 | No signals detected |

---

## Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `mempool_tx_received_total` | Counter | Total transactions seen since startup |
| `mempool_mev_detected_total` | Counter | Transactions with `mev_score > 0` |
| `mempool_fee_p50_lamports` | Gauge | Rolling median fee in lamports |
| `mempool_fee_p95_lamports` | Gauge | Rolling p95 fee in lamports |
| `mempool_processing_latency_ms` | Histogram | End-to-end processing latency per transaction |

---

## Stack

| Crate | Purpose |
|---|---|
| `tokio` | Async runtime |
| `solana-client` | `PubsubClient` WebSocket subscription and `RpcClient` fee enrichment |
| `solana-transaction-status` | Transaction decoding (`UiTransaction`, `EncodedTransaction`) |
| `axum` | HTTP API and SSE stream |
| `sqlx` | Async SQLite persistence |
| `dashmap` | Lock-free concurrent deduplication set |
| `prometheus` | Metrics registry |
| `tokio-retry` | Exponential backoff reconnection |
| `serde` / `serde_json` | Serialization |
| `tracing` | Structured logging |
| `anyhow` | Error handling |

---

## Getting started

### Prerequisites

- Rust 1.78+ (`rustup update stable`)
- SQLite (`apt install libsqlite3-dev` on Debian/Ubuntu)

### Run

```bash
git clone https://github.com/<you>/solana-mempool
cd solana-mempool
cargo build --release
./target/release/solana-mempool
```

By default the binary connects to `wss://api.mainnet-beta.solana.com`. For production use, set a private RPC endpoint (Helius or Triton recommended — the public endpoint throttles aggressively):

```bash
export SOLANA_WS_URL="wss://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>"
export SOLANA_RPC_URL="https://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>"
./target/release/solana-mempool
```

### Configuration

`config.toml`:

```toml
ws_url = "wss://api.mainnet-beta.solana.com"
rpc_url = "https://api.mainnet-beta.solana.com"
db_path = "mempool.db"
api_port = 8080
enrichment_concurrency = 5     # max parallel getTransaction calls
fee_window_size = 10000        # sliding window for percentile computation
```

---

## Project structure

```
src/
├── main.rs          # startup, channel wiring, axum server
├── rpc.rs           # WebSocket subscription and reconnect loop
├── processor.rs     # decode, enrich, deduplicate, score
├── types.rs         # PendingTx, FeeStats, MevScore, MevSignalType
├── storage.rs       # SQLite schema and async insert
├── api.rs           # axum route handlers and SSE stream
└── metrics.rs       # Prometheus registry and gauge updates
```

---

## Design notes

**Why `logsSubscribe` at `processed` commitment?**
This is the earliest observable signal on Solana — before `confirmed` (66% stake supermajority) and well before `finalized` (32 blocks deep). The latency advantage matters for MEV-aware applications.

**Why not `programSubscribe`?**
`programSubscribe` filters by a single program. `logsSubscribe` with `RpcTransactionLogsFilter::All` gives the full transaction flow, which is necessary for cross-program interaction detection and global fee modelling.

**Why a separate enrichment step for fees?**
The `logsSubscribe` notification does not include fee data. A follow-up `getTransaction` call is required per transaction. The enrichment step runs behind a semaphore (configurable, default 5 concurrent calls) to stay within public RPC rate limits. With a paid Helius endpoint, `transactionSubscribe` eliminates this step entirely.

**Why SQLite and not Postgres?**
`INSERT OR IGNORE` deduplication, zero-dependency deployment, and sufficient write throughput for Solana's ~2,000 TPS peak. Migrate to TimescaleDB or QuestDB when time-series queries over fee history become the bottleneck.

---

## Roadmap

- [ ] `transactionSubscribe` via Helius geyser (eliminates enrichment round-trip)
- [ ] NATS pubsub output (`solana.mempool.transactions`, `solana.mempool.mev`)
- [ ] Sub-slot latency model (ms after slot boundary per transaction)
- [ ] Sandwich detection via same-account reverse-direction window
- [ ] Jupiter/Raydium swap size estimation for large-swap MEV signal
- [ ] QuestDB time-series backend for fee history queries

---

## License

MIT
