# Exchange Connectors

![CI](https://github.com/ai-crypto-onramp/gateway-exchange/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/ai-crypto-onramp/gateway-exchange/branch/main/graph/badge.svg)](https://codecov.io/gh/ai-crypto-onramp/gateway-exchange)

Venue-specific adapters (Binance, Kraken, OTC desks) — order placement, fills, and balances for the crypto on-ramp.

## Overview / Responsibilities

Exchange Connectors are the venue adapters that translate internal trading intents
into venue-specific protocol calls and stream back fills, balances, and market data.
They sit behind **Liquidity Routing**, which selects venues and sizes orders; the
connectors execute against the venue and emit fill/balance events asynchronously to
**Reconciliation** and the **Audit Event Log**.

Core responsibilities:

- Place, cancel, and track orders on each supported venue.
- Stream fills and balance updates to downstream consumers.
- Subscribe to venue order books (WebSocket) for market data.
- Enforce per-venue rate limits and credential hygiene.
- Expose a uniform interface so Liquidity Routing is agnostic to the underlying venue.

## Language & Tech Stack

- **Language:** Go (transactional backbone — concurrency, latency, ops maturity).
- **Deployment shape:** one deployable per venue family (binance, kraken, otc),
  each implementing a common `VenueConnector` interface. This isolates venue
  outages, rate limits, and dependency surface per process.
- **Transport:** WebSocket for market data (order book, trades) and REST for order
  management (place/cancel/status) and balance queries.
- **Internal API:** gRPC for callers (Liquidity Routing); internal REST for admin.

## System Requirements

- **Common interface.** Every deployable implements the same contract:

  | Method | Purpose |
  |---|---|
  | `PlaceOrder` | Submit a new order with idempotency key (`client_order_id`) |
  | `CancelOrder` | Cancel an open order by venue order id or client id |
  | `GetOrder` | Fetch current order state |
  | `GetFills` | Fetch executions for an order (or window) |
  | `GetBalances` | Fetch spot/asset balances held at the venue |
  | `SubscribeOrderBook` | Stream order book updates over WebSocket |

- **Per-venue adapters.** At minimum: Binance, Kraken, and one or more OTC desks
  (bespoke REST/SFTP workflows, no public market data stream).
- **Order book WS subscription.** Maintain a resilient WS connection per symbol,
  snapshot + diff book reconstruction, sequence-gap detection.
- **Balance sync.** Periodic REST poll + WS balance event stream where supported;
  reconcile against internal ledger expectations.
- **Rate limit handling per venue.** Token-bucket / weighted cost per endpoint
  following each venue's documented limits (e.g. Binance X-MBX-USED-WEIGHT-1M,
  Kraken counter system). Back off on 429/418.
- **Credential rotation.** Read keys from the secrets vault at startup and on
  rotation signal; never log or persist secrets to disk.
- **Fill streaming.** Emit each fill as a typed event to the event bus for
  Reconciliation and Audit; include venue order id, internal order id, price,
  size, fee, and timestamp.

## Non-Functional Requirements

- **Per-venue circuit breaker.** Trip on error-rate / latency thresholds; stop
  outbound order traffic to that venue and surface a degraded status to
  Liquidity Routing. Auto-half-open probe after cooldown.
- **Idempotent order placement.** Every `PlaceOrder` carries a `client_order_id`
  generated upstream; venue-side dedup guarantees retries do not double-place.
- **Reconnect with backoff on WS.** Exponential backoff with jitter on socket
  disconnect; resubscribe to book + private channels after reconnect; detect
  sequence gaps and force a snapshot refresh.
- **Per-venue rate limit compliance.** Never exceed venue limits; reject or queue
  internal requests when budget is exhausted rather than risking a ban.
- **Observability.** Structured logs, Prometheus metrics (order latency, fill
  latency, WS reconnect count, rate-limit headroom, circuit state), traces
  propagated from Liquidity Routing.

## Technical Specifications

### Common interface (Go pseudocode)

```go
type VenueConnector interface {
    PlaceOrder(ctx context.Context, req OrderRequest) (*VenueOrder, error)
    CancelOrder(ctx context.Context, req CancelRequest) error
    GetOrder(ctx context.Context, venueOrderID string) (*VenueOrder, error)
    GetFills(ctx context.Context, q FillQuery) ([]Fill, error)
    GetBalances(ctx context.Context, assets []string) (Balances, error)
    SubscribeOrderBook(ctx context.Context, symbol string, ch chan<- BookUpdate) error
}

type OrderRequest struct {
    ClientOrderID string
    Symbol        string
    Side          Side
    Type          OrderType
    Quantity      decimal.Decimal
    Price         decimal.Decimal
}

type VenueOrder struct {
    VenueOrderID string
    ClientOrderID string
    Status       OrderStatus
    FilledQty    decimal.Decimal
    AvgPrice     decimal.Decimal
}

type Fill struct {
    VenueOrderID string
    TradeID      string
    Price        decimal.Decimal
    Quantity     decimal.Decimal
    Fee          decimal.Decimal
    Timestamp    time.Time
}
```

### Per-venue configuration

| Venue | REST base | WebSocket | Notes |
|---|---|---|---|
| Binance | `https://api.binance.com` (spot) | `wss://stream.binance.com:9443/ws` | Edge auth, X-MBX headers, weight-based rate limits |
| Kraken | `https://api.kraken.com` | `wss://ws.kraken.com` (public) / `wss://ws-auth.kraken.com` (private) | API key + nonce signing; counter-based rate limits |
| OTC | `OTC_DESK_URL` (bespoke REST/SFTP) | none | Quote-request flow, RFQ, manual settlement confirmations |

### Endpoints

**gRPC (called by Liquidity Routing):**

| Method | Description |
|---|---|
| `PlaceOrder` | Forward order to venue, return `VenueOrder` |
| `CancelOrder` | Cancel by venue or client order id |
| `GetFills` | Fetch fills for an order or time window |

**Internal REST (admin / ops):**

- `GET /healthz` — liveness
- `GET /readyz` — readiness (incl. WS + venue REST reachability)
- `GET /admin/status` — circuit breaker state, rate-limit headroom, last fill
- `POST /admin/rotate-credentials` — trigger credential reload from vault

### Data model

PostgreSQL tables (per-venue deployable shares schema):

- `venue_orders` — `venue_order_id`, `client_order_id`, `venue`, `symbol`,
  `side`, `type`, `qty`, `price`, `status`, `created_at`, `updated_at`.
- `venue_fills` — `venue_order_id`, `trade_id`, `price`, `qty`, `fee`,
  `fee_asset`, `ts`.
- `venue_balances` — `venue`, `asset`, `free`, `locked`, `ts`.
- `venue_credentials` — references to vault secret versions only (no secret
  material stored); `venue`, `key_id`, `vault_path`, `rotated_at`.

### Integrations

- **Called by:** `liquidity-routing` (gRPC, synchronous on the transaction path).
- **Streams to:** `reconciliation` (fills + balances, async via event bus) and
  `audit-event-log` (every order/fill/credential event, async).
- **Secrets:** reads API keys from the platform secrets vault.

### Credential management

- Per-venue API keys stored in the secrets vault under
  `secret/exchange-connectors/<venue>/api-key` and `.../api-secret`.
- Connectors hold only a lease; rotation is performed in the vault and pushed to
  the connector via a rotation signal or `POST /admin/rotate-credentials`.
- Old keys are revoked only after the connector confirms the new key is active.
- Secrets are never logged, never returned in API responses, never written to disk.

## Dependencies

- Per-venue public APIs (Binance, Kraken) and OTC desk REST/SFTP endpoints.
- PostgreSQL — local state for `venue_orders`, `venue_fills`, `venue_balances`.
- Secrets vault (e.g. HashiCorp Vault / cloud KMS-backed secret store).
- `audit-event-log` — consumer for the event bus.
- Shared libraries: decimal arithmetic, gRPC client/server stubs, WS client with
  backoff, metrics + tracing SDKs.

## Configuration

Environment variables (non-secret values; secrets come from the vault):

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | gRPC listen port |
| `VENUE_FAMILY` | `binance` | Which adapter binary to run (`binance`, `kraken`, `otc`) |
| `BINANCE_REST_URL` | `https://api.binance.com` | Binance REST base URL |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/ws` | Binance WS base URL |
| `BINANCE_API_KEY` | — | Binance API key (prod: from vault) |
| `BINANCE_API_SECRET` | — | Binance API secret (prod: from vault) |
| `KRAKEN_REST_URL` | `https://api.kraken.com` | Kraken REST base URL |
| `KRAKEN_WS_URL` | `wss://ws.kraken.com` | Kraken public WS URL |
| `KRAKEN_API_KEY` | — | Kraken API key (prod: from vault) |
| `KRAKEN_API_SECRET` | — | Kraken API secret (prod: from vault) |
| `OTC_DESK_URL` | — | OTC desk REST base URL |
| `OTC_API_KEY` | — | OTC desk API key (prod: from vault) |
| `DB_URL` | `postgres://localhost:5432/exchange?sslmode=disable` | PostgreSQL DSN |
| `VAULT_ADDR` | `http://localhost:8200` | Secrets vault address |
| `VAULT_ROLE` | `exchange-connectors` | Vault auth role |
| `AUDIT_EVENT_LOG_URL` | — | gRPC address of audit-event-log |
| `EVENT_BUS_URL` | `kafka://localhost:9092` | Event bus for fills/balances |
| `RATE_LIMIT_WEIGHT_BUDGET` | `1200` | Per-venue weight budget (Binance default) |
| `CIRCUIT_BREAKER_THRESHOLD` | `0.3` | Error-rate to trip the breaker |
| `WS_RECONNECT_MAX_BACKOFF` | `30s` | Max backoff for WS reconnect |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

## Local Development

The service is built as **one deployable per venue family**, selected by
`VENUE_FAMILY`. The same source tree compiles all three adapters; the selected
adapter is wired at startup.

```bash
# Build all venue binaries
go build -o bin/exchange-connectors ./cmd/exchange-connectors

# Run a specific venue adapter
VENUE_FAMILY=binance PORT=8081 ./bin/exchange-connectors
VENUE_FAMILY=kraken  PORT=8082 ./bin/exchange-connectors
VENUE_FAMILY=otc     PORT=8083 ./bin/exchange-connectors

# Run tests
go test ./...

# Run tests with coverage + race detector
go test -race -cover ./...

# Lint
golangci-lint run ./...

# Generate gRPC stubs
buf generate
```

> Note: because each venue is a separate deployable, you can scale, roll back, and
> isolate incidents (e.g. a Binance outage) without affecting Kraken or OTC flows.
