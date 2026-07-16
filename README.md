# MNEMON

*(Gk. Μνήμων, "the one who remembers" — the record-keeper behind the
MYRMIDONS stack, feeding HEGEMON and EREBUS.)*

A local, queryable, reproducible historical store of Morpho market data —
the data layer for quantitative research on vault strategies (backtests,
risk analysis). Python ingestion on a 5-min scheduler, Parquet storage
partitioned by date, queried via DuckDB.

**Principle: store raw state, derive metrics at query time.** Asset/share
totals are stored as exact integers; APYs, utilization thresholds, price
returns etc. are computed in SQL from the raw series.

## Data sources (all free)

1. **Morpho GraphQL API** (`blue-api.morpho.org/graphql`) — primary, incl.
   hourly historical backfill. See [docs/SCHEMA_NOTES.md](docs/SCHEMA_NOTES.md)
   for what historical fields actually exist (introspected, verified on
   HyperEVM) and the API's gotchas.
2. **DefiLlama free endpoints** — `coins.llama.fi` (prices),
   `yields.llama.fi/pools` (venue yields).
3. **Public HyperEVM RPC** — view-call fallback only, used when the Morpho
   API has no state for a tracked market.

All requests go through one shared HTTP client with a global minimum
request interval and exponential backoff.

## Setup

Requires [uv](https://docs.astral.sh/uv/) (`brew install uv`).

```bash
uv sync                                  # create .venv, install deps
uv run pytest                            # unit tests (offline, fixture-based)
uv run python -m mnemon discover         # sanity check: prints tracked markets
uv run python -m mnemon run              # first run: ingests + full backfill
```

The first `run` backfills every tracked market's hourly history since
creation (plus vault allocations and token prices), so expect a few minutes.
Subsequent runs are incremental and take seconds.

### Scheduling

One entrypoint (`run_mnemon.sh`), invoked every 5 minutes; it internally
decides which jobs are due from the last-success timestamps in
`data/mnemon_state.json`.

**Local (macOS/Linux) via cron** — redirect output yourself:

```
*/5 * * * * /path/to/mnemon/run_mnemon.sh >> /path/to/mnemon/data/logs/cron.log 2>&1
```

**On a VPS via systemd timer** — recommended for a remote box (logs to
journald, survives reboots). See [docs/DEPLOY.md](docs/DEPLOY.md); unit files
are in [`systemd/`](systemd/).

Job cadences:

| job               | cadence | table              | content                                   |
|-------------------|---------|--------------------|-------------------------------------------|
| market_state      | 5 min   | `market_state`     | raw totals, rateAtTarget, oracle price     |
| vault_allocations | 15 min  | `vault_allocations`| per-market supply + cap per vault          |
| prices            | 15 min  | `prices`           | USD prices (llama, morpho fallback)        |
| positions         | hourly  | `positions`        | current borrower positions (accumulates forward) |
| markets           | daily   | `markets`          | dimension: tokens, decimals, lltv, oracle; also triggers backfill of newly seen entities |
| yield_pools       | 6 h     | `yield_pools`      | competing venue yields on tracked chains   |
| heal              | daily   | (repairs the above)| re-pulls recent hourly history, inserts only missing buckets — outage gaps self-repair |

The scheduler tick (systemd timer / cron line) fires every 5 minutes — no
cadence can be shorter than that. This is an analytics archive, not a live
feed: anything that needs sub-minute state (liquidation triggers, execution
checks) belongs in the consumer's own on-chain loop.

Failures in one job never abort the others; logs rotate in `data/logs/`.

If the Morpho API has an outage, the affected 15-min buckets are lost at
15-min granularity, but the daily `heal` job re-pulls the last
`heal_lookback_hours` (default 48) of hourly history and fills whatever is
missing — never overwriting live rows. After a longer outage, widen the
window once: `uv run python -m mnemon heal --hours 168`.

### Market discovery

No hand-maintained market list. `config.yaml` holds **vault addresses**;
every run derives the tracked set = union of markets those vaults currently
allocate into (+ optional `extra_markets`). When a vault adds a market,
tracking starts automatically and its full hourly history is backfilled at
the next daily `markets` job (or immediately via `python -m mnemon backfill`).

## Commands

```bash
uv run python -m mnemon run                  # run due jobs (what cron calls)
uv run python -m mnemon run --only prices    # force specific jobs
uv run python -m mnemon backfill             # backfill anything not yet backfilled
uv run python -m mnemon backfill --force     # re-pull all history
uv run python -m mnemon heal --hours 72      # fill outage gaps from hourly history
uv run python -m mnemon check                # data-quality report: gaps, nulls, last runs
uv run python -m mnemon migrate-legacy out/  # import old TS snapshot-*.json files
uv run python -m mnemon init-db              # refresh DuckDB views only
```

## Querying

The store *is* the API: everything lives in `data/mnemon.duckdb` (views over
the Parquet files) or the Parquet files themselves. Nothing to run, no server
— you point DuckDB at it read-only.

### Three ways to query

**1. DuckDB CLI** — best for ad-hoc exploration (one-time `brew install duckdb`):

```sh
duckdb data/mnemon.duckdb                     # interactive REPL
duckdb data/mnemon.duckdb -c "FROM v_vault_snapshot LIMIT 5"   # one-shot
```

**2. Python, ad-hoc** — no extra install; `.sql(...)` pretty-prints a table:

```sh
uv run python -c "import duckdb; print(duckdb.connect('data/mnemon.duckdb', read_only=True).sql('FROM v_liquidity_risk ORDER BY pct_time_gt99 DESC LIMIT 5'))"
```

**3. Python → pandas** — the integration point for downstream code (the
`myrmidons` metric library, the backtester). `.df()` returns a DataFrame:

```python
import duckdb

con = duckdb.connect("data/mnemon.duckdb", read_only=True)  # read-only: safe while cron writes
df = con.sql("""
    SELECT ts, utilization, apy_at_target, oracle_price
    FROM v_market_state
    WHERE collateral_symbol = 'kHYPE' AND loan_symbol = 'USD₮0'
    ORDER BY ts
""").df()
```

Downstream code doesn't even need the `.duckdb` file — it can read the
Parquet globs directly, which makes the dataset trivially portable:

```python
duckdb.sql("SELECT * FROM read_parquet('data/prices/*/*.parquet', hive_partitioning=1)")
```

### Views

Raw tables, exposed 1:1 (integer amounts, exact): `market_state`, `markets`,
`vault_allocations`, `positions`, `prices`, `yield_pools`, `legacy_snapshots`.

Derived convenience views (symbols joined, human units, metrics computed from
raw state):

| view | one row per | gives you |
|------|-------------|-----------|
| `v_market_state`        | market × timestamp | supply/borrow/liquidity in token units, utilization, `apy_at_target`, `oracle_price` |
| `v_market_snapshot`     | market (current) | newest state + full dimension in one row — the market screener |
| `v_vault_allocations`   | vault × market × ts | supply & cap in token units over time |
| `v_vault_snapshot`      | vault × market (current) | latest allocation with `weight_pct` and `cap_used_pct` |
| `v_liquidity_risk`      | market | all-history `pct_time_gt95/99`, `avg_util_pct`, current util / APY-at-target |
| `v_utilization_regime`  | market | trailing **7d/30d** utilization stats — the *current* regime |
| `v_position_risk`       | market (current) | borrower count, debt, `min_hf`, debt share with HF < 1.05, top-3 concentration |
| `v_vault_drift`         | reallocation event | every move of a market's vault share > 0.5pp: before/after weights, assets moved — newest first |
| `v_prices`              | token × ts | price with the token `symbol` attached |
| `v_price_returns`       | token × hour | hourly log-returns + rolling 7d/30d annualized vol |

All id columns (`market_id`, `vault`, `token_address`) are stored complete —
if they look truncated in a DataFrame print, that's pandas' 50-char display
default: `pd.set_option("display.max_colwidth", None)`.

### Example queries

Current allocation of a vault, richest-first:

```sql
SELECT collateral_symbol, ROUND(supply_assets) AS supplied,
       ROUND(weight_pct, 1) AS weight_pct, ROUND(cap_used_pct, 2) AS cap_used_pct
FROM v_vault_snapshot
WHERE vault = '0x4dc97f968b0ba4edd32d1b9b8aaf54776c134d42' AND supply_assets > 0
ORDER BY supply_assets DESC;
```

Withdrawal-liquidity risk — how often each market sat at extreme utilization:

```sql
SELECT loan_symbol, collateral_symbol, hours_observed,
       ROUND(pct_time_gt99, 1) AS pct_time_gt99,
       ROUND(current_util_pct, 1) AS current_util,
       ROUND(current_apy_at_target_pct, 2) AS apy_at_target
FROM v_liquidity_risk
ORDER BY pct_time_gt99 DESC;
```

Hourly price log-returns per token (feed volatility / correlation):

```sql
SELECT ts, symbol, price_usd,
       LN(price_usd / LAG(price_usd) OVER (PARTITION BY chain_id, token_address ORDER BY ts)) AS log_return
FROM v_prices
QUALIFY log_return IS NOT NULL
ORDER BY symbol, ts;
```

Latest reallocations per vault (a live "what did the allocator just do" feed):

```sql
SELECT ts, collateral_symbol,
       ROUND(prev_weight_pct, 1) AS w_before, ROUND(weight_pct, 1) AS w_after,
       ROUND(weight_change_pct, 1) AS delta_pp, ROUND(supply_assets_change, 1) AS assets_moved
FROM v_vault_drift
WHERE vault = '0x4dc97f968b0ba4edd32d1b9b8aaf54776c134d42'
ORDER BY ts DESC
LIMIT 20;
```

## Consuming MNEMON from other projects

Other projects (a metric library, backtester, market/position scorers) read
the store; they never import the ingestion. Add MNEMON as a git dependency:

```sh
uv add "mnemon @ git+https://github.com/achillesbro/MNEMON.git"
```

```python
from mnemon.reader import MnemonReader

r = MnemonReader("/home/ubuntu/mnemon/data")   # or set MNEMON_DATA
r.tables()                       # what's queryable
r.market_state_latest()          # newest state row per market -> DataFrame
r.market_state(collateral="kHYPE", since="2026-06-01")   # state timeseries
r.liquidity_risk()               # per-market utilization risk
r.vault_snapshot()               # current allocations, weights, cap usage
r.vault_allocations(vault="0x4dC9...", since="2026-06-01")  # allocation history
r.positions(market_id="0xc552...")   # borrower snapshots
r.prices(symbol="kHYPE", since="2026-06-01")
r.yield_pools()                  # competing venue yields
r.sql("SELECT * FROM v_vault_drift WHERE vault = ?", ["0x..."])  # arbitrary SQL
```

`MnemonReader` reads the Parquet globs through its own in-memory DuckDB, so it
never locks the cron's `mnemon.duckdb` and is safe to run concurrently. If a
consumer runs on a different host than the ingestion, either run a MNEMON
instance there too or `rsync` the small `data/` directory across; point
`MNEMON_DATA` at it.

Not Python? Read the Parquet globs directly with any DuckDB/Arrow client
(Rust `duckdb`, Node, polars): `read_parquet('<data>/<table>/**/*.parquet')`.

For an LLM/agent working against the store, point it at [`llms.txt`](llms.txt)
— a full table-and-view reference with grains, units, and conventions.

> **Cadence caveat:** MNEMON is a 15-min analytics store — right for
> backtesting, scoring, and context, but *not* a real-time feed. Never use it
> as the trigger for on-chain actions; validate those against live chain state.

## Layout

```
mnemon/
  config.yaml            # vaults, chains, cadences — the only thing to edit
  run_mnemon.sh          # cron entrypoint
  llms.txt               # table/view reference for LLMs & agents
  src/mnemon/            # api clients, normalizers, jobs, storage, cli
    reader.py            # MnemonReader: read API for other projects
    views.py             # derived-view SQL, shared by ingestion + reader
  systemd/               # VPS timer units (see docs/DEPLOY.md)
  tests/                 # offline unit tests w/ recorded API fixtures
  docs/SCHEMA_NOTES.md   # Morpho API introspection findings & gotchas
  data/                  # (gitignored) parquet + duckdb + state + logs
    market_state/date=YYYY-MM-DD/part-0.parquet
    ...
    mnemon.duckdb
    mnemon_state.json
```

## Design notes

- **Idempotent by construction**: every row's upsert key includes its
  cadence bucket (`ts` floored to 5 min / 15 min / hour / day). Re-runs merge into
  the day's Parquet file, replacing equal keys — gaps heal, nothing duplicates.
- **Exact integers**: raw token/share amounts are DECIMAL(38,0) in Parquet
  (18-decimal shares overflow int64 and float64 loses precision). The raw
  oracle price can exceed 38 digits and is kept as a string; views cast it.
- **Backfill granularity is hourly** (the API's finest historical interval);
  live sampling is 15-min. The `source` column (`live`/`backfill`/`rpc`)
  distinguishes them.
- **Positions are current-only upstream**: the API doesn't enumerate
  historical borrowers, so `positions` accumulates one daily snapshot forward
  from the day tracking starts.
- **Old TS snapshots** (`out/snapshot-*.json`) carry only derived values
  (APY, utilization) without raw state, so they migrate into their own
  `legacy_snapshots` table rather than being disguised as `market_state`.
