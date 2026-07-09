# MNEMON

*(Gk. Μνήμων, "the one who remembers" — the record-keeper behind the
MYRMIDONS stack, feeding HEGEMON and EREBUS.)*

A local, queryable, reproducible historical store of Morpho market data —
the data layer for quantitative research on vault strategies (backtests,
risk analysis). Python ingestion on a 15-min cron, Parquet storage
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

### Cron

```
*/15 * * * * /path/to/mnemon/run_mnemon.sh
```

One entrypoint, invoked every 15 minutes; it internally decides which jobs
are due from the last-success timestamps in `data/mnemon_state.json`:

| job               | cadence | table              | content                                   |
|-------------------|---------|--------------------|-------------------------------------------|
| market_state      | 15 min  | `market_state`     | raw totals, rateAtTarget, oracle price     |
| vault_allocations | hourly  | `vault_allocations`| per-market supply + cap per vault          |
| prices            | hourly  | `prices`           | USD prices (llama, morpho fallback)        |
| markets           | daily   | `markets`          | dimension: tokens, decimals, lltv, oracle; also triggers backfill of newly seen entities |
| positions         | daily   | `positions`        | current borrower positions (accumulates forward) |
| yield_pools       | daily   | `yield_pools`      | competing venue yields on tracked chains   |

Failures in one job never abort the others; logs rotate in `data/logs/`.

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
uv run python -m mnemon check                # data-quality report: gaps, nulls, last runs
uv run python -m mnemon migrate-legacy out/  # import old TS snapshot-*.json files
uv run python -m mnemon init-db              # refresh DuckDB views only
```

## Querying

`data/mnemon.duckdb` holds views over the Parquet files — raw tables 1:1
(`market_state`, `markets`, `vault_allocations`, `positions`, `prices`,
`yield_pools`, `legacy_snapshots`) plus convenience views `v_market_state`
and `v_vault_allocations` with symbols, human units, oracle price and APY
at target already derived.

```python
import duckdb
con = duckdb.connect("data/mnemon.duckdb", read_only=True)
```

Time at utilization > 95% per market (share of hourly observations):

```sql
SELECT market_id, loan_symbol, collateral_symbol,
       AVG(CASE WHEN utilization > 0.95 THEN 1.0 ELSE 0 END) AS share_above_95,
       COUNT(*) AS observations
FROM v_market_state
GROUP BY 1, 2, 3
ORDER BY share_above_95 DESC;
```

Hourly price log-returns per token:

```sql
SELECT ts, token_address, price_usd,
       LN(price_usd / LAG(price_usd) OVER (PARTITION BY chain_id, token_address ORDER BY ts)) AS log_return
FROM prices
QUALIFY log_return IS NOT NULL
ORDER BY token_address, ts;
```

Allocation drift per vault (hourly change in each market's share of the vault):

```sql
WITH alloc AS (
    SELECT ts, vault, market_id, supply_assets,
           supply_assets / SUM(supply_assets) OVER (PARTITION BY vault, ts) AS weight
    FROM v_vault_allocations
    WHERE supply_assets IS NOT NULL
)
SELECT ts, vault, market_id, weight,
       weight - LAG(weight) OVER (PARTITION BY vault, market_id ORDER BY ts) AS weight_change
FROM alloc
QUALIFY ABS(weight_change) > 0.01
ORDER BY ts DESC;
```

## Layout

```
mnemon/
  config.yaml            # vaults, chains, cadences — the only thing to edit
  run_mnemon.sh          # cron entrypoint
  src/mnemon/            # api clients, normalizers, jobs, storage, cli
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
  cadence bucket (`ts` floored to 15 min / hour / day). Re-runs merge into
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
