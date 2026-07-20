# MNEMON — LLM working notes

Morpho-on-HyperEVM data archive. Python 3.12, `uv`-managed, package at
`src/mnemon/`. **Storage is Parquet files + a DuckDB view layer** — there is no
live SQL database, no migrations, no CREATE TABLE. `uv run pytest` (offline,
fixture-based). Entry: `uv run python -m mnemon <run|backfill|check|discover|init-db>`;
`run --only job1,job2` forces jobs regardless of cadence.

## What this is

An analytics **archive**, not a live feed: scheduled jobs sample the Morpho
GraphQL API (+ DefiLlama, + the HEGEMON V2 bot's event sink) into Parquet;
DuckDB views derive metrics at query time. Anything needing sub-minute state
belongs in the consumer's own loop. Downstream statistical work (percentiles,
half-lives, parameter recommendations) belongs to the future `myrmidons`
Python library — **views are per-row algebra only; if a view starts needing
statistics, stop and leave a TODO.**

## Core machinery (reuse, don't reinvent)

- **Tables** = `TableSpec` in `schemas.py` (pyarrow schema + upsert keys +
  partitioned flag), registered in `ALL_TABLES`. Raw integers are
  `BIGINT = decimal128(38,0)` (18-dec shares overflow int64); oracle price is
  a string. A table exists once it has parquet (`Store.has_data`).
- **Upserts** = `Store.upsert(spec, rows)` (`storage.py`): rows merged into
  day-partitioned parquet, `drop_duplicates(subset=spec.keys, keep="last")`,
  atomic tmp+replace. Clock-sampled tables put a `floor_ts`-bucketed `ts` in
  the key; event tables key on identity (first one: `vault_v2_flows` on
  `(tx_hash, log_index)`).
- **Jobs** = `def job_<name>(ctx: Context) -> str` in `jobs/<name>.py`,
  registered in the ordered `JOBS` dict (`jobs/__init__.py`; `markets` first),
  cadence field added to `Cadences` (`config.py`) + `config.yaml`. Failures
  are isolated per job; success stamps `mnemon_state.json`. Scheduler tick is
  5 min (cron/systemd) — no cadence can be finer.
- **State** (`state.py`, `data/mnemon_state.json`): `is_due/mark_success`,
  `is_backfilled/mark_backfilled`, and generic `get_cursor/set_cursor`
  (namespaced keys: `botlog:<file>` for byte offsets, `v2_flows:<chain>:<vault>`
  for last-seen timestamps).
- **Views** = `DerivedView` entries in `views.py` (name, deps frozenset of raw
  tables, SELECT body), created in list order by `create_derived_views` —
  shared by `duck.refresh_views` (persisted db) and `reader.py` (in-memory),
  so they can never drift. A view may reference an earlier view in the list;
  its `deps` still name raw tables only.
- **API** (`morpho_api.py`): `Q_*` GraphQL constants + typed fetchers on
  `MorphoClient`; retries/throttle live in the shared `HttpClient` (`http.py`).
  Pagination is `first`/`skip` loops.
- **Tests**: pure normalizers in `normalize.py` tested against recorded real
  payloads in `tests/fixtures/`; storage/jobs tested with `tmp_path` + `Store`.
  `as_int` handles the API's BigInt number-or-string duality.
- **check** command: per-table row counts/null rates automatically; gap
  detection via `GAP_CHECKS` in `check.py` (spec, entity cols, bucket seconds).
  The daily `heal` job re-pulls recent history to self-repair outage gaps.

## HEGEMON V2 pipeline (added 2026-07-20)

The HEGEMON V2 reallocation bot (repo `HEGEMON_V2`, same VPS) appends every
structured JSONL event to `/home/ubuntu/HEGEMON_V2/data/events/events-YYYY-MM-DD.jsonl`
(bind-mounted, survives container recreation — Docker logs do NOT; there is no
sink history before 2026-07-20 ~10:52 UTC). Contract: bot repo's
`HEGEMON_V2_STRATEGY_SPEC.md` §0 "Event stream & data pipeline".

- `bot_events` job (15 min): tails the sink with a per-file byte-offset+line
  cursor. Complete lines only — a truncated trailing line is retried next run;
  malformed/unknown-type lines log-and-skip (and are advanced past). `scores`
  events → `bot_scores` (tick × market, full-precision u/apy/exitRatio/score/
  gate, vault totals denormalized, ts floored to 60s); every other event →
  `bot_events` keyed `(tick_id, seq=line index in file)`, payload as a JSON
  column. Join a plan to its confirmation and its scores snapshot via `tick_id`.
- `vault_v2_state` / `vault_v2_flows` jobs (hourly): the `vaultV2ByAddress` /
  `vaultV2transactions` API entities (V2 vaults are invisible to V1
  `vaultByAddress` queries). Flows backfill inherently (API has full history;
  first run from t=0, then ts-cursor with 1h idempotent overlap). Vaults come
  from `v2_vaults` in `config.yaml`.
- Views: `v_market_apy` (the bot's exact math — AdaptiveCurveIRM
  `utilizationToRate`, steepness 4, target 0.9, 3-term Taylor compounding;
  **fee assumed 0** because `market_state` has no fee column — documented in
  `docs/SCHEMA_NOTES.md`, verified 0.00 bps vs `bot_scores.apy` live),
  `v_apy_spread`, `v_util_spells` (gaps-and-islands at u ≥ 0.92/0.95, 2h hole
  tolerance), `v_hegemon_benchmark` (equal-weight + best-market passive
  counterfactuals).

## API gotchas (see docs/SCHEMA_NOTES.md for the full list)

- `vaultV2transactions`: `orderBy` enum is `Time` (not `Timestamp`);
  `timestamp_gte` variable type is `Int` (not `BigInt`); Deposit `data` has no
  `receiver` (`onBehalf` receives the shares); type_in `[Deposit, Withdraw,
  Transfer]`.
- `Market.uniqueKey` was renamed `marketId`; filters still accept
  `uniqueKey_in`. BigInt serializes as number-or-string (`as_int`).
- DuckDB + tz-aware timestamps → pandas needs `pytz`; in tests, return
  `CAST(ts AS VARCHAR)` or avoid ts in fetchall. `DECIMAL` comparisons: use
  `float(...)` in Python-side asserts.

## Deployment

VPS `ubuntu@51.210.107.138`, dir `~/mnemon`, clone of
`github.com/achillesbro/MNEMON` (SSH remote for pushing:
`git@github.com:achillesbro/MNEMON.git`). Deploy = push to main, then on the
VPS `git pull` (+ `uv sync` if deps changed) — the 5-min scheduler picks up new
jobs on the next tick. `uv` lives in `~/.local/bin` (not on the default ssh
PATH). The HEGEMON V2 bot + its event sink and the V1 keeper (`/srv/HEGEMON`)
share this VPS; MNEMON reads the sink directly from the filesystem.

**Warning**: a directory named `MNEMON-main` on the operator's machine is a
stale pre-refactor snapshot (no `.git`, no `views.py`/`heal`/`reader`). Never
work there — clone this repo.
