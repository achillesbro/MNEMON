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
  `v_market_health`, `v_apy_spread` (baseline = best NON-broken market,
  `is_broken` flagged), `v_util_spells` (gaps-and-islands at u ≥ 0.92/0.95,
  2h hole tolerance), `v_hegemon_benchmark` (three tiers, see below).

### Broken-market classifier + three-tier benchmark (added 2026-07-21)

`v_market_health` (market × ts: `is_broken`, `broken_reason`, `supply_usd`,
`available_usd` via ASOF join to `prices`) — fixed-rule hysteresis, operator-
tuned thresholds (change them in `views.py`, history reclassifies on refresh):
- **rate_ratchet**: `apy_at_target > 50%` enters broken, `< 25%` exits — the
  AdaptiveCurveIRM ratchets ~2×/5 days pinned at u=1 and decays symmetrically,
  so the IRM itself is the time integrator (don't use instantaneous supply
  APY: it spikes legitimately).
- **pinned_util**: u ≥ 0.999 across the whole trailing 24h enters; exits after
  48h entirely below 0.95 (span guards vs data holes).
- **dust**: supply < $1k USD, unconditional.
- **Thin exemption**: ratchet/pinned only apply while supply < $25k — a deep
  hot market is an opportunity, not a defect. Unpriced ⇒ thin but never dust.
- State machines = enter/exit events + `LAST_VALUE(... IGNORE NULLS)`;
  durations = trailing `RANGE` windows over event time.

`v_hegemon_benchmark` per ts, three tiers: **eligible** (non-broken universe,
echo-chamber antidote), **investable** (eligible + `available_usd ≥ $10k` —
the deployable truth; **$10k mirrors the bot's `minAvailableLiquidity`, keep
them in sync when the bot config changes**), and the **bot's scored set**.
`opportunity_gap_apy` (universe) vs `deployable_gap_apy` (investable) — when
they diverge, yield exists only at un-deployable size. Live calibration
2026-07-21: 6 broken markets (639%-ratchet dust, 84%-pinned zombie, etc.);
universe gap 3482 bps collapsed to 1 bp deployable — HEGEMON's set was
near-optimal.

### FE export job (added 2026-07-21)

`export` job (15 min, runs LAST after `heal`) writes static JSON snapshots to
`cfg.export_dir` (default `<data_dir>/export`, i.e. `~/mnemon/data/export`) for
the website's tools section. Read side only: opens `MnemonReader` (in-memory
DuckDB over the Parquet globs, never the persisted db); atomic tmp+rename
writes; row shaping is pure (`build_market_health`/`build_util_spells`, unit-
tested); skips a file when its source views aren't present rather than failing.
Files: `market_health.json` (latest row per market from `v_market_health` +
`v_market_apy` + `markets`, only markets fresh within 48h, each with a 7d
hourly APY/util sparkline) and `util_spells.json` (`v_util_spells`, trailing
30d, `open` flag). Each has `schema_version`/`generated_at`/`chain_id`. Served
by Caddy at `data.myrmidons-strategies.com` (see plan in
`docs/FE_SURFACING_PLAN.md`); the FE consumes it, not the raw views.

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

On the operator's machine the working copy is `~/MNEMON-main` — since
2026-07-20 it is a proper clone tracking `origin/main` (it was previously a
stale gitless snapshot; that's fixed). Pull before working.
