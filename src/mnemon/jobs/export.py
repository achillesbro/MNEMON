"""export job (15 min): write small FE-facing JSON snapshots of the derived
views to `cfg.export_dir`, for the myrmidons-strategies.com "tools" section to
serve statically (via Caddy) and proxy.

This is the archive's read side: it opens a private in-memory DuckDB over the
Parquet globs (`MnemonReader`) — the same view layer every other consumer sees
— and never touches the persisted `mnemon.duckdb`. Writes are atomic
tmp+rename, mirroring `storage.py`, so the web server never serves a partial
file. Row shaping is pure (`build_market_health` / `build_util_spells`) so it
is unit-tested directly.

Files (each carries schema_version + generated_at + chain_id):
  market_health.json  latest row per market (v_market_health + v_market_apy +
                      markets dim), only markets with data in the trailing 48h,
                      each with a 7d hourly supply-APY/util sparkline.
  util_spells.json    v_util_spells for the trailing 30d (both thresholds),
                      `open` flagged when the spell reaches the market's latest
                      sample.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from mnemon.jobs.context import Context
from mnemon.reader import MnemonReader

SCHEMA_VERSION = 1

# Latest row per market: v_market_health carries the classifier + USD sizing,
# v_market_apy the bot-exact supply/borrow APY, markets the display symbols.
# The 48h freshness cut is relative to the newest sample in the store (not wall
# clock) so it behaves identically in tests and against a paused archive.
_LATEST_SQL = """
WITH h AS (
    SELECT *,
           MAX(ts) OVER (PARTITION BY chain_id, market_id) AS mkt_max_ts,
           MAX(ts) OVER ()                                 AS global_max_ts
    FROM v_market_health
)
SELECT
    h.chain_id,
    h.market_id,
    h.ts,
    m.loan_symbol,
    m.collateral_symbol,
    m.lltv::DOUBLE / 1e18 AS lltv,
    h.u                   AS utilization,
    a.supply_apy,
    a.borrow_apy,
    h.apy_at_target,
    h.supply_usd,
    h.available_usd,
    h.is_broken,
    h.broken_reason
FROM h
LEFT JOIN v_market_apy a
       ON a.chain_id = h.chain_id AND a.market_id = h.market_id AND a.ts = h.ts
LEFT JOIN markets m
       ON m.chain_id = h.chain_id AND m.market_id = h.market_id
WHERE h.ts = h.mkt_max_ts
  AND h.mkt_max_ts >= h.global_max_ts - INTERVAL 48 HOUR
ORDER BY h.is_broken ASC, h.supply_usd DESC NULLS LAST
"""

# 7d hourly (minute=0) supply-APY/util sparkline per market, oldest first.
_HISTORY_SQL = """
WITH g AS (SELECT MAX(ts) AS gmax FROM v_market_apy)
SELECT a.chain_id, a.market_id, a.ts, a.supply_apy, a.u
FROM v_market_apy a, g
WHERE EXTRACT(minute FROM a.ts) = 0
  AND a.ts >= g.gmax - INTERVAL 7 DAY
ORDER BY a.chain_id, a.market_id, a.ts
"""

# Utilization spells over the trailing 30d; `open` = the spell's last point is
# within 2h of the market's newest sample (still ongoing, not a closed episode).
_SPELLS_SQL = """
WITH lt AS (
    SELECT chain_id, market_id, MAX(ts) AS mkt_max_ts
    FROM market_state GROUP BY chain_id, market_id
),
g AS (SELECT MAX(ts) AS gmax FROM market_state)
SELECT
    s.chain_id, s.market_id, s.threshold,
    s.start_ts, s.end_ts, s.duration_min, s.peak_u,
    (s.end_ts >= lt.mkt_max_ts - INTERVAL 2 HOUR) AS open
FROM v_util_spells s
JOIN lt ON lt.chain_id = s.chain_id AND lt.market_id = s.market_id
CROSS JOIN g
WHERE s.end_ts >= g.gmax - INTERVAL 30 DAY
ORDER BY s.end_ts DESC
"""


def job_export(ctx: Context) -> str:
    generated_at = datetime.fromtimestamp(ctx.now, tz=timezone.utc)
    with MnemonReader(ctx.cfg.data_dir) as reader:
        return run_export(reader, ctx.cfg.export_dir, generated_at)


def run_export(reader: MnemonReader, export_dir: Path, generated_at: datetime) -> str:
    """Query the views and write the JSON snapshots. Skips a file when its
    source views aren't present yet (partial/fresh store) rather than failing,
    matching the archive's per-job failure isolation."""
    export_dir = Path(export_dir)
    tables = set(reader.tables())
    files = 0
    n_markets = 0

    if {"v_market_health", "v_market_apy", "markets"} <= tables:
        payload = build_market_health(
            reader.sql(_LATEST_SQL), reader.sql(_HISTORY_SQL), generated_at
        )
        _write_json(export_dir / "market_health.json", payload)
        files += 1
        n_markets = len(payload["markets"])

    if {"v_util_spells", "market_state"} <= tables:
        payload = build_util_spells(reader.sql(_SPELLS_SQL), generated_at)
        _write_json(export_dir / "util_spells.json", payload)
        files += 1

    return f"{files} files, {n_markets} markets @ {generated_at:%Y-%m-%d %H:%M}"


def build_market_health(
    latest: pd.DataFrame, history: pd.DataFrame, generated_at: datetime
) -> dict:
    hist_by_market: dict[str, list[dict]] = {}
    for row in history.itertuples(index=False):
        hist_by_market.setdefault(row.market_id, []).append(
            {"ts": _iso(row.ts), "supply_apy": _num(row.supply_apy), "u": _num(row.u)}
        )

    markets = [
        {
            "market_id": row.market_id,
            "loan_symbol": _str(row.loan_symbol),
            "collateral_symbol": _str(row.collateral_symbol),
            "lltv": _num(row.lltv),
            "ts": _iso(row.ts),
            "utilization": _num(row.utilization),
            "supply_apy": _num(row.supply_apy),
            "borrow_apy": _num(row.borrow_apy),
            "apy_at_target": _num(row.apy_at_target),
            "supply_usd": _num(row.supply_usd),
            "available_usd": _num(row.available_usd),
            "is_broken": bool(row.is_broken),
            "broken_reason": _str(row.broken_reason),
            "history": hist_by_market.get(row.market_id, []),
        }
        for row in latest.itertuples(index=False)
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso(generated_at),
        "chain_id": _sole_chain(latest),
        "markets": markets,
    }


def build_util_spells(spells: pd.DataFrame, generated_at: datetime) -> dict:
    out = [
        {
            "market_id": row.market_id,
            "threshold": _num(row.threshold),
            "start_ts": _iso(row.start_ts),
            "end_ts": _iso(row.end_ts),
            "duration_min": int(row.duration_min),
            "peak_u": _num(row.peak_u),
            "open": bool(row.open),
        }
        for row in spells.itertuples(index=False)
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso(generated_at),
        "chain_id": _sole_chain(spells),
        "spells": out,
    }


# --- helpers ---------------------------------------------------------------


def _write_json(path: Path, payload: dict) -> None:
    """Atomic write: serialize to a sibling tmp then rename (POSIX-atomic), so
    a reader never sees a half-written file. allow_nan=False turns any stray
    NaN/Inf (invalid JSON) into a hard error instead of silent corruption."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":"), allow_nan=False))
    tmp.replace(path)


def _sole_chain(df: pd.DataFrame) -> int | None:
    """The single chain_id in the frame (the archive tracks one chain today);
    None if empty or mixed, so the FE never assumes a chain that isn't uniform."""
    if df.empty:
        return None
    chains = {int(c) for c in df["chain_id"].unique()}
    return next(iter(chains)) if len(chains) == 1 else None


def _iso(ts) -> str | None:
    if ts is None or (not isinstance(ts, datetime) and pd.isna(ts)):
        return None
    ts = pd.Timestamp(ts)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(v) -> float | None:
    if v is None or pd.isna(v):
        return None
    return float(v)


def _str(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    return str(v)
