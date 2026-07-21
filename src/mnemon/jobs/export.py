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

SCHEMA_VERSION = 2

# Latest row per market. Beyond the core health/APY/sizing, this enriches each
# market with data already computed by other views (all share v_market_health's
# deps except positions, joined separately in run_export):
#   v_apy_spread        -> spread_to_best (bps below the best non-broken market)
#   v_market_snapshot   -> oracle_price (1 collateral priced in loan units)
#   v_price_returns     -> collateral annualized volatility (7d/30d)
#   v_utilization_regime-> % of time pinned >95%/>99% + avg util (7d/30d)
# Utilization-regime percentages are divided by 100 so every util/ratio field
# in the export is a fraction — the FE formats them all the same way.
# The 48h freshness cut is relative to the newest sample in the store (not wall
# clock) so it behaves identically in tests and against a paused archive.
_LATEST_SQL = """
WITH h AS (
    SELECT *,
           MAX(ts) OVER (PARTITION BY chain_id, market_id) AS mkt_max_ts,
           MAX(ts) OVER ()                                 AS global_max_ts
    FROM v_market_health
),
vol AS (  -- latest annualized volatility per token
    SELECT chain_id, token_address, vol_7d_ann, vol_30d_ann
    FROM v_price_returns
    QUALIFY ts = MAX(ts) OVER (PARTITION BY chain_id, token_address)
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
    h.broken_reason,
    sp.spread_to_best,
    snap.oracle_price,
    vol.vol_7d_ann  AS collateral_vol_7d,
    vol.vol_30d_ann AS collateral_vol_30d,
    ur.avg_util_7d       / 100 AS avg_util_7d,
    ur.avg_util_30d      / 100 AS avg_util_30d,
    ur.pct_time_gt95_7d  / 100 AS pct_time_gt95_7d,
    ur.pct_time_gt95_30d / 100 AS pct_time_gt95_30d,
    ur.pct_time_gt99_7d  / 100 AS pct_time_gt99_7d,
    ur.pct_time_gt99_30d / 100 AS pct_time_gt99_30d
FROM h
LEFT JOIN v_market_apy a
       ON a.chain_id = h.chain_id AND a.market_id = h.market_id AND a.ts = h.ts
LEFT JOIN markets m
       ON m.chain_id = h.chain_id AND m.market_id = h.market_id
LEFT JOIN v_apy_spread sp
       ON sp.chain_id = h.chain_id AND sp.market_id = h.market_id AND sp.ts = h.ts
LEFT JOIN v_market_snapshot snap
       ON snap.chain_id = h.chain_id AND snap.market_id = h.market_id
LEFT JOIN v_utilization_regime ur
       ON ur.chain_id = h.chain_id AND ur.market_id = h.market_id
LEFT JOIN vol
       ON vol.chain_id = h.chain_id AND vol.token_address = LOWER(m.collateral_token)
WHERE h.ts = h.mkt_max_ts
  AND h.mkt_max_ts >= h.global_max_ts - INTERVAL 48 HOUR
ORDER BY h.is_broken ASC, h.supply_usd DESC NULLS LAST
"""

# Borrower-book risk from the latest positions snapshot (v_position_risk needs
# the `positions` table, which may be absent on a fresh store — merged only when
# available). Percentages -> fractions; min_hf stays a ratio.
_POSITION_RISK_SQL = """
SELECT
    chain_id, market_id,
    borrowers,
    min_hf,
    borrowers_hf_lt_105,
    pct_debt_hf_lt_105 / 100 AS pct_debt_hf_lt_105,
    top3_debt_pct      / 100 AS top3_debt_pct
FROM v_position_risk
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
        latest = reader.sql(_LATEST_SQL)
        # Borrower risk needs the `positions` table; merge it in when present,
        # otherwise those fields are simply absent (FE treats them as optional).
        if "v_position_risk" in tables and not latest.empty:
            latest = latest.merge(
                reader.sql(_POSITION_RISK_SQL), on=["chain_id", "market_id"], how="left"
            )
        payload = build_market_health(latest, reader.sql(_HISTORY_SQL), generated_at)
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

    # dict records (not itertuples) so optional columns — e.g. borrower risk when
    # the positions table is absent — are simply missing rather than an error.
    markets = [
        {
            "market_id": rec["market_id"],
            "loan_symbol": _str(rec.get("loan_symbol")),
            "collateral_symbol": _str(rec.get("collateral_symbol")),
            "lltv": _num(rec.get("lltv")),
            "ts": _iso(rec.get("ts")),
            "utilization": _num(rec.get("utilization")),
            "supply_apy": _num(rec.get("supply_apy")),
            "borrow_apy": _num(rec.get("borrow_apy")),
            "apy_at_target": _num(rec.get("apy_at_target")),
            "supply_usd": _num(rec.get("supply_usd")),
            "available_usd": _num(rec.get("available_usd")),
            "is_broken": bool(rec.get("is_broken")),
            "broken_reason": _str(rec.get("broken_reason")),
            "spread_to_best": _num(rec.get("spread_to_best")),
            "oracle_price": _num(rec.get("oracle_price")),
            "collateral_vol_7d": _num(rec.get("collateral_vol_7d")),
            "collateral_vol_30d": _num(rec.get("collateral_vol_30d")),
            "utilization_regime": {
                "avg_util_7d": _num(rec.get("avg_util_7d")),
                "avg_util_30d": _num(rec.get("avg_util_30d")),
                "pct_time_gt95_7d": _num(rec.get("pct_time_gt95_7d")),
                "pct_time_gt95_30d": _num(rec.get("pct_time_gt95_30d")),
                "pct_time_gt99_7d": _num(rec.get("pct_time_gt99_7d")),
                "pct_time_gt99_30d": _num(rec.get("pct_time_gt99_30d")),
            },
            "borrower_risk": _borrower_risk(rec),
            "history": hist_by_market.get(rec["market_id"], []),
        }
        for rec in latest.to_dict("records")
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso(generated_at),
        "chain_id": _sole_chain(latest),
        "markets": markets,
    }


def _borrower_risk(rec: dict) -> dict | None:
    """Borrower-book summary, or None when this market has no positions data
    (v_position_risk absent, or the market has no borrowers)."""
    borrowers = rec.get("borrowers")
    if borrowers is None or pd.isna(borrowers):
        return None
    return {
        "borrowers": int(borrowers),
        "min_hf": _num(rec.get("min_hf")),
        "borrowers_hf_lt_105": _int(rec.get("borrowers_hf_lt_105")),
        "pct_debt_hf_lt_105": _num(rec.get("pct_debt_hf_lt_105")),
        "top3_debt_pct": _num(rec.get("top3_debt_pct")),
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


def _int(v) -> int | None:
    if v is None or pd.isna(v):
        return None
    return int(v)


def _str(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    return str(v)
