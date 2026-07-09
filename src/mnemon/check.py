"""Data-quality report: `python -m mnemon check`.

Reports, per table: row counts, null rates per column, and — for the
timeseries tables — missing time buckets per market between its first and
last observation. market_state is checked at hourly granularity because the
backfilled portion is hourly; the live portion is denser."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from mnemon.config import Config
from mnemon.schemas import ALL_TABLES, MARKET_STATE, PRICES, VAULT_ALLOCATIONS
from mnemon.state import MnemonState
from mnemon.storage import Store

# (table, entity columns, expected bucket seconds for gap detection)
GAP_CHECKS = [
    (MARKET_STATE, ["chain_id", "market_id"], 3600),
    (VAULT_ALLOCATIONS, ["chain_id", "vault", "market_id"], 3600),
    (PRICES, ["chain_id", "token_address"], 3600),
]


def run_check(cfg: Config) -> str:
    store = Store(cfg.data_dir)
    con = duckdb.connect()
    lines: list[str] = []

    lines.append("== last job success ==")
    state = MnemonState(cfg.state_path)
    for job in ["markets", "market_state", "vault_allocations", "prices", "positions", "yield_pools"]:
        ts = state.last_success(job)
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if ts else "never"
        lines.append(f"  {job:20} {when}")

    for spec in ALL_TABLES.values():
        if not store.has_data(spec):
            continue
        hive = ", hive_partitioning = 1" if spec.partitioned else ""
        rel = f"read_parquet('{store.parquet_glob(spec)}'{hive})"
        lines.append(f"\n== {spec.name} ==")
        n = con.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()[0]
        lines.append(f"  rows: {n:,}")

        null_rates = con.execute(
            f"SELECT {', '.join(f'AVG(CASE WHEN {f.name} IS NULL THEN 1.0 ELSE 0 END) AS {f.name}' for f in spec.schema)} FROM {rel}"
        ).fetchdf()
        notable = {c: f"{v:.1%}" for c, v in null_rates.iloc[0].items() if v > 0}
        lines.append(f"  null rates: {json.dumps(notable) if notable else 'none'}")

    for spec, entity_cols, bucket_s in GAP_CHECKS:
        if not store.has_data(spec):
            continue
        rel = f"read_parquet('{store.parquet_glob(spec)}', hive_partitioning = 1)"
        ent = ", ".join(entity_cols)
        # Expected = one bucket per `bucket_s` between an entity's first and
        # last ts; actual = distinct buckets present. Difference = gaps.
        # Timestamps are formatted in SQL: duckdb needs pytz (not a dependency)
        # to materialize tz-aware timestamps into Python objects.
        gaps = con.execute(f"""
            SELECT {ent},
                   STRFTIME(MIN(ts), '%Y-%m-%d') AS first_ts,
                   STRFTIME(MAX(ts), '%Y-%m-%d') AS last_ts,
                   CAST(1 + DATE_DIFF('second', MIN(ts), MAX(ts)) / {bucket_s} AS BIGINT) AS expected,
                   COUNT(DISTINCT TIME_BUCKET(INTERVAL '{bucket_s} seconds', ts)) AS actual
            FROM {rel}
            GROUP BY {ent}
            HAVING expected > actual
            ORDER BY expected - actual DESC
        """).fetchall()
        lines.append(f"\n== {spec.name}: missing {bucket_s // 3600}h buckets ==")
        if not gaps:
            lines.append("  none - every entity is gap-free")
        for row in gaps[:15]:
            entity = " ".join(str(v)[:12] for v in row[: len(entity_cols)])
            first, last, expected, actual = row[len(entity_cols) :]
            lines.append(f"  {entity}: {expected - actual} missing of {expected} ({first} -> {last})")
        if len(gaps) > 15:
            lines.append(f"  ... and {len(gaps) - 15} more entities with gaps")

    con.close()
    return "\n".join(lines)
