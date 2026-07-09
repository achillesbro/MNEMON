"""One-off migration of the old TS fetcher's out/snapshot-YYYY-MM-DD.json files
into the legacy_snapshots table, so no history is lost.

The old snapshots stored *derived* values (APYs, utilization, liquidity in
loan-token units) without the underlying raw state, so they can't be turned
into market_state rows honestly — they land in their own table with every
original field preserved."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from ingest.schemas import LEGACY_SNAPSHOTS
from ingest.storage import Store

log = logging.getLogger(__name__)


def snapshot_rows(payload: dict, source_file: str) -> list[dict]:
    ts = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
    chain_id = int(payload.get("chainId", 0))
    rows = []
    for m in payload.get("markets", []):
        rows.append(
            {
                "ts": ts,
                "chain_id": chain_id,
                "market_id": m["marketId"],
                "symbol": m.get("symbol"),
                "loan_token": (m.get("loan") or "").lower() or None,
                "collateral_token": (m.get("collateral") or "").lower() or None,
                # old field is spelled "utilisation"
                "utilization": m.get("utilisation", m.get("utilization")),
                "borrow_apy": m.get("borrowAPY"),
                "supply_apy": m.get("supplyAPY"),
                "available_liquidity": m.get("availableLiquidity"),
                "vault_allocation": m.get("vaultAllocation"),
                "source_file": source_file,
            }
        )
    return rows


def migrate(snapshot_dir: Path, store: Store) -> str:
    files = sorted(snapshot_dir.glob("snapshot-*.json"))
    if not files:
        return f"no snapshot-*.json files found in {snapshot_dir}"
    total = 0
    for f in files:
        try:
            rows = snapshot_rows(json.loads(f.read_text()), f.name)
        except (json.JSONDecodeError, KeyError, ValueError):
            log.exception("skipping unparseable snapshot %s", f.name)
            continue
        total += store.upsert(LEGACY_SNAPSHOTS, rows)
    return f"migrated {len(files)} snapshots, {total} rows -> legacy_snapshots"
