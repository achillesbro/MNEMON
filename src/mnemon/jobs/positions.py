"""positions job (5 min): snapshot of current borrower positions per tracked
market. The Morpho API only serves *current* positions, so history accumulates
forward from the day tracking starts — that's a known limitation, documented
in SCHEMA_NOTES."""

from __future__ import annotations

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import POSITIONS
from mnemon.storage import floor_ts


def job_positions(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.positions)
    floor = ctx.cfg.positions_min_supply_usd
    supply = ctx.market_supply_usd
    rows: list[dict] = []
    skipped = 0
    for chain_id in ctx.chain_ids:
        ids = ctx.market_ids(chain_id)
        if floor > 0:
            # Skip small markets; markets with unknown supply (vault/extra, not
            # in the full scan) default to +inf so they are always included.
            kept = [m for m in ids if supply.get((chain_id, m), float("inf")) >= floor]
            skipped += len(ids) - len(kept)
            ids = kept
        if not ids:
            continue
        items = ctx.morpho.positions(ids, [chain_id], ctx.cfg.positions_max_pages)
        rows.extend(normalize.position_rows(items, ts))
    n = ctx.store.upsert(POSITIONS, rows)
    suffix = f" ({skipped} below ${floor:.0f} skipped)" if skipped else ""
    return f"{n} positions @ {ts:%Y-%m-%d}{suffix}"
