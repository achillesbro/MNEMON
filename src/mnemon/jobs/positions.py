"""positions job (daily): snapshot of current borrower positions per tracked
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
    rows: list[dict] = []
    for chain_id in ctx.chain_ids:
        ids = ctx.market_ids(chain_id)
        if not ids:
            continue
        items = ctx.morpho.positions(ids, [chain_id], ctx.cfg.positions_max_pages)
        rows.extend(normalize.position_rows(items, ts))
    n = ctx.store.upsert(POSITIONS, rows)
    return f"{n} positions @ {ts:%Y-%m-%d}"
