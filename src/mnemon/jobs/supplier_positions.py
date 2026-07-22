"""supplier_positions job (hourly): snapshot of current supplier (lender)
positions per tracked market — the concentration side of the book: who could
unilaterally move a market's utilization/yield by withdrawing. Like the
borrower `positions` job, the API only serves *current* positions, so history
accumulates forward. No supply floor: the lender book is tiny (hundreds of
rows chain-wide) so every tracked market is snapshotted."""

from __future__ import annotations

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import SUPPLIER_POSITIONS
from mnemon.storage import floor_ts


def job_supplier_positions(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.supplier_positions)
    rows: list[dict] = []
    for chain_id in ctx.chain_ids:
        ids = ctx.market_ids(chain_id)
        if not ids:
            continue
        items = ctx.morpho.supplier_positions(ids, [chain_id], ctx.cfg.positions_max_pages)
        rows.extend(normalize.supplier_position_rows(items, ts))
    n = ctx.store.upsert(SUPPLIER_POSITIONS, rows)
    return f"{n} supplier positions @ {ts:%Y-%m-%d %H:%M}"
