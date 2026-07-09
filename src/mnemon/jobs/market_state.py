"""market_state job: 15-min snapshots of raw market state.

Live cadence is 15 min (finer than the API's hourly history), so going
forward the table is denser than the backfilled portion — both share the
same schema and the `source` column tells them apart.
"""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.rpc import fetch_market_state_rpc
from mnemon.schemas import MARKET_STATE
from mnemon.storage import floor_ts

log = logging.getLogger(__name__)


def job_market_state(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.market_state)
    rows: list[dict] = []
    for chain_id in ctx.chain_ids:
        ids = ctx.market_ids(chain_id)
        if not ids:
            continue
        items = ctx.morpho.markets_live_state(ids, [chain_id])
        rows.extend(normalize.market_state_rows_live(items, ts))

        # RPC fallback for markets the API knows nothing about (or returned
        # with a null state) — keeps the raw totals series unbroken.
        covered = {it["marketId"] for it in items if it.get("state") is not None}
        missing = [m for m in ids if m not in covered]
        if missing:
            log.warning("chain %d: %d markets missing from API, trying rpc: %s", chain_id, len(missing), missing)
            rows.extend(fetch_market_state_rpc(ctx.cfg.chain(chain_id), missing, ts))

    n = ctx.store.upsert(MARKET_STATE, rows)
    return f"{n} rows @ {ts:%Y-%m-%d %H:%M}"
