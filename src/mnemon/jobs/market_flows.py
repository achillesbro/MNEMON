"""market_flows job: every Morpho Blue market event (Supply/Withdraw/Borrow/
Repay/SupplyCollateral/WithdrawCollateral/Liquidation) for the whole chain,
from the `marketTransactions` API entity.

Same fetch-from-cursor + event-key pattern as vault_v2_flows, per chain rather
than per vault, with two deliberate differences:

- The first run does NOT backfill from t=0 (the chain has millions of
  historical events) — it starts at `now - market_flows_backfill_hours`.
- Deep history is walked in TIMESTAMP-WINDOWED batches: the API rejects
  skip > 10,000, so each batch pulls at most 100 pages, is committed (upsert +
  cursor + state save), and the next batch re-queries from the new cursor with
  skip reset to 0. At most MAX_BATCHES batches per run keep a catch-up run to
  a few minutes so the rest of the tick's jobs aren't starved; the scheduler's
  next tick continues from the saved cursor. An API outage mid-run self-heals
  the same way: everything already committed stays, the cursor points at it.
"""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import MARKET_FLOWS

log = logging.getLogger(__name__)

OVERLAP_S = 3600  # re-fetch window to absorb API lag; deduped by the event key
MAX_BATCHES = 4  # x ~10k events (100 pages) per run — ~5 min at the throttle


def job_market_flows(ctx: Context) -> str:
    total = 0
    for chain in ctx.cfg.chains:
        cursor_key = f"market_flows:{chain.chain_id}"
        cursor = ctx.state.get_cursor(cursor_key)
        if cursor is None:
            since = int(ctx.now) - ctx.cfg.market_flows_backfill_hours * 3600
        else:
            since = max(0, int(cursor) - OVERLAP_S)
        for _batch in range(MAX_BATCHES):
            items, truncated = ctx.morpho.market_transactions(chain.chain_id, since_ts=since)
            if not items:
                break
            total += ctx.store.upsert(MARKET_FLOWS, normalize.market_flow_rows(items))
            latest = max(int(it["timestamp"]) for it in items)
            ctx.state.set_cursor(cursor_key, latest)
            ctx.state.save()  # each committed batch survives a later crash
            if not truncated:
                break
            if latest <= since:
                # >10k events sharing one second would spin forever; never
                # expected on-chain, but bail loudly rather than loop.
                log.warning("market_flows: cursor stuck at ts %d on chain %d", latest, chain.chain_id)
                break
            since = latest  # contiguous ASC stream; event key dedupes the seam
    return f"{total} events ({len(ctx.cfg.chains)} chains)"
