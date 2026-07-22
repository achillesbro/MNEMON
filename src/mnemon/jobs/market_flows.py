"""market_flows job: every Morpho Blue market event (Supply/Withdraw/Borrow/
Repay/SupplyCollateral/WithdrawCollateral/Liquidation) for the whole chain,
from the `marketTransactions` API entity.

Same fetch-from-cursor + event-key pattern as vault_v2_flows, per chain rather
than per vault, with one deliberate difference: the first run does NOT backfill
from t=0 (the chain has millions of historical events) — it starts at
`now - market_flows_backfill_hours`. Whole-chain (no market filter) so new
markets are captured from their first event without waiting for discovery.
An API outage self-heals: the cursor only advances to the last event actually
received, so the next run resumes where this one stopped."""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import MARKET_FLOWS

log = logging.getLogger(__name__)

OVERLAP_S = 3600  # re-fetch window to absorb API lag; deduped by the event key


def job_market_flows(ctx: Context) -> str:
    rows: list[dict] = []
    for chain in ctx.cfg.chains:
        cursor_key = f"market_flows:{chain.chain_id}"
        cursor = ctx.state.get_cursor(cursor_key)
        if cursor is None:
            since = int(ctx.now) - ctx.cfg.market_flows_backfill_hours * 3600
        else:
            since = max(0, int(cursor) - OVERLAP_S)
        items = ctx.morpho.market_transactions(chain.chain_id, since_ts=since)
        if not items:
            continue
        rows.extend(normalize.market_flow_rows(items))
        ctx.state.set_cursor(cursor_key, max(int(it["timestamp"]) for it in items))
    n = ctx.store.upsert(MARKET_FLOWS, rows)
    return f"{n} events ({len(ctx.cfg.chains)} chains)"
