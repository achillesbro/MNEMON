"""heal job: repair gaps left by upstream outages.

When the Morpho API is down (e.g. the 502/504 window of 2026-07-16), live
15-min runs fail and those buckets are lost forever at 15-min granularity —
but the API's *hourly history* still covers the window after the fact. This
job re-pulls the last `heal_lookback_hours` of hourly history for every
tracked market, vault, and token, and inserts ONLY the buckets that are
missing (`Store.insert_missing`), so:

- outages shorter than the lookback leave zero gaps at hourly granularity;
- existing live rows are never overwritten (they carry oracle_price_raw,
  which history rows lack — a plain upsert would null it out).

Runs daily by default (cadence `heal`); `python -m mnemon heal --hours N`
forces a wider window after a longer outage. Idempotent: a gap-free window
adds zero rows. positions/yield_pools are point-in-time snapshots with no
usable upstream history, so they cannot be healed and aren't touched.
"""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.jobs.prices import tracked_tokens
from mnemon.schemas import MARKET_STATE, PRICES, VAULT_ALLOCATIONS

log = logging.getLogger(__name__)


def job_heal(ctx: Context, lookback_hours: int | None = None) -> str:
    hours = lookback_hours or ctx.cfg.heal_lookback_hours
    end = int(ctx.now)
    start = end - hours * 3600
    healed = {"market_state": 0, "vault_allocations": 0, "prices": 0}

    for chain_id in ctx.chain_ids:
        for market_id in ctx.market_ids(chain_id):
            payload = ctx.morpho.market_history(market_id, chain_id, start, end)
            if payload is None:
                continue
            rows = normalize.market_state_rows_history(market_id, chain_id, payload["historicalState"])
            healed["market_state"] += ctx.store.insert_missing(MARKET_STATE, rows)

    for vault in ctx.cfg.vaults:
        payload = ctx.morpho.vault_allocation_history(vault.address, vault.chain_id, start, end)
        if payload is None:
            continue
        rows = normalize.vault_allocation_history_rows(payload, vault.chain_id)
        healed["vault_allocations"] += ctx.store.insert_missing(VAULT_ALLOCATIONS, rows)

    for chain_id, tokens in tracked_tokens(ctx).items():
        for token in sorted(tokens):
            payload = ctx.morpho.asset_price_history(token, chain_id, start, end)
            series = (payload or {}).get("historicalPriceUsd") or []
            rows = normalize.price_rows_morpho_history(chain_id, token, series)
            healed["prices"] += ctx.store.insert_missing(PRICES, rows)

    filled = {k: v for k, v in healed.items() if v}
    if filled:
        log.info("heal filled gaps: %s (window %dh)", filled, hours)
    return (
        f"+{healed['market_state']} market_state, +{healed['vault_allocations']} vault_alloc, "
        f"+{healed['prices']} price rows (window {hours}h)"
    )
