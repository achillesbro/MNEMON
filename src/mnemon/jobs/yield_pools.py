"""yield_pools job (daily): competing venue yields from yields.llama.fi,
filtered to the chains we track (config: chains[].yields_chain)."""

from __future__ import annotations

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import YIELD_POOLS
from mnemon.storage import floor_ts


def job_yield_pools(ctx: Context) -> str:
    chains = {c.yields_chain for c in ctx.cfg.chains if c.yields_chain}
    if not chains:
        return "no yields_chain configured, skipped"
    ts = floor_ts(ctx.now, ctx.cfg.cadences.yield_pools)
    pools = ctx.llama.yield_pools()
    rows = normalize.yield_pool_rows(pools, chains, ts)
    n = ctx.store.upsert(YIELD_POOLS, rows)
    return f"{n} pools @ {ts:%Y-%m-%d}"
