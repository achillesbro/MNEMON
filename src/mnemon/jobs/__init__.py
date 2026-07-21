"""Job registry and runner. Each job is a function(ctx) -> summary string.

`run_due_jobs` isolates failures: one job blowing up must not abort the
others, so each runs in its own try/except and only successful jobs advance
their last-success timestamp.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from mnemon.jobs.context import Context
from mnemon.jobs.bot_events import job_bot_events
from mnemon.jobs.export import job_export
from mnemon.jobs.heal import job_heal
from mnemon.jobs.market_state import job_market_state
from mnemon.jobs.markets_dim import job_markets_dim
from mnemon.jobs.positions import job_positions
from mnemon.jobs.prices import job_prices
from mnemon.jobs.vault_allocations import job_vault_allocations
from mnemon.jobs.vault_v2_flows import job_vault_v2_flows
from mnemon.jobs.vault_v2_state import job_vault_v2_state
from mnemon.jobs.yield_pools import job_yield_pools

log = logging.getLogger(__name__)

# Order matters: markets_dim runs before dependents so a brand-new market has
# its dimension row (decimals, symbols) before state rows reference it.
# heal runs before export so gaps are patched first; export runs LAST — it only
# reads the views over what every other job has already written this tick.
JOBS: dict[str, Callable[[Context], str]] = {
    "markets": job_markets_dim,
    "market_state": job_market_state,
    "vault_allocations": job_vault_allocations,
    "prices": job_prices,
    "positions": job_positions,
    "yield_pools": job_yield_pools,
    "bot_events": job_bot_events,
    "vault_v2_state": job_vault_v2_state,
    "vault_v2_flows": job_vault_v2_flows,
    "heal": job_heal,
    "export": job_export,
}


def run_due_jobs(ctx: Context, only: list[str] | None = None) -> dict[str, str]:
    results: dict[str, str] = {}
    for name, job in JOBS.items():
        if only is not None and name not in only:
            continue
        cadence = getattr(ctx.cfg.cadences, name)
        if only is None and not ctx.state.is_due(name, cadence, ctx.now):
            continue
        started = time.time()
        try:
            summary = job(ctx)
        except Exception:
            log.exception("job %s failed", name)
            results[name] = "FAILED (see log)"
            continue
        ctx.state.mark_success(name, ctx.now)
        ctx.state.save()  # persist after each job so a later crash loses nothing
        results[name] = f"{summary} ({time.time() - started:.1f}s)"
        log.info("job %s: %s", name, results[name])
    return results
