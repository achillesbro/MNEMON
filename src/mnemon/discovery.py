"""Market discovery: the tracked market set is derived, not hand-maintained.

tracked markets = union of markets any configured vault currently allocates
into, plus config.extra_markets. When a vault adds a market, tracking starts
automatically at the next run. The last good result is cached in the state
file so one failed API call doesn't blank out tracking.
"""

from __future__ import annotations

import logging

from mnemon.config import Config
from mnemon.morpho_api import MorphoClient
from mnemon.state import MnemonState

log = logging.getLogger(__name__)


def discover_markets(cfg: Config, morpho: MorphoClient, state: MnemonState) -> list[tuple[int, str]]:
    """Returns sorted (chain_id, market_id) pairs."""
    tracked: set[tuple[int, str]] = {(m.chain_id, m.market_id) for m in cfg.extra_markets}
    failed = False
    for vault in cfg.vaults:
        try:
            payload = morpho.vault_allocations(vault.address, vault.chain_id)
        except Exception:
            log.exception("discovery failed for vault %s", vault.label)
            failed = True
            continue
        if payload is None:
            log.warning("vault %s not found on API", vault.label)
            continue
        for alloc in (payload.get("state") or {}).get("allocation") or []:
            tracked.add((vault.chain_id, alloc["market"]["marketId"]))

    if failed and not tracked:
        cached = [(int(c), m) for c, m in state.cached_tracked()]
        log.warning("discovery failed entirely; using %d cached markets", len(cached))
        return sorted(cached)

    if failed:
        # Partial failure: union with the cache so a flaky call can't shrink
        # the tracked set (shrinking would leave silent gaps in the data).
        tracked |= {(int(c), m) for c, m in state.cached_tracked()}

    result = sorted(tracked)
    if result:
        state.cache_tracked([[c, m] for c, m in result])
    return result
