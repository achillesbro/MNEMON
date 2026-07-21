"""Market discovery: the tracked market set is derived, not hand-maintained.

tracked markets = union of:
  - markets any configured vault currently allocates into,
  - config.extra_markets,
  - every market on each `full_scan_chains` chain with supply >= the floor
    (config.min_market_supply_usd) — this widens the archive from "what HEGEMON
    holds" to "the whole HyperEVM market universe" for the FE market analyser.
When a vault adds a market, tracking starts automatically at the next run. The
last good result is cached in the state file so one failed API call doesn't
blank out tracking.

Returns the sorted market list AND a {(chain_id, market_id): supply_usd} map
from the full scan (used by the positions job to skip small markets).
"""

from __future__ import annotations

import logging

from mnemon.config import Config
from mnemon.morpho_api import MorphoClient
from mnemon.state import MnemonState

log = logging.getLogger(__name__)


def discover_markets(
    cfg: Config, morpho: MorphoClient, state: MnemonState
) -> tuple[list[tuple[int, str]], dict[tuple[int, str], float]]:
    """Returns (sorted (chain_id, market_id) pairs, supply_usd map)."""
    tracked: set[tuple[int, str]] = {(m.chain_id, m.market_id) for m in cfg.extra_markets}
    supply_usd: dict[tuple[int, str], float] = {}
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

    # Full-chain scan: add every market above the supply floor.
    for chain_id in cfg.full_scan_chains:
        try:
            markets = morpho.all_markets(chain_id)
        except Exception:
            log.exception("full-chain scan failed for chain %d", chain_id)
            failed = True
            continue
        added = 0
        for m in markets:
            usd = (m.get("state") or {}).get("supplyAssetsUsd") or 0.0
            key = (chain_id, m["marketId"])
            supply_usd[key] = usd
            if usd >= cfg.min_market_supply_usd:
                if key not in tracked:
                    added += 1
                tracked.add(key)
        log.info(
            "chain %d full scan: %d markets, %d above $%.0f floor (%d new)",
            chain_id, len(markets), sum(1 for v in supply_usd.values() if v >= cfg.min_market_supply_usd),
            cfg.min_market_supply_usd, added,
        )

    if failed and not tracked:
        cached = [(int(c), m) for c, m in state.cached_tracked()]
        log.warning("discovery failed entirely; using %d cached markets", len(cached))
        return sorted(cached), supply_usd

    if failed:
        # Partial failure: union with the cache so a flaky call can't shrink
        # the tracked set (shrinking would leave silent gaps in the data).
        tracked |= {(int(c), m) for c, m in state.cached_tracked()}

    result = sorted(tracked)
    if result:
        state.cache_tracked([[c, m] for c, m in result])
    return result, supply_usd
