"""Historical backfill from the Morpho API's timeseries (hourly, the finest
interval it exposes) plus DefiLlama charts for prices.

Runs once per entity, tracked via flags in the state file, so history never
starts at zero: when a vault adds a market, the next daily markets job pulls
that market's full hourly history since creation.

Complexity budgeting (measured, see docs/SCHEMA_NOTES.md): a marketById query
with all 6 series over a year costs ~60k of the 1M budget; vault allocation
history costs ~points x markets x series. Windows are chunked conservatively.

Price backfill strategy: the Morpho API serves each asset's full hourly USD
price history in ONE query, whereas coins.llama.fi charts are capped at 500
points per call (~18 calls per token-year). To stay a good API citizen we
backfill from Morpho first and only walk llama charts for assets Morpho has
no history for. Live hourly prices still come from llama (with confidence).
"""

from __future__ import annotations

import logging
import time

from ingest import normalize
from ingest.defillama import CHART_MAX_SPAN, coin_key
from ingest.jobs.context import Context
from ingest.schemas import MARKET_STATE, PRICES, VAULT_ALLOCATIONS

log = logging.getLogger(__name__)

MARKET_WINDOW_S = 365 * 86400  # 1y of hourly x 6 series ~ 60k complexity
VAULT_WINDOW_S = 180 * 86400  # alloc history scales with market count too


def backfill_new_entities(ctx: Context, dim_rows: list[dict]) -> int:
    """Backfill anything in the current dimension set that has never been
    backfilled. Cheap no-op when flags are already set."""
    done = 0
    now = int(ctx.now)

    for row in dim_rows:
        key = f"market:{row['chain_id']}:{row['market_id']}"
        if ctx.state.is_backfilled(key) or row["creation_ts"] is None:
            continue
        backfill_market(ctx, row["chain_id"], row["market_id"], int(row["creation_ts"].timestamp()), now)
        ctx.state.mark_backfilled(key)
        ctx.state.save()
        done += 1

    for vault in ctx.cfg.vaults:
        key = f"vault_alloc:{vault.chain_id}:{vault.address.lower()}"
        if ctx.state.is_backfilled(key):
            continue
        backfill_vault_allocations(ctx, vault.address, vault.chain_id, now)
        ctx.state.mark_backfilled(key)
        ctx.state.save()
        done += 1

    # Token price history: start each token at the creation of the earliest
    # tracked market that uses it (no point backfilling before that).
    starts: dict[tuple[int, str], int] = {}
    for row in dim_rows:
        if row["creation_ts"] is None:
            continue
        created = int(row["creation_ts"].timestamp())
        for tok in (row["loan_token"], row["collateral_token"]):
            if tok:
                k = (row["chain_id"], tok)
                starts[k] = min(starts.get(k, created), created)
    for (chain_id, token), start_ts in sorted(starts.items()):
        key = f"price:{chain_id}:{token}"
        if ctx.state.is_backfilled(key):
            continue
        backfill_token_prices(ctx, chain_id, token, start_ts, now)
        ctx.state.mark_backfilled(key)
        ctx.state.save()
        done += 1

    return done


def backfill_market(ctx: Context, chain_id: int, market_id: str, start_ts: int, end_ts: int) -> None:
    log.info("backfilling market %s on %d since %s", market_id[:10], chain_id, time.strftime("%Y-%m-%d", time.gmtime(start_ts)))
    total = 0
    for win_start, win_end in _windows(start_ts, end_ts, MARKET_WINDOW_S):
        payload = ctx.morpho.market_history(market_id, chain_id, win_start, win_end)
        if payload is None:
            log.warning("market %s: no history payload", market_id[:10])
            return
        rows = normalize.market_state_rows_history(market_id, chain_id, payload["historicalState"])
        total += ctx.store.upsert(MARKET_STATE, rows)
    log.info("market %s: %d hourly rows backfilled", market_id[:10], total)


def backfill_vault_allocations(ctx: Context, address: str, chain_id: int, end_ts: int) -> None:
    meta = ctx.morpho.vault_allocations(address, chain_id)
    if meta is None:
        log.warning("vault %s not on API, skipping alloc backfill", address)
        return
    start_ts = _earliest_market_creation(ctx, chain_id) or end_ts - VAULT_WINDOW_S
    log.info("backfilling allocations for vault %s since %s", address[:10], time.strftime("%Y-%m-%d", time.gmtime(start_ts)))
    total = 0
    for win_start, win_end in _windows(start_ts, end_ts, VAULT_WINDOW_S):
        payload = ctx.morpho.vault_allocation_history(address, chain_id, win_start, win_end)
        if payload is None:
            return
        rows = normalize.vault_allocation_history_rows(payload, chain_id)
        total += ctx.store.upsert(VAULT_ALLOCATIONS, rows)
    log.info("vault %s: %d hourly allocation rows backfilled", address[:10], total)


def backfill_token_prices(ctx: Context, chain_id: int, token: str, start_ts: int, end_ts: int) -> None:
    # Morpho first: full hourly history in one query.
    payload = ctx.morpho.asset_price_history(token, chain_id, start_ts, end_ts)
    series = (payload or {}).get("historicalPriceUsd") or []
    if series:
        rows = normalize.price_rows_morpho_history(chain_id, token, series)
        n = ctx.store.upsert(PRICES, rows)
        log.info("token %s: %d hourly prices backfilled (morpho)", token[:10], n)
        return

    # Fallback: walk coins.llama.fi charts in 500-point hourly windows.
    key = coin_key(ctx.cfg.chain(chain_id).llama_slug, token)
    total, cursor = 0, start_ts
    while cursor < end_ts:
        points = ctx.llama.price_chart(key, cursor, span=CHART_MAX_SPAN, period="1h")
        total += ctx.store.upsert(PRICES, normalize.price_rows_llama_chart(chain_id, token, points))
        cursor += CHART_MAX_SPAN * 3600  # advance by window even if empty: data may start later
    log.info("token %s: %d hourly prices backfilled (llama chart)", token[:10], total)


def _windows(start: int, end: int, step: int) -> list[tuple[int, int]]:
    return [(s, min(s + step, end)) for s in range(start, end, step)]


def _earliest_market_creation(ctx: Context, chain_id: int) -> int | None:
    ids = ctx.market_ids(chain_id)
    if not ids:
        return None
    items = ctx.morpho.markets_meta(ids, [chain_id])
    creations = [int(it["creationTimestamp"]) for it in items if it.get("creationTimestamp")]
    return min(creations) if creations else None
