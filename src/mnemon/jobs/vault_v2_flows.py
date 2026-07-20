"""vault_v2_flows job: Deposit/Withdraw events for Vault V2 vaults.

First event-keyed table — rows are identified by (tx_hash, log_index), not a
clock bucket. The vaultV2transactions API has full history, so backfill is
inherent: the first run fetches from t=0; subsequent runs fetch from a
per-vault last-seen-timestamp cursor minus a 1h overlap (the upsert key makes
the overlap idempotent). This fetch-from-cursor + event-key pattern is the
template for future event tables (market_events, reallocations).
"""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import VAULT_V2_FLOWS

log = logging.getLogger(__name__)

OVERLAP_S = 3600  # re-fetch window to absorb API lag; deduped by the event key


def job_vault_v2_flows(ctx: Context) -> str:
    rows: list[dict] = []
    for v in ctx.cfg.v2_vaults:
        cursor_key = f"v2_flows:{v.chain_id}:{v.address.lower()}"
        since = max(0, int(ctx.state.get_cursor(cursor_key, 0)) - OVERLAP_S)
        items = ctx.morpho.vault_v2_transactions(v.address, v.chain_id, since_ts=since)
        if not items:
            continue
        rows.extend(normalize.vault_v2_flow_rows(items))
        ctx.state.set_cursor(cursor_key, max(int(it["timestamp"]) for it in items))
    n = ctx.store.upsert(VAULT_V2_FLOWS, rows)
    return f"{n} rows ({len(ctx.cfg.v2_vaults)} vaults)"
