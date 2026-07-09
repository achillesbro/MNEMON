"""vault_allocations job (hourly): current allocation of each configured vault
across its markets, straight from vaultByAddress.state.allocation."""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import VAULT_ALLOCATIONS
from mnemon.storage import floor_ts

log = logging.getLogger(__name__)


def job_vault_allocations(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.vault_allocations)
    rows: list[dict] = []
    for vault in ctx.cfg.vaults:
        payload = ctx.morpho.vault_allocations(vault.address, vault.chain_id)
        if payload is None:
            log.warning("vault %s not found on API", vault.label)
            continue
        rows.extend(normalize.vault_allocation_rows(payload, vault.chain_id, ts))
    n = ctx.store.upsert(VAULT_ALLOCATIONS, rows)
    return f"{n} rows @ {ts:%Y-%m-%d %H:%M}"
