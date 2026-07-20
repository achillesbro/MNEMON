"""vault_v2_state job: hourly snapshots of Vault V2 vault-level state.

Same clock-sampled shape as market_state, but from the vaultV2ByAddress API
entity (V2 vaults are invisible to the V1 vaultByAddress queries). Raw asset /
share integers stay exact; sharePrice/totalAssetsUsd are API floats.
"""

from __future__ import annotations

import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import VAULT_V2_STATE
from mnemon.storage import floor_ts

log = logging.getLogger(__name__)


def job_vault_v2_state(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.vault_v2_state)
    rows: list[dict] = []
    for v in ctx.cfg.v2_vaults:
        payload = ctx.morpho.vault_v2_state(v.address, v.chain_id)
        if payload is None:
            log.warning("vault_v2_state: API returned null for %s (%s)", v.address, v.label)
            continue
        rows.append(normalize.vault_v2_state_row(payload, ts))
    n = ctx.store.upsert(VAULT_V2_STATE, rows)
    return f"{n} rows @ {ts:%Y-%m-%d %H:%M}"
