"""markets dimension job (daily): one current row per tracked market.

Also the place where new markets get their historical backfill: a market we
have never backfilled gets its full hourly history pulled once, so history
never starts at zero when a vault adds a market.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ingest import normalize
from ingest.jobs.backfill import backfill_new_entities
from ingest.jobs.context import Context
from ingest.schemas import MARKETS

log = logging.getLogger(__name__)


def job_markets_dim(ctx: Context) -> str:
    fetched_at = datetime.fromtimestamp(int(ctx.now), tz=timezone.utc)
    rows: list[dict] = []
    for chain_id in ctx.chain_ids:
        ids = ctx.market_ids(chain_id)
        if not ids:
            continue
        items = ctx.morpho.markets_meta(ids, [chain_id])
        rows.extend(normalize.markets_dim_rows(items, fetched_at))
    n = ctx.store.upsert(MARKETS, rows)

    backfilled = backfill_new_entities(ctx, rows)
    suffix = f", backfilled {backfilled} new entities" if backfilled else ""
    return f"{n} markets{suffix}"
