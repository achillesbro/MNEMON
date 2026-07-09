"""prices job (hourly): USD prices for every token touched by tracked markets
(loan + collateral), from coins.llama.fi with the Morpho API as fallback for
tokens DefiLlama doesn't cover or prices with low confidence."""

from __future__ import annotations

import logging

import pyarrow.parquet as pq

from mnemon import normalize
from mnemon.defillama import coin_key
from mnemon.jobs.context import Context
from mnemon.schemas import MARKETS, PRICES
from mnemon.storage import floor_ts

log = logging.getLogger(__name__)

# Below this DefiLlama confidence we also record the Morpho price so the
# query layer can choose; llama rows are kept regardless (source column).
LOW_CONFIDENCE = 0.8


def tracked_tokens(ctx: Context) -> dict[int, set[str]]:
    """{chain_id: {token_address}} from the markets dimension table, falling
    back to a live meta query if the dim hasn't been written yet."""
    tokens: dict[int, set[str]] = {}
    dim_path = ctx.store.table_dir(MARKETS) / "current.parquet"
    if dim_path.exists():
        df = pq.read_table(dim_path, columns=["chain_id", "loan_token", "collateral_token"]).to_pandas()
        for _, row in df.iterrows():
            for tok in (row["loan_token"], row["collateral_token"]):
                # pandas turns null collateral (idle markets) into NaN, which
                # is truthy — only real address strings may enter the set.
                if isinstance(tok, str) and tok:
                    tokens.setdefault(int(row["chain_id"]), set()).add(tok)
        return tokens
    for chain_id in ctx.chain_ids:
        for it in ctx.morpho.markets_meta(ctx.market_ids(chain_id), [chain_id]):
            for asset in (it.get("loanAsset"), it.get("collateralAsset")):
                if asset and asset.get("address"):
                    tokens.setdefault(chain_id, set()).add(asset["address"].lower())
    return tokens


def job_prices(ctx: Context) -> str:
    ts = floor_ts(ctx.now, ctx.cfg.cadences.prices)
    tokens = tracked_tokens(ctx)
    rows: list[dict] = []

    slug_to_chain = {ctx.cfg.chain(cid).llama_slug: cid for cid in tokens}
    keys = [coin_key(ctx.cfg.chain(cid).llama_slug, addr) for cid, addrs in tokens.items() for addr in addrs]
    coins = ctx.llama.current_prices(keys)
    rows.extend(normalize.price_rows_llama_current(coins, slug_to_chain, ts))

    # Morpho fallback for tokens llama missed or priced with low confidence.
    for chain_id, addrs in tokens.items():
        slug = ctx.cfg.chain(chain_id).llama_slug
        weak = [
            addr
            for addr in addrs
            if ((coins.get(coin_key(slug, addr)) or {}).get("confidence") or 0) < LOW_CONFIDENCE
        ]
        if weak:
            items = ctx.morpho.asset_prices(weak, [chain_id])
            rows.extend(normalize.price_rows_morpho(items, ts))

    n = ctx.store.upsert(PRICES, rows)
    return f"{n} prices @ {ts:%Y-%m-%d %H:%M}"
