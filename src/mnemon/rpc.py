"""On-chain view-call fallback, used ONLY when the Morpho API returns no state
for a tracked market (coverage gap). Low volume by construction: a handful of
eth_call per affected market per run, no logs, no backfills.

web3 is imported lazily so the rest of the pipeline works even if it isn't
installed or no rpc_url is configured.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from mnemon.config import ChainConfig

log = logging.getLogger(__name__)

MORPHO_BLUE_ABI = [
    {
        "name": "market",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "id", "type": "bytes32"}],
        "outputs": [
            {"name": "totalSupplyAssets", "type": "uint128"},
            {"name": "totalSupplyShares", "type": "uint128"},
            {"name": "totalBorrowAssets", "type": "uint128"},
            {"name": "totalBorrowShares", "type": "uint128"},
            {"name": "lastUpdate", "type": "uint128"},
            {"name": "fee", "type": "uint128"},
        ],
    },
]


def fetch_market_state_rpc(chain: ChainConfig, market_ids: list[str], ts: datetime) -> list[dict]:
    """market_state rows straight from the Morpho Blue contract. rate_at_target,
    utilization and oracle price are left null — they need extra contracts and
    this path only exists to keep the raw totals series unbroken."""
    if not chain.rpc_url or not chain.morpho_blue:
        log.warning("chain %s: no rpc_url/morpho_blue configured, skipping rpc fallback", chain.chain_id)
        return []

    from web3 import Web3  # lazy: only needed on this rare path

    w3 = Web3(Web3.HTTPProvider(chain.rpc_url, request_kwargs={"timeout": 15}))
    contract = w3.eth.contract(address=Web3.to_checksum_address(chain.morpho_blue), abi=MORPHO_BLUE_ABI)

    rows = []
    for market_id in market_ids:
        try:
            m = contract.functions.market(bytes.fromhex(market_id[2:])).call()
        except Exception:
            log.exception("rpc fallback failed for market %s", market_id)
            continue
        supply_assets, supply_shares, borrow_assets, borrow_shares = m[0], m[1], m[2], m[3]
        rows.append(
            {
                "ts": ts,
                "chain_id": chain.chain_id,
                "market_id": market_id,
                "total_supply_assets": supply_assets,
                "total_supply_shares": supply_shares,
                "total_borrow_assets": borrow_assets,
                "total_borrow_shares": borrow_shares,
                "rate_at_target": None,
                "utilization": borrow_assets / supply_assets if supply_assets else None,
                "oracle_price_raw": None,
                "api_timestamp": datetime.fromtimestamp(m[4], tz=timezone.utc) if m[4] else None,
                "source": "rpc",
            }
        )
    return rows
