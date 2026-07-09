"""Table definitions: pyarrow schemas, upsert keys, partitioning.

Design principle: store *raw state* (asset/share amounts as exact integers,
oracle price as the raw scaled value) and derive metrics at query time.

Type choices:
- Raw token amounts / shares are DECIMAL(38, 0): exact (18-decimal share
  amounts overflow int64) and natively queryable in DuckDB without casts.
- The Morpho oracle price is scaled by 10^(36 + loanDec - collateralDec) and
  can exceed 38 digits, so it is stored as a string (`oracle_price_raw`) and
  converted to a float in the v_market_state view.
- Timestamps are UTC, floored to the job's cadence bucket (the upsert key),
  with the API's own timestamp kept alongside where available.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

BIGINT = pa.decimal128(38, 0)
TS = pa.timestamp("us", tz="UTC")


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: pa.Schema
    keys: list[str]  # upsert key; re-runs replace rows with equal keys
    partitioned: bool = True  # date=YYYY-MM-DD directories derived from `ts`


MARKET_STATE = TableSpec(
    name="market_state",
    schema=pa.schema(
        [
            ("ts", TS),
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("total_supply_assets", BIGINT),
            ("total_supply_shares", BIGINT),
            ("total_borrow_assets", BIGINT),
            ("total_borrow_shares", BIGINT),
            ("rate_at_target", BIGINT),  # per-second rate, WAD-scaled (AdaptiveCurveIRM)
            ("utilization", pa.float64()),
            ("oracle_price_raw", pa.string()),  # scale: 10^(36 + loanDec - collDec)
            ("api_timestamp", TS),  # API's own state timestamp, for staleness checks
            ("source", pa.string()),  # live | backfill | rpc
        ]
    ),
    keys=["chain_id", "market_id", "ts"],
)

MARKETS = TableSpec(
    name="markets",
    schema=pa.schema(
        [
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("loan_token", pa.string()),
            ("loan_symbol", pa.string()),
            ("loan_decimals", pa.int32()),
            ("collateral_token", pa.string()),  # null for idle markets
            ("collateral_symbol", pa.string()),
            ("collateral_decimals", pa.int32()),
            ("oracle", pa.string()),
            ("irm", pa.string()),
            ("lltv", BIGINT),  # WAD-scaled
            ("creation_ts", TS),
            ("listed", pa.bool_()),
            ("fetched_at", TS),
        ]
    ),
    keys=["chain_id", "market_id"],
    partitioned=False,  # dimension table: one current row per market
)

VAULT_ALLOCATIONS = TableSpec(
    name="vault_allocations",
    schema=pa.schema(
        [
            ("ts", TS),
            ("chain_id", pa.int32()),
            ("vault", pa.string()),
            ("market_id", pa.string()),
            ("supply_assets", BIGINT),
            ("supply_shares", BIGINT),  # null in backfilled rows (no history series)
            ("supply_cap", BIGINT),
            ("source", pa.string()),
        ]
    ),
    keys=["chain_id", "vault", "market_id", "ts"],
)

POSITIONS = TableSpec(
    name="positions",
    schema=pa.schema(
        [
            ("ts", TS),  # daily bucket; API serves current positions only
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("borrower", pa.string()),
            ("collateral", BIGINT),
            ("borrow_shares", BIGINT),
            ("borrow_assets", BIGINT),
            ("supply_shares", BIGINT),
            ("health_factor", pa.float64()),
        ]
    ),
    keys=["chain_id", "market_id", "borrower", "ts"],
)

PRICES = TableSpec(
    name="prices",
    schema=pa.schema(
        [
            ("ts", TS),
            ("chain_id", pa.int32()),
            ("token_address", pa.string()),  # lowercased
            ("price_usd", pa.float64()),
            ("source", pa.string()),  # llama | llama_chart | morpho | morpho_history
            ("confidence", pa.float64()),  # llama only
        ]
    ),
    keys=["chain_id", "token_address", "ts"],
)

YIELD_POOLS = TableSpec(
    name="yield_pools",
    schema=pa.schema(
        [
            ("ts", TS),
            ("pool_id", pa.string()),
            ("chain", pa.string()),
            ("project", pa.string()),
            ("symbol", pa.string()),
            ("tvl_usd", pa.float64()),
            ("apy", pa.float64()),
            ("apy_base", pa.float64()),
            ("apy_reward", pa.float64()),
        ]
    ),
    keys=["pool_id", "ts"],
)

LEGACY_SNAPSHOTS = TableSpec(
    name="legacy_snapshots",
    schema=pa.schema(
        [
            ("ts", TS),
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("symbol", pa.string()),
            ("loan_token", pa.string()),
            ("collateral_token", pa.string()),
            ("utilization", pa.float64()),
            ("borrow_apy", pa.float64()),
            ("supply_apy", pa.float64()),
            ("available_liquidity", pa.float64()),  # loan-token units (old derived value)
            ("vault_allocation", pa.float64()),
            ("source_file", pa.string()),
        ]
    ),
    keys=["chain_id", "market_id", "ts"],
)

ALL_TABLES: dict[str, TableSpec] = {
    t.name: t
    for t in [MARKET_STATE, MARKETS, VAULT_ALLOCATIONS, POSITIONS, PRICES, YIELD_POOLS, LEGACY_SNAPSHOTS]
}
