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
            ("ts", TS),  # cadence bucket; API serves current positions only
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

BOT_SCORES = TableSpec(
    name="bot_scores",
    schema=pa.schema(
        [
            ("ts", TS),  # tick timestamp, floored to a 60s bucket (see jobs/bot_events.py)
            ("chain_id", pa.int32()),
            ("vault", pa.string()),
            ("tick_id", pa.string()),
            ("market_id", pa.string()),
            ("collateral_symbol", pa.string()),
            ("loan_symbol", pa.string()),
            ("u", pa.float64()),  # utilization 0..1
            ("apy", pa.float64()),  # supply APY, decimal (0.14 = 14%)
            ("exit_ratio", pa.float64()),
            ("score", pa.float64()),
            ("gate", pa.string()),  # null when not gated
            ("vault_assets", BIGINT),  # bot position in this market, asset units
            ("total_assets", BIGINT),  # vault-level, denormalized per row
            ("idle_assets", BIGINT),
            ("source_file", pa.string()),
        ]
    ),
    keys=["vault", "ts", "market_id"],
)

BOT_EVENTS = TableSpec(
    name="bot_events",
    schema=pa.schema(
        [
            ("ts", TS),  # event's own timestamp (not bucketed; keyed by tick_id+seq)
            ("chain_id", pa.int32()),
            ("tick_id", pa.string()),
            ("seq", pa.int32()),  # line index within its source file (emission order)
            ("type", pa.string()),
            ("tx_hash", pa.string()),  # null unless a tx event
            ("block_number", BIGINT),  # null unless tx_confirmed
            ("payload", pa.string()),  # event-specific fields as JSON (plan, reason, gas, ...)
            ("source_file", pa.string()),
        ]
    ),
    keys=["tick_id", "seq"],
)

VAULT_V2_STATE = TableSpec(
    name="vault_v2_state",
    schema=pa.schema(
        [
            ("ts", TS),
            ("chain_id", pa.int32()),
            ("vault", pa.string()),
            ("total_assets", BIGINT),
            ("idle_assets", BIGINT),
            ("total_supply", BIGINT),  # share supply (18-dec)
            ("share_price", pa.float64()),  # assets per share, API float
            ("total_assets_usd", pa.float64()),
        ]
    ),
    keys=["chain_id", "vault", "ts"],
)

MARKET_FLOWS = TableSpec(
    name="market_flows",
    schema=pa.schema(
        [
            ("ts", TS),  # block timestamp (event time, not bucketed)
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("block_number", BIGINT),
            ("tx_hash", pa.string()),
            ("log_index", pa.int32()),
            # Supply | Withdraw | Borrow | Repay | SupplyCollateral |
            # WithdrawCollateral | Liquidation (MarketTransactionType enum)
            ("type", pa.string()),
            ("account", pa.string()),  # the position's user (liquidatee on Liquidation)
            # loan-token units for Supply/Withdraw/Borrow/Repay,
            # collateral-token units for SupplyCollateral/WithdrawCollateral,
            # null on Liquidation (see repaid/seized below)
            ("assets", BIGINT),
            ("shares", BIGINT),  # null for collateral transfers and liquidations
            ("liquidator", pa.string()),  # Liquidation only ------------------
            ("repaid_assets", BIGINT),  # loan-token units
            ("seized_assets", BIGINT),  # collateral-token units
            ("bad_debt_assets", BIGINT),  # loan-token units
        ]
    ),
    keys=["tx_hash", "log_index"],
)

SUPPLIER_POSITIONS = TableSpec(
    name="supplier_positions",
    schema=pa.schema(
        [
            ("ts", TS),  # hourly bucket; API serves current positions only
            ("chain_id", pa.int32()),
            ("market_id", pa.string()),
            ("supplier", pa.string()),
            ("supply_shares", BIGINT),
            ("supply_assets", BIGINT),
        ]
    ),
    keys=["chain_id", "market_id", "supplier", "ts"],
)

VAULT_V2_FLOWS = TableSpec(
    name="vault_v2_flows",
    schema=pa.schema(
        [
            ("ts", TS),  # block timestamp (event time, not bucketed)
            ("chain_id", pa.int32()),
            ("vault", pa.string()),
            ("block_number", BIGINT),
            ("tx_hash", pa.string()),
            ("log_index", pa.int32()),
            ("type", pa.string()),  # Deposit | Withdraw
            ("sender", pa.string()),
            ("receiver", pa.string()),  # null on deposits (onBehalf is the recipient)
            ("on_behalf", pa.string()),
            ("assets", BIGINT),
            ("shares", BIGINT),
        ]
    ),
    # First event-keyed table: identity is the on-chain event, not a clock bucket.
    keys=["tx_hash", "log_index"],
)

ALL_TABLES: dict[str, TableSpec] = {
    t.name: t
    for t in [
        MARKET_STATE,
        MARKETS,
        VAULT_ALLOCATIONS,
        POSITIONS,
        PRICES,
        YIELD_POOLS,
        LEGACY_SNAPSHOTS,
        BOT_SCORES,
        BOT_EVENTS,
        VAULT_V2_STATE,
        VAULT_V2_FLOWS,
        MARKET_FLOWS,
        SUPPLIER_POSITIONS,
    ]
}
