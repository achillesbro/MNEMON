"""DuckDB query layer: a .duckdb file holding views over the Parquet globs.

Views are re-created after every run (cheap, and picks up new tables). Raw
tables are exposed 1:1; v_market_state adds the joins/derivations you almost
always want: symbols, human-unit amounts, oracle price, APY at target.
"""

from __future__ import annotations

import logging

import duckdb

from ingest.config import Config
from ingest.schemas import ALL_TABLES
from ingest.storage import Store

log = logging.getLogger(__name__)

SECONDS_PER_YEAR = 31_536_000


def refresh_views(cfg: Config, store: Store) -> None:
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.duckdb_path))
    try:
        available = set()
        for spec in ALL_TABLES.values():
            if not store.has_data(spec):
                continue
            hive = ", hive_partitioning = 1" if spec.partitioned else ""
            con.execute(
                f"CREATE OR REPLACE VIEW {spec.name} AS "
                f"SELECT * FROM read_parquet('{store.parquet_glob(spec)}'{hive})"
            )
            available.add(spec.name)

        if {"market_state", "markets"} <= available:
            # Derivations live here, not in storage: raw state stays raw.
            # oracle price scale is 10^(36 + loanDec - collDec) per Morpho Blue;
            # rate_at_target is a per-second WAD rate (AdaptiveCurveIRM), so
            # APY at target = exp(rate * secondsPerYear) - 1.
            con.execute(f"""
                CREATE OR REPLACE VIEW v_market_state AS
                SELECT
                    ms.ts,
                    ms.chain_id,
                    ms.market_id,
                    m.loan_symbol,
                    m.collateral_symbol,
                    m.lltv::DOUBLE / 1e18                                    AS lltv,
                    ms.total_supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
                    ms.total_borrow_assets::DOUBLE / POW(10, m.loan_decimals) AS borrow_assets,
                    (ms.total_supply_assets - ms.total_borrow_assets)::DOUBLE
                        / POW(10, m.loan_decimals)                            AS liquidity,
                    ms.utilization,
                    ms.rate_at_target::DOUBLE / 1e18                          AS rate_at_target_per_sec,
                    EXP(ms.rate_at_target::DOUBLE / 1e18 * {SECONDS_PER_YEAR}) - 1 AS apy_at_target,
                    TRY_CAST(ms.oracle_price_raw AS DOUBLE)
                        / POW(10, 36 + m.loan_decimals - m.collateral_decimals) AS oracle_price,
                    ms.source
                FROM market_state ms
                LEFT JOIN markets m USING (chain_id, market_id)
            """)

        if {"vault_allocations", "markets"} <= available:
            con.execute("""
                CREATE OR REPLACE VIEW v_vault_allocations AS
                SELECT
                    va.ts,
                    va.chain_id,
                    va.vault,
                    va.market_id,
                    m.loan_symbol,
                    m.collateral_symbol,
                    va.supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
                    va.supply_cap::DOUBLE / POW(10, m.loan_decimals)    AS supply_cap,
                    va.source
                FROM vault_allocations va
                LEFT JOIN markets m USING (chain_id, market_id)
            """)
        log.info("duckdb views refreshed: %s", ", ".join(sorted(available)) or "none")
    finally:
        con.close()
