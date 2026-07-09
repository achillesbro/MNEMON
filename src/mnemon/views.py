"""Single source of truth for the derived DuckDB views.

Both the ingestion (`mnemon.duck.refresh_views`, which persists views into
data/mnemon.duckdb) and the read API (`mnemon.reader`, which builds them in an
in-memory connection over the Parquet globs) create these same views from the
definitions here — so what the cron persists and what a consumer sees can
never drift apart.

Each view's SQL references the raw table names only (never file paths), so it
works identically whether the raw tables are Parquet-backed views in the
persisted db or read_parquet views in a consumer's in-memory db.
"""

from __future__ import annotations

from dataclasses import dataclass

SECONDS_PER_YEAR = 31_536_000


@dataclass(frozen=True)
class DerivedView:
    name: str
    deps: frozenset[str]  # raw tables that must be present for this view to build
    sql: str  # SELECT body; wrapped in `CREATE OR REPLACE VIEW <name> AS ...`


DERIVED_VIEWS: list[DerivedView] = [
    DerivedView(
        "v_market_state",
        frozenset({"market_state", "markets"}),
        # Derivations live here, not in storage: raw state stays raw.
        # oracle price scale is 10^(36 + loanDec - collDec) per Morpho Blue;
        # rate_at_target is a per-second WAD rate (AdaptiveCurveIRM), so
        # APY at target = exp(rate * secondsPerYear) - 1.
        f"""
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
        """,
    ),
    DerivedView(
        "v_vault_allocations",
        frozenset({"vault_allocations", "markets"}),
        """
        SELECT
            va.ts,
            va.chain_id,
            va.vault,
            va.market_id,
            m.loan_symbol,
            m.collateral_symbol,
            va.supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
            va.supply_cap::DOUBLE   / POW(10, m.loan_decimals) AS supply_cap,
            va.source
        FROM vault_allocations va
        LEFT JOIN markets m USING (chain_id, market_id)
        """,
    ),
    DerivedView(
        "v_vault_snapshot",
        frozenset({"vault_allocations", "markets"}),
        # Each vault's CURRENT allocation (its latest ts), with each market's
        # share of the vault and how much of its cap is used. The self-join on
        # MAX(ts) keeps it live as new rows land.
        """
        WITH latest AS (SELECT vault, MAX(ts) AS mx FROM vault_allocations GROUP BY vault)
        SELECT
            va.ts,
            va.vault,
            m.loan_symbol,
            COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
            va.market_id,
            va.supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
            va.supply_cap::DOUBLE   / POW(10, m.loan_decimals) AS supply_cap,
            100.0 * va.supply_assets
                / NULLIF(SUM(va.supply_assets) OVER (PARTITION BY va.vault), 0) AS weight_pct,
            100.0 * va.supply_assets / NULLIF(va.supply_cap, 0) AS cap_used_pct
        FROM vault_allocations va
        JOIN latest l ON va.vault = l.vault AND va.ts = l.mx
        LEFT JOIN markets m USING (chain_id, market_id)
        """,
    ),
    DerivedView(
        "v_liquidity_risk",
        frozenset({"market_state", "markets"}),
        # Per-market withdrawal-liquidity profile over all history (how often
        # utilization was extreme = how often you could not have exited) plus
        # the current utilization / rate regime. arg_max(x, ts) = value of x in
        # the row with the newest ts.
        f"""
        SELECT
            ms.chain_id,
            ms.market_id,
            m.loan_symbol,
            COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
            COUNT(*) AS hours_observed,
            100.0 * AVG(CASE WHEN ms.utilization > 0.95 THEN 1 ELSE 0 END) AS pct_time_gt95,
            100.0 * AVG(CASE WHEN ms.utilization > 0.99 THEN 1 ELSE 0 END) AS pct_time_gt99,
            100.0 * AVG(ms.utilization)               AS avg_util_pct,
            100.0 * arg_max(ms.utilization, ms.ts)    AS current_util_pct,
            100.0 * (EXP(arg_max(ms.rate_at_target, ms.ts)::DOUBLE / 1e18 * {SECONDS_PER_YEAR}) - 1)
                AS current_apy_at_target_pct
        FROM market_state ms
        LEFT JOIN markets m USING (chain_id, market_id)
        GROUP BY ms.chain_id, ms.market_id, m.loan_symbol, m.collateral_symbol
        """,
    ),
    DerivedView(
        "v_prices",
        frozenset({"prices", "markets"}),
        # Attach a human symbol to each price row by matching the token address
        # against the loan/collateral columns of the markets dimension.
        """
        SELECT
            p.ts,
            p.chain_id,
            p.token_address,
            COALESCE(ml.loan_symbol, mc.collateral_symbol) AS symbol,
            p.price_usd,
            p.source,
            p.confidence
        FROM prices p
        LEFT JOIN (SELECT DISTINCT chain_id, loan_token, loan_symbol FROM markets) ml
            ON p.chain_id = ml.chain_id AND p.token_address = ml.loan_token
        LEFT JOIN (SELECT DISTINCT chain_id, collateral_token, collateral_symbol FROM markets) mc
            ON p.chain_id = mc.chain_id AND p.token_address = mc.collateral_token
        """,
    ),
]


def create_derived_views(con, available: set[str]) -> list[str]:
    """Create every derived view whose dependencies are present. `con` is any
    DuckDB connection where the raw tables already exist as tables/views."""
    created: list[str] = []
    for view in DERIVED_VIEWS:
        if view.deps <= available:
            con.execute(f"CREATE OR REPLACE VIEW {view.name} AS {view.sql}")
            created.append(view.name)
    return created
