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
    DerivedView(
        "v_market_snapshot",
        frozenset({"market_state", "markets"}),
        # One row per market: the newest state plus the full dimension — a
        # "market screener" so consumers grab one table instead of joining three.
        f"""
        SELECT
            ms.ts,
            ms.chain_id,
            ms.market_id,
            m.loan_symbol,
            COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
            m.loan_token,
            m.collateral_token,
            m.oracle,
            m.irm,
            m.creation_ts,
            m.listed,
            m.lltv::DOUBLE / 1e18                                     AS lltv,
            ms.total_supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
            ms.total_borrow_assets::DOUBLE / POW(10, m.loan_decimals) AS borrow_assets,
            (ms.total_supply_assets - ms.total_borrow_assets)::DOUBLE
                / POW(10, m.loan_decimals)                            AS liquidity,
            ms.utilization,
            EXP(ms.rate_at_target::DOUBLE / 1e18 * {SECONDS_PER_YEAR}) - 1 AS apy_at_target,
            TRY_CAST(ms.oracle_price_raw AS DOUBLE)
                / POW(10, 36 + m.loan_decimals - m.collateral_decimals) AS oracle_price
        FROM market_state ms
        LEFT JOIN markets m USING (chain_id, market_id)
        QUALIFY ms.ts = MAX(ms.ts) OVER (PARTITION BY ms.chain_id, ms.market_id)
        """,
    ),
    DerivedView(
        "v_position_risk",
        frozenset({"positions", "markets"}),
        # Per-market borrower risk from the latest positions snapshot: how much
        # debt sits within 5% of liquidation (HF < 1.05), and how concentrated
        # the book is. list_sum(list_slice(list_sort(...))) = top-3 debt share.
        """
        WITH latest AS (
            SELECT chain_id, market_id, MAX(ts) AS mx FROM positions GROUP BY 1, 2
        ),
        p AS (
            SELECT po.* FROM positions po
            JOIN latest l ON po.chain_id = l.chain_id AND po.market_id = l.market_id AND po.ts = l.mx
        )
        SELECT
            p.chain_id,
            p.market_id,
            m.loan_symbol,
            COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
            MAX(p.ts) AS ts,
            COUNT(*) AS borrowers,
            SUM(p.borrow_assets)::DOUBLE / POW(10, m.loan_decimals) AS total_borrow,
            MIN(p.health_factor) AS min_hf,
            COUNT(*) FILTER (WHERE p.health_factor < 1.05) AS borrowers_hf_lt_105,
            COALESCE(SUM(p.borrow_assets) FILTER (WHERE p.health_factor < 1.05), 0)::DOUBLE
                / POW(10, m.loan_decimals) AS debt_hf_lt_105,
            100.0 * COALESCE(SUM(p.borrow_assets) FILTER (WHERE p.health_factor < 1.05), 0)::DOUBLE
                / NULLIF(SUM(p.borrow_assets)::DOUBLE, 0) AS pct_debt_hf_lt_105,
            100.0 * list_sum(list_slice(list_sort(list(p.borrow_assets::DOUBLE), 'DESC'), 1, 3))
                / NULLIF(SUM(p.borrow_assets)::DOUBLE, 0) AS top3_debt_pct
        FROM p
        LEFT JOIN markets m USING (chain_id, market_id)
        GROUP BY p.chain_id, p.market_id, m.loan_symbol, m.collateral_symbol, m.loan_decimals
        """,
    ),
    DerivedView(
        "v_utilization_regime",
        frozenset({"market_state", "markets"}),
        # Trailing 7d/30d utilization regime per market. The all-history stats
        # in v_liquidity_risk dilute as markets mature; these windows answer
        # "what is this market like NOW". now() evaluates at query time, so the
        # windows always trail the present.
        f"""
        SELECT
            ms.chain_id,
            ms.market_id,
            m.loan_symbol,
            COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
            100.0 * AVG(ms.utilization) FILTER (WHERE ms.ts > now() - INTERVAL 7 DAY)  AS avg_util_7d,
            100.0 * AVG(ms.utilization) FILTER (WHERE ms.ts > now() - INTERVAL 30 DAY) AS avg_util_30d,
            100.0 * AVG(CASE WHEN ms.utilization > 0.95 THEN 1 ELSE 0 END)
                FILTER (WHERE ms.ts > now() - INTERVAL 7 DAY)  AS pct_time_gt95_7d,
            100.0 * AVG(CASE WHEN ms.utilization > 0.95 THEN 1 ELSE 0 END)
                FILTER (WHERE ms.ts > now() - INTERVAL 30 DAY) AS pct_time_gt95_30d,
            100.0 * AVG(CASE WHEN ms.utilization > 0.99 THEN 1 ELSE 0 END)
                FILTER (WHERE ms.ts > now() - INTERVAL 7 DAY)  AS pct_time_gt99_7d,
            100.0 * AVG(CASE WHEN ms.utilization > 0.99 THEN 1 ELSE 0 END)
                FILTER (WHERE ms.ts > now() - INTERVAL 30 DAY) AS pct_time_gt99_30d,
            100.0 * arg_max(ms.utilization, ms.ts) AS current_util_pct,
            100.0 * (EXP(arg_max(ms.rate_at_target, ms.ts)::DOUBLE / 1e18 * {SECONDS_PER_YEAR}) - 1)
                AS current_apy_at_target_pct
        FROM market_state ms
        LEFT JOIN markets m USING (chain_id, market_id)
        GROUP BY ms.chain_id, ms.market_id, m.loan_symbol, m.collateral_symbol
        """,
    ),
    DerivedView(
        "v_price_returns",
        frozenset({"prices", "markets"}),
        # Hourly log-returns and rolling annualized volatility per token — the
        # collateral-risk input (compare vol against a market's 1 - LLTV buffer).
        # Restricted to on-the-hour rows so a finer prices cadence doesn't
        # silently change the return horizon; sqrt(8760) annualizes 1h returns.
        # RANGE windows (not ROWS) so price gaps don't stretch the lookback.
        """
        WITH hourly AS (
            SELECT ts, chain_id, token_address, symbol, price_usd
            FROM v_prices
            WHERE EXTRACT(minute FROM ts) = 0 AND price_usd > 0
        ),
        r AS (
            SELECT *,
                   LN(price_usd / LAG(price_usd) OVER (PARTITION BY chain_id, token_address ORDER BY ts))
                       AS log_return_1h
            FROM hourly
        )
        SELECT
            ts,
            chain_id,
            token_address,
            symbol,
            price_usd,
            log_return_1h,
            STDDEV_SAMP(log_return_1h) OVER (
                PARTITION BY chain_id, token_address ORDER BY ts
                RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
            ) * SQRT(8760) AS vol_7d_ann,
            STDDEV_SAMP(log_return_1h) OVER (
                PARTITION BY chain_id, token_address ORDER BY ts
                RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
            ) * SQRT(8760) AS vol_30d_ann
        FROM r
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
