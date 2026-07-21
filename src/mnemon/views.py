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
        "v_vault_drift",
        frozenset({"vault_allocations", "markets"}),
        # Reallocation event log: one row per (vault, market, ts) where the
        # market's share of the vault moved by more than 0.5pp since the
        # previous observation — sized for a live "what did HEGEMON just do"
        # feed. Interest accrual drifts weights a few bps per bucket; the
        # 0.5pp floor keeps that noise out while catching real reallocations
        # (consumers can filter harder on weight_change_pct). Newest first.
        """
        WITH alloc AS (
            SELECT
                va.ts,
                va.chain_id,
                va.vault,
                va.market_id,
                m.loan_symbol,
                COALESCE(m.collateral_symbol, 'IDLE') AS collateral_symbol,
                va.supply_assets::DOUBLE / POW(10, m.loan_decimals) AS supply_assets,
                100.0 * va.supply_assets
                    / NULLIF(SUM(va.supply_assets) OVER (PARTITION BY va.vault, va.ts), 0) AS weight_pct
            FROM vault_allocations va
            LEFT JOIN markets m USING (chain_id, market_id)
            WHERE va.supply_assets IS NOT NULL
        ),
        d AS (
            SELECT *,
                LAG(weight_pct) OVER (PARTITION BY vault, market_id ORDER BY ts) AS prev_weight_pct,
                supply_assets - LAG(supply_assets) OVER (PARTITION BY vault, market_id ORDER BY ts)
                    AS supply_assets_change
            FROM alloc
        )
        SELECT
            ts,
            chain_id,
            vault,
            market_id,
            loan_symbol,
            collateral_symbol,
            supply_assets,
            supply_assets_change,
            prev_weight_pct,
            weight_pct,
            weight_pct - prev_weight_pct AS weight_change_pct
        FROM d
        WHERE ABS(weight_pct - prev_weight_pct) > 0.5
        ORDER BY ts DESC
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
    DerivedView(
        "v_market_apy",
        frozenset({"market_state"}),
        # Supply APY with exactly the HEGEMON bot's math (maths.ts):
        # AdaptiveCurveIRM piecewise curve (steepness 4, target u 0.9) from
        # rate_at_target, supplyRate = borrowRate * u * (1 - fee), 3-term
        # Taylor compounding of rate*secondsPerYear. KNOWN GAP: market_state
        # has no `fee` column so fee = 0 is assumed (tracked HyperEVM markets
        # run fee = 0 today; see docs/SCHEMA_NOTES.md). Verified to 1e-12
        # against a Python port of the bot math in tests/test_views_v2.py.
        f"""
        WITH base AS (
            SELECT ts, chain_id, market_id,
                   utilization                   AS u,
                   rate_at_target::DOUBLE / 1e18 AS rat
            FROM market_state
            WHERE rate_at_target IS NOT NULL AND utilization IS NOT NULL
        ), borrow AS (
            SELECT *,
                CASE
                    WHEN u >= 1.0 THEN 4 * rat
                    WHEN u >= 0.9 THEN rat + (4 * rat - rat) * (u - 0.9) / (1.0 - 0.9)
                    WHEN u > 0    THEN rat / 4 + (rat - rat / 4) * u / 0.9
                    ELSE rat / 4
                END AS borrow_rate_per_sec
            FROM base
        ), rates AS (
            SELECT *, borrow_rate_per_sec * u AS supply_rate_per_sec  -- * (1 - fee), fee unavailable
            FROM borrow
        )
        SELECT ts, chain_id, market_id, u,
               borrow_rate_per_sec,
               (borrow_rate_per_sec * {SECONDS_PER_YEAR})
                   + POW(borrow_rate_per_sec * {SECONDS_PER_YEAR}, 2) / 2
                   + POW(borrow_rate_per_sec * {SECONDS_PER_YEAR}, 3) / 6 AS borrow_apy,
               (supply_rate_per_sec * {SECONDS_PER_YEAR})
                   + POW(supply_rate_per_sec * {SECONDS_PER_YEAR}, 2) / 2
                   + POW(supply_rate_per_sec * {SECONDS_PER_YEAR}, 3) / 6 AS supply_apy
        FROM rates
        """,
    ),
    DerivedView(
        "v_market_health",
        frozenset({"market_state", "markets", "prices"}),
        # Broken-market classifier with hysteresis (operator-tuned constants):
        #   R1 ratchet: apy_at_target > 50% -> broken, exit < 25%. The
        #      AdaptiveCurveIRM ratchets rateAtTarget ~2x per ~5 days pinned at
        #      u=1 (and decays symmetrically), so this threshold is inherently
        #      time-integrated — no extra dwell needed.
        #   R2 pinned: u >= 0.999 for the entire trailing 24h -> broken, exit
        #      after 48h entirely below 0.95. Span guards avoid false
        #      positives across data holes.
        #   R3 dust: supply < $1k USD -> broken unconditionally.
        #   Thin exemption: R1/R2 only apply while supply < $25k USD — a DEEP
        #      market sustaining a high ratcheted rate is an opportunity, not
        #      a defect. Unpriced markets are treated as thin (rules apply)
        #      but never dust (can't prove size).
        # State machine = enter/exit events + LAST_VALUE(... IGNORE NULLS).
        # Fixed-rule algebra only; data-driven thresholds belong to myrmidons.
        """
        WITH priced AS (
            SELECT
                ms.ts, ms.chain_id, ms.market_id, ms.utilization AS u,
                EXP(ms.rate_at_target::DOUBLE / 1e18 * 31536000) - 1 AS apy_at_target,
                ms.total_supply_assets::DOUBLE / POW(10, m.loan_decimals)
                    * p.price_usd AS supply_usd,
                (ms.total_supply_assets - ms.total_borrow_assets)::DOUBLE
                    / POW(10, m.loan_decimals) * p.price_usd AS available_usd
            FROM market_state ms
            LEFT JOIN markets m USING (chain_id, market_id)
            ASOF LEFT JOIN prices p
                ON p.chain_id = ms.chain_id
               AND p.token_address = LOWER(m.loan_token)
               AND p.ts <= ms.ts
        ), events AS (
            SELECT *,
                COALESCE(supply_usd < 25000, TRUE)  AS is_thin,
                COALESCE(supply_usd < 1000, FALSE)  AS is_dust,
                CASE WHEN apy_at_target > 0.50 THEN 1
                     WHEN apy_at_target < 0.25 THEN 0
                END AS r1_event,
                CASE WHEN MIN(CASE WHEN u >= 0.999 THEN 1 ELSE 0 END) OVER w24 = 1
                          AND ts - MIN(ts) OVER w24 >= INTERVAL 22 HOUR THEN 1
                     WHEN MAX(u) OVER w48 < 0.95
                          AND ts - MIN(ts) OVER w48 >= INTERVAL 44 HOUR THEN 0
                END AS r2_event
            FROM priced
            WINDOW
                w24 AS (PARTITION BY chain_id, market_id ORDER BY ts
                        RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND CURRENT ROW),
                w48 AS (PARTITION BY chain_id, market_id ORDER BY ts
                        RANGE BETWEEN INTERVAL 48 HOUR PRECEDING AND CURRENT ROW)
        ), states AS (
            SELECT *,
                COALESCE(LAST_VALUE(r1_event IGNORE NULLS) OVER cum, 0) = 1 AS r1_state,
                COALESCE(LAST_VALUE(r2_event IGNORE NULLS) OVER cum, 0) = 1 AS r2_state
            FROM events
            WINDOW cum AS (PARTITION BY chain_id, market_id ORDER BY ts
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        )
        SELECT
            ts, chain_id, market_id, u, apy_at_target, supply_usd, available_usd,
            is_dust OR ((r1_state OR r2_state) AND is_thin) AS is_broken,
            CASE WHEN is_dust THEN 'dust'
                 WHEN r1_state AND is_thin THEN 'rate_ratchet'
                 WHEN r2_state AND is_thin THEN 'pinned_util'
            END AS broken_reason
        FROM states
        """,
    ),
    DerivedView(
        "v_apy_spread",
        frozenset({"market_state", "markets", "prices"}),  # via v_market_apy + v_market_health
        # spread_to_best is measured against the best NON-BROKEN market at
        # that ts (else a dust market's exploded APY poisons every spread).
        # Broken rows keep their spread vs the eligible leader, flagged.
        """
        SELECT a.ts, a.chain_id, a.market_id, a.supply_apy,
               COALESCE(h.is_broken, FALSE) AS is_broken,
               a.supply_apy - MAX(a.supply_apy)
                   FILTER (NOT COALESCE(h.is_broken, FALSE))
                   OVER (PARTITION BY a.chain_id, a.ts) AS spread_to_best
        FROM v_market_apy a
        LEFT JOIN v_market_health h USING (ts, chain_id, market_id)
        """,
    ),
    DerivedView(
        "v_util_spells",
        frozenset({"market_state"}),
        # Gaps-and-islands: contiguous episodes of u >= threshold per market
        # for the strategy's U_SAT (0.92) / U_CRIT (0.95). An episode breaks
        # when u drops below the threshold or the series has a hole > 2h
        # (backfilled portion is hourly). Episode identification only —
        # statistical estimation belongs to the myrmidons Python library.
        """
        WITH thresholds(threshold) AS (VALUES (0.92), (0.95)),
        flagged AS (
            SELECT t.threshold, ms.chain_id, ms.market_id, ms.ts,
                   ms.utilization AS u,
                   (ms.total_supply_assets - ms.total_borrow_assets)::DOUBLE AS available_liquidity_raw,
                   CASE WHEN ms.utilization >= t.threshold THEN 1 ELSE 0 END AS above
            FROM market_state ms
            CROSS JOIN thresholds t
        ), runs AS (
            SELECT *,
                CASE WHEN above = 1
                      AND (LAG(above) OVER w = 1)
                      AND ts - LAG(ts) OVER w <= INTERVAL 2 HOUR
                     THEN 0 ELSE 1 END AS is_start
            FROM flagged
            WINDOW w AS (PARTITION BY threshold, chain_id, market_id ORDER BY ts)
        ), islands AS (
            SELECT *,
                SUM(is_start) OVER (
                    PARTITION BY threshold, chain_id, market_id ORDER BY ts
                ) AS spell_id
            FROM runs
            WHERE above = 1
        )
        SELECT chain_id, market_id, threshold,
               MIN(ts)                               AS start_ts,
               MAX(ts)                               AS end_ts,
               DATE_DIFF('minute', MIN(ts), MAX(ts)) AS duration_min,
               MAX(u)                                AS peak_u,
               MIN(available_liquidity_raw)          AS min_available_liquidity
        FROM islands
        GROUP BY chain_id, market_id, threshold, spell_id
        """,
    ),
    DerivedView(
        "v_hegemon_benchmark",
        frozenset({"market_state", "markets", "prices", "bot_scores"}),
        # Passive counterfactuals per ts over the ELIGIBLE universe (every
        # non-broken market, per v_market_health — not just where HEGEMON
        # allocates, to avoid the echo chamber), plus the same aggregates over
        # the bot's own scored set, plus the INVESTABLE tier — eligible AND
        # available liquidity >= $10k USD (the bot's minAvailableLiquidity
        # floor): the "deployable truth". opportunity_gap_apy compares the
        # whole universe; deployable_gap_apy compares only markets the bot
        # could actually enter at size.
        """
        WITH scored AS (SELECT DISTINCT market_id FROM bot_scores),
        joined AS (
            SELECT a.ts, a.chain_id, a.market_id, a.supply_apy,
                   COALESCE(h.is_broken, FALSE) AS is_broken,
                   NOT COALESCE(h.is_broken, FALSE)
                       AND COALESCE(h.available_usd >= 10000, FALSE) AS investable,
                   a.market_id IN (SELECT market_id FROM scored) AS in_bot_set
            FROM v_market_apy a
            LEFT JOIN v_market_health h USING (ts, chain_id, market_id)
        )
        SELECT ts, chain_id,
               AVG(supply_apy) FILTER (NOT is_broken)              AS equal_weight_apy,
               MAX(supply_apy) FILTER (NOT is_broken)              AS best_market_apy,
               COUNT(*)        FILTER (NOT is_broken)              AS markets,
               AVG(supply_apy) FILTER (investable)                 AS investable_equal_weight_apy,
               MAX(supply_apy) FILTER (investable)                 AS investable_best_apy,
               COUNT(*)        FILTER (investable)                 AS investable_markets,
               AVG(supply_apy) FILTER (in_bot_set)                 AS bot_equal_weight_apy,
               MAX(supply_apy) FILTER (in_bot_set)                 AS bot_best_apy,
               COUNT(*)        FILTER (in_bot_set)                 AS bot_markets,
               MAX(supply_apy) FILTER (NOT is_broken)
                   - MAX(supply_apy) FILTER (in_bot_set)           AS opportunity_gap_apy,
               MAX(supply_apy) FILTER (investable)
                   - MAX(supply_apy) FILTER (in_bot_set)           AS deployable_gap_apy
        FROM joined
        GROUP BY ts, chain_id
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
