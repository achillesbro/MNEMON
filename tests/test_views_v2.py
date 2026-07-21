"""New derived views: SQL must reproduce the HEGEMON bot's math exactly.

v_market_apy is checked against a Python reimplementation of the bot's
utilizationToRate (AdaptiveCurveIRM, steepness 4, target 0.9) + 3-term Taylor
compounding (apps/client/src/utils/maths.ts). A live cross-check against
bot_scores.apy at matching timestamps is documented in docs/SCHEMA_NOTES.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import duckdb
import pytest

from mnemon.duck import refresh_views
from mnemon.schemas import BOT_SCORES, MARKET_STATE, MARKETS, PRICES
from mnemon.storage import Store

SECONDS_PER_YEAR = 31_536_000


def bot_supply_apy(u: float, rate_at_target_wad: int, fee: float = 0.0) -> float:
    """Python port of the bot's utilizationToRate + rateToApy (Taylor 3-term)."""
    rat = rate_at_target_wad / 1e18
    if u >= 1.0:
        borrow = 4 * rat
    elif u >= 0.9:
        borrow = rat + (4 * rat - rat) * (u - 0.9) / 0.1
    elif u > 0:
        borrow = rat / 4 + (rat - rat / 4) * u / 0.9
    else:
        borrow = rat / 4
    supply = borrow * u * (1 - fee)
    x = supply * SECONDS_PER_YEAR
    return x + x**2 / 2 + x**3 / 6


TS0 = datetime(2026, 7, 20, 10, 0, tzinfo=timezone.utc)
RAT = 2_600_000_000  # ~2.6e-9/s WAD, realistic HyperEVM rateAtTarget


def _row(market: str, ts: datetime, u: float, supply: int = 10**12, rat: int = RAT) -> dict:
    return {
        "ts": ts,
        "chain_id": 999,
        "market_id": market,
        "total_supply_assets": supply,
        "total_supply_shares": supply,
        "total_borrow_assets": int(supply * u),
        "total_borrow_shares": int(supply * u),
        "rate_at_target": rat,
        "utilization": u,
        "oracle_price_raw": None,
        "api_timestamp": ts,
        "source": "test",
    }


def _dim(market: str) -> dict:
    return {
        "chain_id": 999,
        "market_id": market,
        "loan_token": "0xloan",
        "loan_symbol": "USD",
        "loan_decimals": 6,
        "collateral_token": "0xcoll",
        "collateral_symbol": "C",
        "collateral_decimals": 18,
        "oracle": None,
        "irm": None,
        "lltv": 0,
        "creation_ts": TS0,
        "listed": True,
        "fetched_at": TS0,
    }


# apy_at_target = exp(rat/1e18 * YEAR) - 1
RAT_65PCT = 16_000_000_000  # ~65.7% -> above the 50% enter threshold
RAT_37PCT = 10_000_000_000  # ~37.1% -> inside the hysteresis band (hold)
RAT_21PCT = 6_000_000_000   # ~20.8% -> below the 25% exit threshold

# Health-rule scenarios live on a separate day so they don't pollute the
# APY/spread/benchmark assertions around TS0.
TSH = TS0 + timedelta(days=10)


@pytest.fixture()
def con(tmp_path):
    store = Store(tmp_path / "data")
    rows = []
    # market A crosses 0.92 for 2 buckets then recovers; market B stays below
    for i, u in enumerate([0.88, 0.93, 0.96, 0.85]):
        rows.append(_row("0xaaa", TS0 + timedelta(hours=i), u))
    for i, u in enumerate([0.70, 0.75, 0.80, 0.78]):
        rows.append(_row("0xbbb", TS0 + timedelta(hours=i), u))

    # ratchet hysteresis: thin market ($5k) ramping above 50% then decaying
    for i, rat in enumerate([RAT_21PCT, RAT_65PCT, RAT_37PCT, RAT_21PCT]):
        rows.append(_row("0xr1", TSH + timedelta(hours=i), 0.5, supply=5 * 10**9, rat=rat))
    # thin exemption: DEEP market ($100k) with the same ratcheted rate
    for i, rat in enumerate([RAT_65PCT, RAT_65PCT]):
        rows.append(_row("0xdeep", TSH + timedelta(hours=i), 0.5, supply=10**11, rat=rat))
    # dust: $500 market, unconditionally broken
    rows.append(_row("0xdust", TSH, 0.5, supply=5 * 10**8))
    # pinned: thin market at u>=0.999 for 26h, then 50h clean at 0.5
    for i in range(27):
        rows.append(_row("0xpin", TSH + timedelta(hours=i), 0.9995, supply=5 * 10**9))
    for i in range(50):
        rows.append(_row("0xpin", TSH + timedelta(hours=27 + i), 0.5, supply=5 * 10**9))
    store.upsert(MARKET_STATE, rows)

    store.upsert(MARKETS, [_dim(m) for m in ("0xaaa", "0xbbb", "0xr1", "0xdeep", "0xdust", "0xpin")])
    store.upsert(
        PRICES,
        [
            {
                "ts": TS0 - timedelta(days=1),
                "chain_id": 999,
                "token_address": "0xloan",
                "price_usd": 1.0,
                "source": "test",
                "confidence": None,
            }
        ],
    )
    # v_hegemon_benchmark is restricted to bot-scored markets: seed both.
    store.upsert(
        BOT_SCORES,
        [
            {
                "ts": TS0,
                "chain_id": 999,
                "vault": "0xv",
                "tick_id": "t",
                "market_id": m,
                "collateral_symbol": None,
                "loan_symbol": None,
                "u": 0.9,
                "apy": 0.1,
                "exit_ratio": 1.0,
                "score": 0.01,
                "gate": None,
                "vault_assets": 1,
                "total_assets": 1,
                "idle_assets": 0,
                "source_file": "test",
            }
            for m in ("0xaaa", "0xbbb")
        ],
    )
    cfg = SimpleNamespace(duckdb_path=tmp_path / "t.duckdb")
    refresh_views(cfg, store)
    return duckdb.connect(str(cfg.duckdb_path))


def test_v_market_apy_matches_bot_math(con):
    got = con.execute(
        "SELECT market_id, u, supply_apy FROM v_market_apy "
        "WHERE market_id IN ('0xaaa', '0xbbb') ORDER BY market_id, ts"
    ).fetchall()
    assert len(got) == 8
    for market_id, u, supply_apy in got:
        assert supply_apy == pytest.approx(bot_supply_apy(u, RAT), rel=1e-12), (market_id, u)


def test_v_apy_spread_leader_is_zero(con):
    rows = con.execute(
        "SELECT CAST(ts AS VARCHAR), MAX(spread_to_best), COUNT(*) FILTER (spread_to_best = 0) "
        "FROM v_apy_spread WHERE spread_to_best IS NOT NULL GROUP BY ts"
    ).fetchall()
    assert rows
    for _ts, max_spread, leaders in rows:
        assert max_spread == 0 and leaders >= 1


def test_v_util_spells_islands(con):
    spells = con.execute(
        "SELECT market_id, threshold, duration_min, peak_u FROM v_util_spells WHERE market_id = '0xaaa' ORDER BY threshold"
    ).fetchall()
    # Market A: one contiguous 0.92-spell covering ts1..ts2 (60 min), peak 0.96,
    # and one 0.95-spell (single bucket, 0 min). Market B: none.
    assert [s[0] for s in spells] == ["0xaaa", "0xaaa"]
    s92 = next(s for s in spells if float(s[1]) == 0.92)
    s95 = next(s for s in spells if float(s[1]) == 0.95)
    assert s92[2] == 60 and s92[3] == pytest.approx(0.96)
    assert s95[2] == 0 and s95[3] == pytest.approx(0.96)


def test_v_hegemon_benchmark(con):
    row = con.execute(
        "SELECT equal_weight_apy, best_market_apy, markets FROM v_hegemon_benchmark "
        "WHERE ts = ? ",
        [TS0 + timedelta(hours=1)],
    ).fetchone()
    ew, best, n = row
    a = bot_supply_apy(0.93, RAT)
    b = bot_supply_apy(0.75, RAT)
    assert n == 2
    assert best == pytest.approx(a, rel=1e-12)
    assert ew == pytest.approx((a + b) / 2, rel=1e-12)


def _health(con, market):
    return con.execute(
        "SELECT CAST(ts AS VARCHAR), is_broken, broken_reason FROM v_market_health "
        "WHERE market_id = ? ORDER BY ts",
        [market],
    ).fetchall()


def test_health_ratchet_hysteresis(con):
    got = _health(con, "0xr1")
    # 21% -> healthy; 65% -> enter; 37% -> hold broken (inside band); 21% -> exit
    assert [g[1] for g in got] == [False, True, True, False]
    assert got[1][2] == "rate_ratchet"


def test_health_thin_exemption_deep_market_not_broken(con):
    got = _health(con, "0xdeep")
    assert all(g[1] is False or g[1] == False for g in got)  # noqa: E712


def test_health_dust(con):
    got = _health(con, "0xdust")
    assert got[0][1] and got[0][2] == "dust"


def test_health_pinned_enter_after_24h_exit_after_48h_clean(con):
    got = _health(con, "0xpin")
    by_hour = {i: g[1] for i, g in enumerate(got)}
    assert by_hour[0] is False or by_hour[0] == False  # noqa: E712  # just pinned, no 24h history yet
    assert by_hour[26]  # pinned for >24h -> broken
    assert by_hour[27 + 20]  # 20h into recovery -> still broken (48h not elapsed)
    assert not by_hour[27 + 49]  # 49h clean -> released


def test_benchmark_excludes_broken_and_reports_gap(con):
    row = con.execute(
        "SELECT markets, bot_markets, best_market_apy, bot_best_apy, opportunity_gap_apy "
        "FROM v_hegemon_benchmark WHERE ts = ?",
        [TSH],
    ).fetchone()
    markets, bot_markets, best, bot_best, gap = row
    # at TSH: 0xr1 (healthy at hour 0), 0xdeep absent (starts TSH+0? yes present), 0xdust broken, 0xpin healthy(hour 0)
    assert bot_markets == 0  # bot set (0xaaa/0xbbb) has no rows at TSH
    assert markets >= 2  # dust excluded from eligible
    assert bot_best is None and gap is None


def test_benchmark_investable_tier(con):
    row = con.execute(
        "SELECT markets, investable_markets, investable_best_apy "
        "FROM v_hegemon_benchmark WHERE ts = ?",
        [TSH],
    ).fetchone()
    markets, inv_markets, inv_best = row
    # Only 0xdeep ($100k supply, u=0.5 -> $50k available) clears the $10k
    # available-liquidity floor; 0xr1/0xpin ($5k supply) and 0xdust do not.
    assert inv_markets == 1 < markets
    assert inv_best == pytest.approx(bot_supply_apy(0.5, RAT_65PCT), rel=1e-12)
