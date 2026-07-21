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
from mnemon.schemas import BOT_SCORES, MARKET_STATE
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


@pytest.fixture()
def con(tmp_path):
    store = Store(tmp_path / "data")
    rows = []
    # market A crosses 0.92 for 2 buckets then recovers; market B stays below
    for i, u in enumerate([0.88, 0.93, 0.96, 0.85]):
        rows.append(_row("0xaaa", TS0 + timedelta(hours=i), u))
    for i, u in enumerate([0.70, 0.75, 0.80, 0.78]):
        rows.append(_row("0xbbb", TS0 + timedelta(hours=i), u))
    store.upsert(MARKET_STATE, rows)
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
        "SELECT market_id, u, supply_apy FROM v_market_apy ORDER BY market_id, ts"
    ).fetchall()
    assert len(got) == 8
    for market_id, u, supply_apy in got:
        assert supply_apy == pytest.approx(bot_supply_apy(u, RAT), rel=1e-12), (market_id, u)


def test_v_apy_spread_leader_is_zero(con):
    rows = con.execute(
        "SELECT CAST(ts AS VARCHAR), MAX(spread_to_best), COUNT(*) FILTER (spread_to_best = 0) "
        "FROM v_apy_spread GROUP BY ts"
    ).fetchall()
    for _ts, max_spread, leaders in rows:
        assert max_spread == 0 and leaders >= 1


def test_v_util_spells_islands(con):
    spells = con.execute(
        "SELECT market_id, threshold, duration_min, peak_u FROM v_util_spells ORDER BY threshold"
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
