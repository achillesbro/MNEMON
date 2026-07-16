"""DuckDB view tests: build a tiny store from rows, refresh the views, and
assert the derived views exist and compute correctly. Pure-SQL views can
break silently on a schema tweak, so exercise each one end to end."""

from datetime import datetime, timezone

import duckdb
import pytest

from mnemon.config import Config
from mnemon.duck import refresh_views
from mnemon.schemas import MARKET_STATE, MARKETS, POSITIONS, PRICES, VAULT_ALLOCATIONS
from mnemon.storage import Store

TS = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)
MID_A = "0xaaa"
MID_IDLE = "0xidle"
USDT0 = "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb"
KHYPE = "0xfd739d4e423301ce9385c1fb8850539d657c296d"
VAULT = "0x4dc97f968b0ba4edd32d1b9b8aaf54776c134d42"


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path)
    s.upsert(
        MARKETS,
        [
            dict(chain_id=999, market_id=MID_A, loan_token=USDT0, loan_symbol="USD₮0",
                 loan_decimals=6, collateral_token=KHYPE, collateral_symbol="kHYPE",
                 collateral_decimals=18, oracle="0xo", irm="0xi", lltv=915000000000000000,
                 creation_ts=TS, listed=True, fetched_at=TS),
            dict(chain_id=999, market_id=MID_IDLE, loan_token=USDT0, loan_symbol="USD₮0",
                 loan_decimals=6, collateral_token=None, collateral_symbol=None,
                 collateral_decimals=None, oracle=None, irm="0xi", lltv=0,
                 creation_ts=TS, listed=True, fetched_at=TS),
        ],
    )
    # two hourly market_state rows so utilization stats have something to chew on
    for hh, util in [(10, 0.98), (11, 1.0)]:
        s.upsert(MARKET_STATE, [dict(
            ts=TS.replace(hour=hh), chain_id=999, market_id=MID_A,
            total_supply_assets=1_000_000, total_supply_shares=1_000_000_000,
            total_borrow_assets=int(1_000_000 * util), total_borrow_shares=900_000_000,
            rate_at_target=9133578135, utilization=util, oracle_price_raw="68996150867968122500000000",
            api_timestamp=None, source="backfill")])
    s.upsert(VAULT_ALLOCATIONS, [
        dict(ts=TS, chain_id=999, vault=VAULT, market_id=MID_A, supply_assets=300_000_000,
             supply_shares=None, supply_cap=10_000_000_000, source="live"),
        dict(ts=TS, chain_id=999, vault=VAULT, market_id=MID_IDLE, supply_assets=100_000_000,
             supply_shares=None, supply_cap=10_000_000_000, source="live"),
    ])
    s.upsert(PRICES, [
        dict(ts=TS, chain_id=999, token_address=KHYPE, price_usd=68.5, source="llama", confidence=0.99),
        dict(ts=TS.replace(hour=13), chain_id=999, token_address=KHYPE, price_usd=70.0,
             source="llama", confidence=0.99),  # second hour -> one log return
        dict(ts=TS, chain_id=999, token_address=USDT0, price_usd=0.999, source="llama", confidence=0.99),
    ])
    # three borrowers: one near liquidation, one dominant by size, one tiny
    s.upsert(POSITIONS, [
        dict(ts=TS, chain_id=999, market_id=MID_A, borrower="0xb1", collateral=10**18,
             borrow_shares=10**9, borrow_assets=600_000, supply_shares=0, health_factor=1.02),
        dict(ts=TS, chain_id=999, market_id=MID_A, borrower="0xb2", collateral=10**19,
             borrow_shares=10**10, borrow_assets=300_000, supply_shares=0, health_factor=2.5),
        dict(ts=TS, chain_id=999, market_id=MID_A, borrower="0xb3", collateral=10**17,
             borrow_shares=10**8, borrow_assets=100_000, supply_shares=0, health_factor=8.0),
    ])
    return s


@pytest.fixture
def con(tmp_path, store):
    cfg = Config.model_validate({
        "data_dir": str(tmp_path),
        "chains": [{"chain_id": 999, "name": "hyperevm", "llama_slug": "hyperliquid"}],
        "vaults": [],
    })
    refresh_views(cfg, store)
    return duckdb.connect(str(cfg.duckdb_path), read_only=True)


def test_all_views_exist(con):
    views = {r[0] for r in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
    assert {"v_market_state", "v_vault_allocations", "v_vault_snapshot",
            "v_liquidity_risk", "v_prices", "v_market_snapshot",
            "v_position_risk", "v_utilization_regime", "v_price_returns"} <= views


def test_views_carry_full_ids(con):
    """Ids must never be truncated in the stored/derived data (display-side
    truncation is a pandas setting, not ours)."""
    for view, col in [("v_market_snapshot", "market_id"), ("v_vault_snapshot", "vault"),
                      ("v_position_risk", "market_id"), ("v_prices", "token_address")]:
        vals = [r[0] for r in con.execute(f"SELECT {col} FROM {view}").fetchall()]
        assert vals and all(v.startswith("0x") for v in vals)
    # the 42-char vault address survives intact
    assert con.execute("SELECT vault FROM v_vault_snapshot LIMIT 1").fetchone()[0] == VAULT


def test_v_market_snapshot_one_row_per_market_latest(con):
    # epoch instead of selecting ts: fetching timestamptz needs pytz (not a
    # dep), and EXTRACT(hour ...) would follow the session timezone
    rows = con.execute(
        "SELECT market_id, EXTRACT(epoch FROM ts), utilization, lltv, listed FROM v_market_snapshot"
    ).fetchall()
    assert len(rows) == 1  # one market has state; one row, the latest
    market_id, epoch, util, lltv, listed = rows[0]
    assert market_id == MID_A
    assert epoch == TS.replace(hour=11).timestamp()  # newest of the two rows
    assert util == pytest.approx(1.0)
    assert lltv == pytest.approx(0.915) and listed


def test_v_position_risk_hf_and_concentration(con):
    row = con.execute("""
        SELECT borrowers, total_borrow, min_hf, borrowers_hf_lt_105,
               debt_hf_lt_105, pct_debt_hf_lt_105, top3_debt_pct
        FROM v_position_risk WHERE market_id = ?
    """, [MID_A]).fetchone()
    borrowers, total, min_hf, n_risky, debt_risky, pct_risky, top3 = row
    assert borrowers == 3
    assert total == pytest.approx(1.0)          # 1_000_000 / 10^6
    assert min_hf == pytest.approx(1.02)
    assert n_risky == 1                          # only 0xb1 under 1.05
    assert debt_risky == pytest.approx(0.6)
    assert pct_risky == pytest.approx(60.0)
    assert top3 == pytest.approx(100.0)          # only 3 borrowers total


def test_v_utilization_regime_windows(con):
    # fixture data is from 2026-07-09 — outside any trailing window, so the
    # FILTERed aggregates must be NULL rather than wrong, and current_* still real
    row = con.execute("""
        SELECT avg_util_7d, current_util_pct FROM v_utilization_regime WHERE market_id = ?
    """, [MID_A]).fetchone()
    assert row[0] is None
    assert row[1] == pytest.approx(100.0)


def test_v_price_returns_log_return_and_vol(con):
    import math

    rows = con.execute("""
        SELECT log_return_1h, vol_7d_ann FROM v_price_returns
        WHERE symbol = 'kHYPE' ORDER BY ts
    """).fetchall()
    assert len(rows) == 2
    assert rows[0][0] is None  # first observation has no return
    assert rows[1][0] == pytest.approx(math.log(70.0 / 68.5))
    # a single return has no sample stddev -> NULL, not 0
    assert rows[1][1] is None


def test_v_market_state_derivations(con):
    row = con.execute("""
        SELECT supply_assets, apy_at_target, oracle_price, lltv
        FROM v_market_state WHERE market_id = ? ORDER BY ts LIMIT 1
    """, [MID_A]).fetchone()
    supply, apy, oracle_price, lltv = row
    assert supply == pytest.approx(1.0)         # 1_000_000 / 10^6
    assert apy > 0                               # exp(rate*yr)-1
    assert oracle_price == pytest.approx(68.996150867968, abs=1e-6)  # 10^(36+6-18) scale
    assert lltv == pytest.approx(0.915)


def test_v_vault_snapshot_weights_sum_to_100(con):
    rows = con.execute("""
        SELECT collateral_symbol, weight_pct, cap_used_pct FROM v_vault_snapshot
        WHERE vault = ? ORDER BY weight_pct DESC
    """, [VAULT]).fetchall()
    assert {r[0] for r in rows} == {"kHYPE", "IDLE"}  # null collateral -> 'IDLE'
    assert sum(r[1] for r in rows) == pytest.approx(100.0)
    assert rows[0][1] == pytest.approx(75.0)          # 300 of 400
    assert rows[0][2] == pytest.approx(3.0)           # 300 / 10_000 cap


def test_v_liquidity_risk_util_stats(con):
    row = con.execute("""
        SELECT hours_observed, pct_time_gt95, pct_time_gt99, current_util_pct
        FROM v_liquidity_risk WHERE market_id = ?
    """, [MID_A]).fetchone()
    hours, gt95, gt99, current = row
    assert hours == 2
    assert gt95 == pytest.approx(100.0)   # both rows > 0.95
    assert gt99 == pytest.approx(50.0)    # only the 1.0 row > 0.99
    assert current == pytest.approx(100.0)  # latest ts row


def test_v_prices_attaches_symbol(con):
    rows = dict(con.execute("SELECT token_address, symbol FROM v_prices").fetchall())
    assert rows[KHYPE] == "kHYPE"
    assert rows[USDT0] == "USD₮0"
