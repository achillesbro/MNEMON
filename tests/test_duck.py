"""DuckDB view tests: build a tiny store from rows, refresh the views, and
assert the derived views exist and compute correctly. Pure-SQL views can
break silently on a schema tweak, so exercise each one end to end."""

from datetime import datetime, timezone

import duckdb
import pytest

from mnemon.config import Config
from mnemon.duck import refresh_views
from mnemon.schemas import MARKET_STATE, MARKETS, PRICES, VAULT_ALLOCATIONS
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
        dict(ts=TS, chain_id=999, token_address=USDT0, price_usd=0.999, source="llama", confidence=0.99),
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
            "v_liquidity_risk", "v_prices"} <= views


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
