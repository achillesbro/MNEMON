"""MnemonReader tests: build a tiny store, then read it back the way a
downstream project would."""

from datetime import datetime, timezone

import pytest

from mnemon.reader import MnemonReader
from mnemon.schemas import MARKET_STATE, MARKETS, PRICES, VAULT_ALLOCATIONS
from mnemon.storage import Store

TS = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)
MID = "0xaaa"
USDT0 = "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb"
KHYPE = "0xfd739d4e423301ce9385c1fb8850539d657c296d"
VAULT = "0x4dc97f968b0ba4edd32d1b9b8aaf54776c134d42"


@pytest.fixture
def data_dir(tmp_path):
    s = Store(tmp_path)
    s.upsert(MARKETS, [dict(
        chain_id=999, market_id=MID, loan_token=USDT0, loan_symbol="USD₮0", loan_decimals=6,
        collateral_token=KHYPE, collateral_symbol="kHYPE", collateral_decimals=18,
        oracle="0xo", irm="0xi", lltv=915000000000000000, creation_ts=TS, listed=True, fetched_at=TS)])
    for hh, util in [(10, 0.98), (11, 1.0)]:
        s.upsert(MARKET_STATE, [dict(
            ts=TS.replace(hour=hh), chain_id=999, market_id=MID,
            total_supply_assets=1_000_000, total_supply_shares=1_000_000_000,
            total_borrow_assets=int(1_000_000 * util), total_borrow_shares=900_000_000,
            rate_at_target=9133578135, utilization=util,
            oracle_price_raw="68996150867968122500000000", api_timestamp=None, source="backfill")])
    s.upsert(VAULT_ALLOCATIONS, [dict(
        ts=TS, chain_id=999, vault=VAULT, market_id=MID, supply_assets=300_000_000,
        supply_shares=None, supply_cap=10_000_000_000, source="live")])
    s.upsert(PRICES, [dict(
        ts=TS, chain_id=999, token_address=KHYPE, price_usd=68.5, source="llama", confidence=0.99)])
    return tmp_path


def test_tables_lists_raw_and_views(data_dir):
    r = MnemonReader(data_dir)
    t = r.tables()
    assert {"market_state", "markets", "v_market_state", "v_liquidity_risk", "v_prices"} <= set(t)


def test_market_state_latest(data_dir):
    df = MnemonReader(data_dir).market_state_latest()
    assert len(df) == 1
    assert df.iloc[0]["utilization"] == pytest.approx(1.0)  # newest of the two rows
    assert df.iloc[0]["supply_assets"] == pytest.approx(1.0)


def test_typed_filters_are_parameterized(data_dir):
    r = MnemonReader(data_dir)
    assert len(r.market_state(collateral="kHYPE")) == 2
    assert len(r.market_state(collateral="NOPE")) == 0
    assert len(r.prices(symbol="kHYPE")) == 1


def test_vault_snapshot(data_dir):
    df = MnemonReader(data_dir).vault_snapshot(vault=VAULT.upper())  # case-insensitive
    assert len(df) == 1
    assert df.iloc[0]["weight_pct"] == pytest.approx(100.0)


def test_env_var_fallback(data_dir, monkeypatch):
    monkeypatch.setenv("MNEMON_DATA", str(data_dir))
    assert MnemonReader().market_state_latest().iloc[0]["collateral_symbol"] == "kHYPE"


def test_missing_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        MnemonReader(tmp_path / "nope")


def test_no_config_raises(monkeypatch):
    monkeypatch.delenv("MNEMON_DATA", raising=False)
    with pytest.raises(ValueError):
        MnemonReader()


def test_context_manager(data_dir):
    with MnemonReader(data_dir) as r:
        assert not r.liquidity_risk().empty
