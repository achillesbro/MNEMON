"""Storage tests: idempotent upsert, exact big-int round-trips, date splitting."""

from datetime import datetime, timezone

import pyarrow.parquet as pq

from ingest.schemas import MARKET_STATE, MARKETS
from ingest.storage import Store, floor_ts


def row(ts_hour: int, market="0xaaa", supply=10**24):
    """A market_state row; supply defaults to > int64 max to prove exactness."""
    return {
        "ts": datetime(2026, 7, 9, ts_hour, 0, tzinfo=timezone.utc),
        "chain_id": 999,
        "market_id": market,
        "total_supply_assets": supply,
        "total_supply_shares": supply * 10**6,
        "total_borrow_assets": supply // 2,
        "total_borrow_shares": None,
        "rate_at_target": 9133578135,
        "utilization": 0.5,
        "oracle_price_raw": "68996150867968122500000000",
        "api_timestamp": None,
        "source": "live",
    }


def read_all(store: Store, spec):
    files = sorted(store.table_dir(spec).rglob("*.parquet"))
    import pandas as pd

    return pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)


def test_upsert_is_idempotent(tmp_path):
    store = Store(tmp_path)
    rows = [row(1), row(2)]
    assert store.upsert(MARKET_STATE, rows) == 2
    store.upsert(MARKET_STATE, rows)  # exact re-run: no duplicates
    df = read_all(store, MARKET_STATE)
    assert len(df) == 2


def test_upsert_replaces_on_key_and_heals_gaps(tmp_path):
    store = Store(tmp_path)
    store.upsert(MARKET_STATE, [row(1, supply=100)])
    # A later run re-covers the same bucket with a corrected value + adds one.
    store.upsert(MARKET_STATE, [row(1, supply=200), row(2)])
    df = read_all(store, MARKET_STATE).sort_values("ts")
    assert len(df) == 2
    assert int(df.iloc[0]["total_supply_assets"]) == 200  # new value won


def test_bigints_round_trip_exactly(tmp_path):
    store = Store(tmp_path)
    huge = 60876356185400123456789012345  # 29 digits, far past float precision
    store.upsert(MARKET_STATE, [row(1, supply=huge)])
    df = read_all(store, MARKET_STATE)
    assert int(df.iloc[0]["total_supply_assets"]) == huge
    assert int(df.iloc[0]["total_supply_shares"]) == huge * 10**6
    assert df.iloc[0]["oracle_price_raw"] == "68996150867968122500000000"
    assert df.iloc[0]["total_borrow_shares"] is None or str(df.iloc[0]["total_borrow_shares"]) == "None"


def test_rows_split_across_date_partitions(tmp_path):
    store = Store(tmp_path)
    r1, r2 = row(23), row(23)
    r2["ts"] = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)
    store.upsert(MARKET_STATE, [r1, r2])
    days = sorted(p.name for p in store.table_dir(MARKET_STATE).iterdir())
    assert days == ["date=2026-07-09", "date=2026-07-10"]


def test_unpartitioned_dimension_overwrites_by_key(tmp_path):
    store = Store(tmp_path)
    dim = {
        "chain_id": 999,
        "market_id": "0xaaa",
        "loan_token": "0x1",
        "loan_symbol": "USDT0",
        "loan_decimals": 6,
        "collateral_token": None,
        "collateral_symbol": None,
        "collateral_decimals": None,
        "oracle": None,
        "irm": "0x2",
        "lltv": 0,
        "creation_ts": datetime(2025, 7, 11, tzinfo=timezone.utc),
        "listed": True,
        "fetched_at": datetime(2026, 7, 9, tzinfo=timezone.utc),
    }
    store.upsert(MARKETS, [dim])
    dim2 = dict(dim, listed=False)
    store.upsert(MARKETS, [dim2])
    df = read_all(store, MARKETS)
    assert len(df) == 1
    assert not bool(df.iloc[0]["listed"])


def test_floor_ts_is_utc_and_bucket_aligned():
    dt = floor_ts(1783685251, 900)
    assert dt.tzinfo is not None
    assert dt.minute % 15 == 0 and dt.second == 0
