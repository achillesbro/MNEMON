"""Normalization tests against recorded API responses (tests/fixtures/).

The fixtures are real payloads from blue-api.morpho.org / coins.llama.fi /
yields.llama.fi captured on 2026-07-09, trimmed for size."""

from datetime import datetime, timezone

from mnemon import normalize
from mnemon.storage import floor_ts

TS = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)
KHYPE_MKT = "0xc5526286d537c890fdd879d17d80c4a22dc7196c1e1fff0dd6c853692a759c62"


def test_as_int_accepts_numbers_and_strings():
    # The API serializes BigInt as number when small, string when large.
    assert normalize.as_int(66127656) == 66127656
    assert normalize.as_int("68996150867968122500000000") == 68996150867968122500000000
    assert normalize.as_int(None) is None


def test_floor_ts_buckets():
    # 1783685251 = 2026-07-10 12:07:31 UTC
    assert floor_ts(1783685251, 900) == datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
    assert floor_ts(1783685251, 3600) == datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
    assert floor_ts(1783685251, 86400) == datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)


def test_market_state_rows_live(fixture):
    items = fixture("markets_live_state")["items"]
    rows = normalize.market_state_rows_live(items, TS)
    assert len(rows) == 2
    khype = next(r for r in rows if r["market_id"] == KHYPE_MKT)
    assert khype["chain_id"] == 999
    assert khype["total_supply_assets"] == 66127656
    assert khype["total_supply_shares"] == 60876356185400
    assert khype["rate_at_target"] == 9133578135
    # oracle price arrives as a big string and must stay exact
    assert khype["oracle_price_raw"] == "68996150867968122500000000"
    assert khype["ts"] == TS
    assert khype["source"] == "live"
    assert khype["api_timestamp"].year >= 2026


def test_market_state_rows_live_skips_null_state(fixture):
    items = fixture("markets_live_state")["items"]
    items[0]["state"] = None
    assert len(normalize.market_state_rows_live(items, TS)) == 1


def test_market_state_rows_history_merges_series_into_hour_buckets(fixture):
    hist = fixture("market_history")["historicalState"]
    rows = normalize.market_state_rows_history(KHYPE_MKT, 999, hist)
    assert rows, "expected rows from 6h window"
    # one row per hour bucket, all six raw columns populated
    assert len({r["ts"] for r in rows}) == len(rows)
    for r in rows:
        assert r["ts"].minute == 0 and r["ts"].second == 0
        assert r["source"] == "backfill"
        assert r["total_supply_assets"] is not None
        assert r["total_borrow_shares"] is not None
        assert r["utilization"] is not None
        assert r["oracle_price_raw"] is None  # API has no oracle price history
    # the trailing partial point ("now") must not create an extra bucket:
    # the fixture's first supplyAssets point is off-bucket
    raw_xs = [p["x"] for p in hist["supplyAssets"]]
    assert any(x % 3600 != 0 for x in raw_xs)


def test_market_state_history_later_point_wins_within_bucket():
    hist = {
        "supplyAssets": [
            {"x": 3600, "y": "100"},
            {"x": 3601, "y": "200"},  # later point in same bucket wins
        ]
    }
    rows = normalize.market_state_rows_history("0xabc", 1, hist)
    assert len(rows) == 1
    assert rows[0]["total_supply_assets"] == 200


def test_markets_dim_rows_handles_idle_market(fixture):
    items = fixture("markets_meta")["items"]
    rows = normalize.markets_dim_rows(items, TS)
    idle = next(r for r in rows if r["collateral_token"] is None)
    assert idle["collateral_symbol"] is None
    assert idle["loan_decimals"] == 6
    regular = next(r for r in rows if r["market_id"] == KHYPE_MKT)
    assert regular["collateral_symbol"] == "kHYPE"
    assert regular["lltv"] > 0
    assert regular["oracle"] == regular["oracle"].lower()
    assert regular["creation_ts"].tzinfo is not None


def test_vault_allocation_rows(fixture):
    vault = fixture("vault_allocations")
    rows = normalize.vault_allocation_rows(vault, 999, TS)
    assert len(rows) == 4
    for r in rows:
        assert r["vault"] == vault["address"].lower()
        assert r["supply_assets"] is not None
        assert r["supply_cap"] is not None
        assert r["source"] == "live"


def test_vault_allocation_history_rows(fixture):
    vault = fixture("vault_alloc_history")
    rows = normalize.vault_allocation_history_rows(vault, 999)
    assert rows
    # supplyAssets and supplyCap for the same (bucket, market) merge into one row
    keys = [(r["ts"], r["market_id"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert any(r["supply_assets"] is not None and r["supply_cap"] is not None for r in rows)
    assert all(r["supply_shares"] is None for r in rows)  # no shares series in the API


def test_position_rows(fixture):
    items = fixture("positions_page")["items"]
    rows = normalize.position_rows(items, TS)
    assert len(rows) == 3
    for r in rows:
        assert r["market_id"] == KHYPE_MKT
        assert r["borrower"].startswith("0x") and r["borrower"] == r["borrower"].lower()
        assert r["borrow_shares"] > 0
        assert r["collateral"] is not None


def test_price_rows_llama_current(fixture):
    coins = fixture("llama_current")["coins"]
    rows = normalize.price_rows_llama_current(coins, {"hyperliquid": 999}, TS)
    assert len(rows) == 2
    for r in rows:
        assert r["chain_id"] == 999
        assert r["source"] == "llama"
        assert r["price_usd"] > 0
        assert r["confidence"] is not None
    # unknown slugs are ignored rather than misattributed
    assert normalize.price_rows_llama_current(coins, {"ethereum": 1}, TS) == []


def test_price_rows_llama_chart_floors_to_hour(fixture):
    coins = fixture("llama_chart")["coins"]
    key, coin = next(iter(coins.items()))
    rows = normalize.price_rows_llama_chart(999, "0x5555555555555555555555555555555555555555", coin["prices"])
    assert rows
    for r in rows:
        assert r["ts"].minute == 0 and r["ts"].second == 0
        assert r["source"] == "llama_chart"


def test_price_rows_morpho_history(fixture):
    asset = fixture("asset_price_history")
    rows = normalize.price_rows_morpho_history(999, asset["address"], asset["historicalPriceUsd"])
    assert rows
    assert all(r["source"] == "morpho_history" for r in rows)
    assert all(r["token_address"] == asset["address"].lower() for r in rows)


def test_yield_pool_rows_filters_chain(fixture):
    pools = fixture("yield_pools_sample")["data"]
    rows = normalize.yield_pool_rows(pools, {"Hyperliquid L1"}, TS)
    assert len(rows) == 5  # fixture holds 5 Hyperliquid L1 pools + 2 other-chain
    assert all(r["chain"] == "Hyperliquid L1" for r in rows)
