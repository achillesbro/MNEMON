"""Healing semantics: insert_missing fills gaps without touching live rows."""

from datetime import datetime, timezone

import pyarrow.parquet as pq

from mnemon.schemas import MARKET_STATE
from mnemon.storage import Store


def state_row(hour: int, source: str, oracle: str | None):
    return {
        "ts": datetime(2026, 7, 16, hour, 0, tzinfo=timezone.utc),
        "chain_id": 999,
        "market_id": "0xaaa",
        "total_supply_assets": 1_000_000,
        "total_supply_shares": 1_000_000_000,
        "total_borrow_assets": 800_000,
        "total_borrow_shares": 700_000_000,
        "rate_at_target": 9133578135,
        "utilization": 0.8,
        "oracle_price_raw": oracle,
        "api_timestamp": None,
        "source": source,
    }


def read_all(store: Store):
    files = sorted(store.table_dir(MARKET_STATE).rglob("*.parquet"))
    import pandas as pd

    return pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True).sort_values("ts")


def test_insert_missing_fills_only_gaps(tmp_path):
    store = Store(tmp_path)
    # Live captured hours 8 and 11; hours 9-10 lost to an outage.
    store.upsert(MARKET_STATE, [state_row(8, "live", "111"), state_row(11, "live", "222")])

    # Heal re-pulls hourly history covering 8..11 (all four hours).
    healed = [state_row(h, "backfill", None) for h in (8, 9, 10, 11)]
    added = store.insert_missing(MARKET_STATE, healed)

    assert added == 2  # only the two missing hours were inserted
    df = read_all(store)
    assert list(df["source"]) == ["live", "backfill", "backfill", "live"]
    # live rows kept their oracle price — the heal did NOT null it out
    # (pandas reads parquet nulls back as NaN, hence the isna checks)
    prices = list(df["oracle_price_raw"])
    assert prices[0] == "111" and prices[3] == "222"
    assert df["oracle_price_raw"].isna().tolist() == [False, True, True, False]


def test_insert_missing_is_idempotent(tmp_path):
    store = Store(tmp_path)
    store.upsert(MARKET_STATE, [state_row(8, "live", "111")])
    healed = [state_row(h, "backfill", None) for h in (8, 9)]
    assert store.insert_missing(MARKET_STATE, healed) == 1
    assert store.insert_missing(MARKET_STATE, healed) == 0  # second heal: no-op
    assert len(read_all(store)) == 2


def test_upsert_still_replaces(tmp_path):
    # Regression guard: the refactor must not change upsert semantics.
    store = Store(tmp_path)
    store.upsert(MARKET_STATE, [state_row(8, "live", "111")])
    store.upsert(MARKET_STATE, [state_row(8, "live", "999")])
    df = read_all(store)
    assert len(df) == 1
    assert df.iloc[0]["oracle_price_raw"] == "999"


def test_heal_cadence_in_config():
    from mnemon.config import Cadences, Config

    assert Cadences().heal == 86400
    cfg = Config.model_validate(
        {"data_dir": "/tmp/x", "chains": [], "vaults": [], "heal_lookback_hours": 72}
    )
    assert cfg.heal_lookback_hours == 72
