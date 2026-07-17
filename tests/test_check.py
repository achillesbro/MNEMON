"""check report: gap math must not invent phantom missing buckets."""

from datetime import datetime, timezone

from mnemon.check import run_check
from mnemon.config import Config
from mnemon.schemas import MARKET_STATE, MARKETS
from mnemon.storage import Store


def state_row(ts, market="0xaaa"):
    return {
        "ts": ts,
        "chain_id": 999,
        "market_id": market,
        "total_supply_assets": 1_000_000,
        "total_supply_shares": 1_000_000_000,
        "total_borrow_assets": 800_000,
        "total_borrow_shares": 700_000_000,
        "rate_at_target": 9133578135,
        "utilization": 0.8,
        "oracle_price_raw": None,
        "api_timestamp": None,
        "source": "live",
    }


def make_cfg(tmp_path):
    return Config.model_validate({
        "data_dir": str(tmp_path),
        "chains": [{"chain_id": 999, "name": "hyperevm", "llama_slug": "hyperliquid"}],
        "vaults": [],
    })


def test_no_phantom_gap_when_latest_row_is_late_in_the_hour(tmp_path):
    """Regression: hourly rows at 10:00 and 11:00 plus a live 5-min row at
    11:50 is a gap-free series — float division + rounding CAST used to report
    '1 missing' whenever MAX(ts) sat past the half-bucket point."""
    store = Store(tmp_path)
    for hh, mm in [(10, 0), (11, 0), (11, 50)]:
        store.upsert(MARKET_STATE, [state_row(datetime(2026, 7, 16, hh, mm, tzinfo=timezone.utc))])
    report = run_check(make_cfg(tmp_path))
    assert "missing" not in report.split("missing 1h buckets ==")[1].splitlines()[1] \
        or "none" in report.split("missing 1h buckets ==")[1].splitlines()[1]
    assert "none - every entity is gap-free" in report


def test_real_gap_is_still_reported_with_full_ids(tmp_path):
    store = Store(tmp_path)
    market = "0x" + "ab" * 32  # full 66-char id
    # hours 10 and 13: hours 11 and 12 genuinely missing
    for hh in (10, 13):
        store.upsert(MARKET_STATE, [state_row(datetime(2026, 7, 16, hh, tzinfo=timezone.utc), market)])
    report = run_check(make_cfg(tmp_path))
    assert "2 missing of 4" in report
    assert market in report  # full id, never truncated


def test_job_list_includes_heal(tmp_path):
    report = run_check(make_cfg(tmp_path))
    assert "heal" in report


def test_markets_dimension_has_no_ts_and_is_skipped_by_gap_checks(tmp_path):
    store = Store(tmp_path)
    store.upsert(MARKETS, [dict(
        chain_id=999, market_id="0xaaa", loan_token="0x1", loan_symbol="X", loan_decimals=6,
        collateral_token=None, collateral_symbol=None, collateral_decimals=None,
        oracle=None, irm="0x2", lltv=0,
        creation_ts=datetime(2026, 1, 1, tzinfo=timezone.utc), listed=True,
        fetched_at=datetime(2026, 7, 16, tzinfo=timezone.utc))])
    report = run_check(make_cfg(tmp_path))
    assert "== markets ==" in report
