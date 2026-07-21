"""export job: build a store with healthy / dust / ratchet / pinned markets on
one timeline, run the job, and read the JSON snapshots back the way the FE will.

The classifier math itself is proven in test_views_v2.py; here we assert the
export's shaping, filtering, and file contract (schema_version, freshness cut,
ordering, sparkline, spell `open` flag, atomic overwrite)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from mnemon.jobs.export import build_util_spells, job_export, run_export
from mnemon.reader import MnemonReader
from mnemon.schemas import MARKET_STATE, MARKETS, POSITIONS, PRICES
from mnemon.storage import Store

TS0 = datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc)
LOAN = "0xloan"
RAT_LOW = 2_600_000_000    # ~8.5% apy_at_target — healthy
RAT_HIGH = 16_000_000_000  # ~65% apy_at_target — above the 50% ratchet enter


def _state(market: str, ts: datetime, u: float, supply: int, rat: int) -> dict:
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


def _dim(market: str, collateral: str | None) -> dict:
    return {
        "chain_id": 999,
        "market_id": market,
        "loan_token": LOAN,
        "loan_symbol": "USDT0",
        "loan_decimals": 6,
        "collateral_token": "0xcoll" if collateral else None,
        "collateral_symbol": collateral,
        "collateral_decimals": 18,
        "oracle": None,
        "irm": None,
        "lltv": 860000000000000000,  # 0.86 WAD
        "creation_ts": TS0,
        "listed": True,
        "fetched_at": TS0,
    }


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(tmp_path / "data")
    rows: list[dict] = []
    # 0xgood: deep ($1M), healthy rate, hourly for 30h, util below spell bands.
    for i in range(30):
        rows.append(_state("0xgood", TS0 + timedelta(hours=i), 0.80, 10**12, RAT_LOW))
    # 0xr1: thin ($5k), ratcheted rate throughout -> rate_ratchet.
    for i in range(30):
        rows.append(_state("0xr1", TS0 + timedelta(hours=i), 0.50, 5 * 10**9, RAT_HIGH))
    # 0xpin: thin ($5k), pinned u>=0.999 for the full 30h -> pinned_util.
    for i in range(30):
        rows.append(_state("0xpin", TS0 + timedelta(hours=i), 0.9995, 5 * 10**9, RAT_LOW))
    # 0xdust: single recent row, $500 -> dust (unconditional).
    rows.append(_state("0xdust", TS0 + timedelta(hours=29), 0.50, 5 * 10**8, RAT_LOW))
    s.upsert(MARKET_STATE, rows)

    s.upsert(MARKETS, [
        _dim("0xgood", "kHYPE"),
        _dim("0xr1", "wstHYPE"),
        _dim("0xpin", "UBTC"),
        _dim("0xdust", None),  # idle market: null collateral
    ])
    s.upsert(PRICES, [{
        "ts": TS0 - timedelta(days=1),
        "chain_id": 999,
        "token_address": LOAN,
        "price_usd": 1.0,
        "source": "test",
        "confidence": None,
    }])
    # Borrower book for 0xgood: 3 borrowers, one within 5% of liquidation.
    def _pos(borrower: str, borrow: int, hf: float) -> dict:
        return {
            "ts": TS0 + timedelta(hours=29),
            "chain_id": 999,
            "market_id": "0xgood",
            "borrower": borrower,
            "collateral": borrow * 2,
            "borrow_shares": borrow,
            "borrow_assets": borrow,
            "supply_shares": 0,
            "health_factor": hf,
        }
    s.upsert(POSITIONS, [
        _pos("0xb1", 700_000, 1.02),   # near liquidation (HF < 1.05)
        _pos("0xb2", 200_000, 1.60),
        _pos("0xb3", 100_000, 2.50),
    ])
    return s


def _ctx(store: Store, export_dir: Path):
    cfg = SimpleNamespace(data_dir=store.data_dir, export_dir=export_dir)
    return SimpleNamespace(cfg=cfg, now=(TS0 + timedelta(hours=30)).timestamp())


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def test_job_writes_both_files(store, tmp_path):
    out = tmp_path / "export"
    summary = job_export(_ctx(store, out))
    assert (out / "market_health.json").exists()
    assert (out / "util_spells.json").exists()
    assert "2 files" in summary and "4 markets" in summary


def test_market_health_contract(store, tmp_path):
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    doc = _load(out / "market_health.json")

    assert doc["schema_version"] == 2
    assert doc["chain_id"] == 999
    assert doc["generated_at"].endswith("Z")
    by_id = {m["market_id"]: m for m in doc["markets"]}
    assert set(by_id) == {"0xgood", "0xr1", "0xpin", "0xdust"}

    # Healthy market: not broken, sorts first, carries display + USD fields.
    good = by_id["0xgood"]
    assert good["is_broken"] is False and good["broken_reason"] is None
    assert doc["markets"][0]["market_id"] == "0xgood"  # is_broken ASC ordering
    assert good["loan_symbol"] == "USDT0" and good["collateral_symbol"] == "kHYPE"
    assert good["lltv"] == pytest.approx(0.86)
    assert good["supply_usd"] == pytest.approx(1_000_000, rel=1e-6)
    assert good["supply_apy"] > 0

    # Broken markets carry their reason; idle market has null collateral.
    assert by_id["0xr1"]["is_broken"] and by_id["0xr1"]["broken_reason"] == "rate_ratchet"
    assert by_id["0xpin"]["broken_reason"] == "pinned_util"
    assert by_id["0xdust"]["broken_reason"] == "dust"
    assert by_id["0xdust"]["collateral_symbol"] is None


def test_market_health_enrichment(store, tmp_path):
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    by_id = {m["market_id"]: m for m in _load(out / "market_health.json")["markets"]}
    good = by_id["0xgood"]

    # spread_to_best present; the healthy market is the eligible leader (0 spread).
    assert good["spread_to_best"] == pytest.approx(0.0, abs=1e-9)

    # utilization_regime keys are always emitted. Values use now()-relative
    # windows (query-time), so assert shape/range rather than exact numbers.
    reg = good["utilization_regime"]
    assert set(reg) == {
        "avg_util_7d", "avg_util_30d",
        "pct_time_gt95_7d", "pct_time_gt95_30d",
        "pct_time_gt99_7d", "pct_time_gt99_30d",
    }
    assert all(v is None or 0.0 <= v <= 1.0 for v in reg.values())

    # borrower_risk: 3 borrowers, one within 5% of liquidation; top-3 = 100%.
    br = good["borrower_risk"]
    assert br["borrowers"] == 3
    assert br["min_hf"] == pytest.approx(1.02)
    assert br["borrowers_hf_lt_105"] == 1
    assert br["top3_debt_pct"] == pytest.approx(1.0)  # only 3 borrowers -> 100%
    assert 0 < br["pct_debt_hf_lt_105"] <= 1

    # Markets without a borrower book expose borrower_risk = null.
    assert by_id["0xpin"]["borrower_risk"] is None
    # Optional fields degrade to null, never missing keys.
    assert "oracle_price" in good and "collateral_vol_7d" in good


def test_market_health_sparkline(store, tmp_path):
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    good = next(m for m in _load(out / "market_health.json")["markets"] if m["market_id"] == "0xgood")
    # 30 hourly rows within the 7d window -> 30 sparkline points, oldest first.
    assert len(good["history"]) == 30
    assert good["history"][0]["ts"] < good["history"][-1]["ts"]
    assert all(p["supply_apy"] is not None and p["u"] is not None for p in good["history"])


def test_stale_market_excluded(store, tmp_path):
    # A market whose newest row is >48h behind the store's newest is dropped.
    store.upsert(MARKET_STATE, [_state("0xold", TS0 - timedelta(days=5), 0.5, 10**12, RAT_LOW)])
    store.upsert(MARKETS, [_dim("0xold", "OLD")])
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    ids = {m["market_id"] for m in _load(out / "market_health.json")["markets"]}
    assert "0xold" not in ids


def test_util_spells_contract(store, tmp_path):
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    doc = _load(out / "util_spells.json")

    assert doc["schema_version"] == 2 and doc["chain_id"] == 999
    pin = [s for s in doc["spells"] if s["market_id"] == "0xpin"]
    # Pinned market breaks both the 0.92 and 0.95 bands, both still open.
    assert {s["threshold"] for s in pin} == {0.92, 0.95}
    assert all(s["open"] for s in pin)
    assert all(s["duration_min"] > 0 and s["peak_u"] == pytest.approx(0.9995) for s in pin)
    # Healthy market never crosses the bands.
    assert not any(s["market_id"] == "0xgood" for s in doc["spells"])


def test_rerun_overwrites_atomically(store, tmp_path):
    out = tmp_path / "export"
    job_export(_ctx(store, out))
    first = _load(out / "market_health.json")["generated_at"]

    ctx = _ctx(store, out)
    ctx.now = (TS0 + timedelta(hours=31)).timestamp()  # later generated_at
    job_export(ctx)
    second = _load(out / "market_health.json")["generated_at"]

    assert second != first
    assert not list(out.glob("*.tmp"))  # no stray temp files left behind


def test_empty_spells_frame_shapes_cleanly():
    doc = build_util_spells(pd.DataFrame(columns=["chain_id"]), datetime(2026, 7, 21, tzinfo=timezone.utc))
    assert doc["spells"] == [] and doc["chain_id"] is None and doc["schema_version"] == 2


def test_run_export_skips_when_views_absent(tmp_path):
    # Store with a priced market but no market_state -> no health/spell views.
    s = Store(tmp_path / "data")
    s.upsert(PRICES, [{
        "ts": TS0, "chain_id": 999, "token_address": LOAN,
        "price_usd": 1.0, "source": "test", "confidence": None,
    }])
    out = tmp_path / "export"
    with MnemonReader(s.data_dir) as reader:
        summary = run_export(reader, out, datetime(2026, 7, 21, tzinfo=timezone.utc))
    assert "0 files" in summary
    assert not out.exists() or not list(out.glob("*.json"))
