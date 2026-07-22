"""Flow / concentration / oracle views over hand-built stores.

One market (0xm) with a known event tape checks the flow views' scaling,
signs, aggregation, and whale sizing; a second market (0xo) with a stepped
oracle price checks the oracle-vs-DefiLlama deviation and its depeg spells.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import duckdb
import pytest

from mnemon.duck import refresh_views
from mnemon.schemas import MARKET_FLOWS, MARKET_STATE, MARKETS, PRICES, SUPPLIER_POSITIONS
from mnemon.storage import Store

TS0 = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
LOAN_DEC = 6  # USDT0-like
COLL_DEC = 18
# oracle price scale = 10^(36 + 6 - 18) = 10^24; collateral $2 / loan $1 = 2.0
ORACLE_2_0 = str(2 * 10**24)
ORACLE_2_2 = str(22 * 10**23)


def _dim(market: str) -> dict:
    return {
        "chain_id": 999,
        "market_id": market,
        "loan_token": "0xloan",
        "loan_symbol": "USDT0",
        "loan_decimals": LOAN_DEC,
        "collateral_token": "0xcoll",
        "collateral_symbol": "C",
        "collateral_decimals": COLL_DEC,
        "oracle": None,
        "irm": None,
        "lltv": 860000000000000000,
        "creation_ts": TS0,
        "listed": True,
        "fetched_at": TS0,
    }


def _state(market: str, ts: datetime, supply_loan: float, oracle_raw: str | None) -> dict:
    supply = int(supply_loan * 10**LOAN_DEC)
    return {
        "ts": ts,
        "chain_id": 999,
        "market_id": market,
        "total_supply_assets": supply,
        "total_supply_shares": supply,
        "total_borrow_assets": supply // 2,
        "total_borrow_shares": supply // 2,
        "rate_at_target": 2_600_000_000,
        "utilization": 0.5,
        "oracle_price_raw": oracle_raw,
        "api_timestamp": ts,
        "source": "test",
    }


def _flow(ts: datetime, type_: str, log_index: int, **amounts) -> dict:
    return {
        "ts": ts,
        "chain_id": 999,
        "market_id": "0xm",
        "block_number": 1000 + log_index,
        "tx_hash": f"0xtx{log_index}",
        "log_index": log_index,
        "type": type_,
        "account": "0xuser",
        "assets": amounts.get("assets"),
        "shares": amounts.get("shares"),
        "liquidator": amounts.get("liquidator"),
        "repaid_assets": amounts.get("repaid_assets"),
        "seized_assets": amounts.get("seized_assets"),
        "bad_debt_assets": amounts.get("bad_debt_assets"),
    }


@pytest.fixture()
def con(tmp_path):
    store = Store(tmp_path / "data")
    store.upsert(MARKETS, [_dim("0xm"), _dim("0xo")])
    store.upsert(
        PRICES,
        [
            {"ts": TS0 - timedelta(days=1), "chain_id": 999, "token_address": tok,
             "price_usd": px, "source": "test", "confidence": None}
            for tok, px in [("0xloan", 1.0), ("0xcoll", 2.0)]
        ],
    )

    # 0xm: 100k loan-token supply one hour before the event tape.
    states = [_state("0xm", TS0 - timedelta(hours=1), 100_000, None)]
    # 0xo: hourly samples with a 3h oracle step 2.0 -> 2.2 (10% deviation).
    oracle_tape = [ORACLE_2_0, ORACLE_2_0, ORACLE_2_2, ORACLE_2_2, ORACLE_2_2, ORACLE_2_0]
    states += [
        _state("0xo", TS0 + timedelta(hours=i), 50_000, raw)
        for i, raw in enumerate(oracle_tape)
    ]
    store.upsert(MARKET_STATE, states)

    store.upsert(
        MARKET_FLOWS,
        [
            _flow(TS0 + timedelta(minutes=1), "Supply", 1,
                  assets=10_000 * 10**LOAN_DEC, shares=1),
            _flow(TS0 + timedelta(minutes=2), "Withdraw", 2,
                  assets=2_000 * 10**LOAN_DEC, shares=1),
            _flow(TS0 + timedelta(minutes=3), "Borrow", 3,
                  assets=5_000 * 10**LOAN_DEC, shares=1),
            _flow(TS0 + timedelta(minutes=4), "Repay", 4,
                  assets=1_000 * 10**LOAN_DEC, shares=1),
            _flow(TS0 + timedelta(minutes=5), "SupplyCollateral", 5,
                  assets=3 * 10**COLL_DEC),
            _flow(TS0 + timedelta(minutes=6), "Liquidation", 6,
                  liquidator="0xliq", repaid_assets=500 * 10**LOAN_DEC,
                  seized_assets=1 * 10**COLL_DEC, bad_debt_assets=100 * 10**LOAN_DEC),
        ],
    )

    # Lender book for 0xm: 80% / 15% / 5% split.
    store.upsert(
        SUPPLIER_POSITIONS,
        [
            {"ts": TS0, "chain_id": 999, "market_id": "0xm", "supplier": s,
             "supply_shares": a, "supply_assets": a}
            for s, a in [("0xwhale", 80_000 * 10**LOAN_DEC),
                         ("0xmid", 15_000 * 10**LOAN_DEC),
                         ("0xsmall", 5_000 * 10**LOAN_DEC)]
        ],
    )

    cfg = SimpleNamespace(duckdb_path=tmp_path / "t.duckdb")
    refresh_views(cfg, store)
    return duckdb.connect(str(cfg.duckdb_path))


def test_v_market_flows_scaling_and_signs(con):
    rows = con.execute(
        "SELECT type, loan_assets, collateral_assets, supply_flow, borrow_flow, "
        "repaid_assets, seized_assets, bad_debt_assets "
        "FROM v_market_flows WHERE market_id = '0xm' ORDER BY log_index"
    ).fetchall()
    assert len(rows) == 6
    by_type = {r[0]: r for r in rows}

    assert by_type["Supply"][1] == pytest.approx(10_000)
    assert by_type["Supply"][3] == pytest.approx(10_000)  # supply_flow +
    assert by_type["Withdraw"][3] == pytest.approx(-2_000)
    assert by_type["Borrow"][4] == pytest.approx(5_000)   # borrow_flow +
    assert by_type["Repay"][4] == pytest.approx(-1_000)

    coll = by_type["SupplyCollateral"]
    assert coll[1] is None and coll[2] == pytest.approx(3.0)
    assert coll[3] is None and coll[4] is None  # no loan-side flow

    liq = by_type["Liquidation"]
    assert liq[5] == pytest.approx(500) and liq[6] == pytest.approx(1.0)
    assert liq[3] == pytest.approx(-100)  # bad debt socialized out of supply
    assert liq[4] == pytest.approx(-500)  # repaid debt closes borrow


def test_v_market_netflow_hourly_bucket(con):
    row = con.execute(
        "SELECT supply_in, supply_out, net_supply_flow, borrow_in, borrow_out, "
        "net_borrow_flow, n_events, n_liquidations "
        "FROM v_market_netflow WHERE market_id = '0xm'"
    ).fetchall()
    assert len(row) == 1  # all events in one hour bucket
    supply_in, supply_out, net_supply, borrow_in, borrow_out, net_borrow, n, nliq = row[0]
    assert supply_in == pytest.approx(10_000)
    assert supply_out == pytest.approx(2_100)  # withdraw 2000 + bad debt 100
    assert net_supply == pytest.approx(7_900)
    assert borrow_in == pytest.approx(5_000)
    assert borrow_out == pytest.approx(1_500)  # repay 1000 + liquidation 500
    assert net_borrow == pytest.approx(3_500)
    assert n == 6 and nliq == 1


def test_v_liquidations_usd_sizing(con):
    rows = con.execute(
        "SELECT borrower, liquidator, repaid_assets, seized_assets, repaid_usd, seized_usd "
        "FROM v_liquidations"
    ).fetchall()
    assert len(rows) == 1
    borrower, liquidator, repaid, seized, repaid_usd, seized_usd = rows[0]
    assert borrower == "0xuser" and liquidator == "0xliq"
    assert repaid == pytest.approx(500) and seized == pytest.approx(1.0)
    assert repaid_usd == pytest.approx(500.0)  # loan @ $1
    assert seized_usd == pytest.approx(2.0)    # 1 C @ $2


def test_v_whale_flows_sizes_against_asof_supply(con):
    rows = con.execute(
        "SELECT type, flow, market_supply, pct_of_supply FROM v_whale_flows ORDER BY type"
    ).fetchall()
    # Supply floor = 5% of the 100k ASOF supply: 10k Supply (10%) and 5k
    # Borrow (exactly 5%) qualify; everything else is below.
    assert [r[0] for r in rows] == ["Borrow", "Supply"]
    for _type, flow, supply, pct in rows:
        assert supply == pytest.approx(100_000)
        assert pct == pytest.approx(100.0 * abs(flow) / supply)


def test_v_supplier_concentration(con):
    row = con.execute(
        "SELECT suppliers, total_supply, top1_supplier, top1_supply_pct, top3_supply_pct "
        "FROM v_supplier_concentration WHERE market_id = '0xm'"
    ).fetchone()
    suppliers, total, top1, top1_pct, top3_pct = row
    assert suppliers == 3
    assert total == pytest.approx(100_000)
    assert top1 == "0xwhale"
    assert float(top1_pct) == pytest.approx(80.0)
    assert float(top3_pct) == pytest.approx(100.0)


def test_v_oracle_price_check_deviation(con):
    rows = con.execute(
        "SELECT CAST(ts AS VARCHAR), oracle_price, ref_price, deviation "
        "FROM v_oracle_price_check WHERE market_id = '0xo' ORDER BY ts"
    ).fetchall()
    assert len(rows) == 6  # 0xm rows have no oracle price -> excluded
    for _ts, oracle, ref, dev in rows:
        assert ref == pytest.approx(2.0)  # $2 collateral / $1 loan
        assert dev == pytest.approx(oracle / 2.0 - 1)
    assert rows[0][3] == pytest.approx(0.0)
    assert rows[2][3] == pytest.approx(0.1)  # 2.2 vs 2.0


def test_v_depeg_spells_islands(con):
    spells = con.execute(
        "SELECT threshold, epoch(start_ts), duration_min, peak_abs_deviation, peak_deviation "
        "FROM v_depeg_spells WHERE market_id = '0xo' ORDER BY threshold"
    ).fetchall()
    # One contiguous 3-sample spell (hours 2..4) at both thresholds.
    assert len(spells) == 2
    for threshold, start, duration, peak_abs, peak in spells:
        assert float(threshold) in (0.02, 0.05)
        assert start == (TS0 + timedelta(hours=2)).timestamp()
        assert duration == 120
        assert peak_abs == pytest.approx(0.1)
        assert peak == pytest.approx(0.1)  # signed: oracle rich vs reference
