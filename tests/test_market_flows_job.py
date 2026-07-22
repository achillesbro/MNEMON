"""market_flows job: first-run lookback is bounded (never t=0), the cursor
advances to the last event seen, and re-fetch overlap dedupes on the event key."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import duckdb

from mnemon.jobs.market_flows import OVERLAP_S, job_market_flows
from mnemon.state import MnemonState
from mnemon.storage import Store

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "market_transactions.json").read_text()
)

NOW = 1_784_800_000  # 2026-07-23 ~00:26 UTC, after every fixture event
BACKFILL_HOURS = 168


class StubMorpho:
    def __init__(self, items: list[dict]) -> None:
        self.items = items
        self.calls: list[int] = []  # since_ts of each call

    def market_transactions(self, chain_id: int, since_ts: int, max_pages: int = 400) -> list[dict]:
        self.calls.append(since_ts)
        return [it for it in self.items if int(it["timestamp"]) >= since_ts]


def _ctx(tmp_path, morpho: StubMorpho):
    cfg = SimpleNamespace(
        chains=[SimpleNamespace(chain_id=999)],
        market_flows_backfill_hours=BACKFILL_HOURS,
    )
    return SimpleNamespace(
        cfg=cfg,
        morpho=morpho,
        store=Store(tmp_path / "data"),
        state=MnemonState(tmp_path / "state.json"),
        now=float(NOW),
    )


def test_first_run_backfills_bounded_window_not_t0(tmp_path):
    morpho = StubMorpho(FIXTURE["items"])
    ctx = _ctx(tmp_path, morpho)
    summary = job_market_flows(ctx)

    assert morpho.calls == [NOW - BACKFILL_HOURS * 3600]  # bounded, not 0
    n_expected = len(morpho.market_transactions(999, NOW - BACKFILL_HOURS * 3600))
    assert f"{n_expected} events" in summary

    max_ts = max(int(it["timestamp"]) for it in FIXTURE["items"])
    assert ctx.state.get_cursor("market_flows:999") == max_ts


def test_second_run_resumes_from_cursor_with_overlap_and_dedupes(tmp_path):
    morpho = StubMorpho(FIXTURE["items"])
    ctx = _ctx(tmp_path, morpho)
    job_market_flows(ctx)
    job_market_flows(ctx)  # overlap window re-fetches the newest events

    max_ts = max(int(it["timestamp"]) for it in FIXTURE["items"])
    assert morpho.calls[1] == max_ts - OVERLAP_S
    assert ctx.state.get_cursor("market_flows:999") == max_ts

    con = duckdb.connect()
    n, distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT tx_hash || '/' || log_index) "
        f"FROM read_parquet('{tmp_path}/data/market_flows/*/*.parquet')"
    ).fetchone()
    con.close()
    assert n == distinct  # event key dedupes the overlap re-fetch


def test_no_new_events_leaves_cursor_untouched(tmp_path):
    morpho = StubMorpho(FIXTURE["items"])
    ctx = _ctx(tmp_path, morpho)
    job_market_flows(ctx)
    cursor = ctx.state.get_cursor("market_flows:999")

    morpho.items = []  # API returns nothing new
    summary = job_market_flows(ctx)
    assert "0 events" in summary
    assert ctx.state.get_cursor("market_flows:999") == cursor
