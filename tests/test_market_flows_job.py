"""market_flows job: first-run lookback is bounded (never t=0), the cursor
advances to the last event seen, re-fetch overlap dedupes on the event key,
and deep history is walked in timestamp-windowed batches (the API rejects
skip > 10,000, so pagination can never just skip deeper)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import duckdb

from mnemon.jobs.market_flows import MAX_BATCHES, OVERLAP_S, job_market_flows
from mnemon.state import MnemonState
from mnemon.storage import Store

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "market_transactions.json").read_text()
)

NOW = 1_784_800_000  # 2026-07-23 ~00:26 UTC, after every fixture event
BACKFILL_HOURS = 168


class StubMorpho:
    """Serves fixture items ASC from since_ts, at most `per_call` per call —
    mimicking the skip-capped window the real fetcher returns."""

    def __init__(self, items: list[dict], per_call: int | None = None) -> None:
        self.items = sorted(items, key=lambda it: int(it["timestamp"]))
        self.per_call = per_call
        self.calls: list[int] = []  # since_ts of each call

    def market_transactions(
        self, chain_id: int, since_ts: int, max_pages: int = 100
    ) -> tuple[list[dict], bool]:
        self.calls.append(since_ts)
        matching = [it for it in self.items if int(it["timestamp"]) >= since_ts]
        if self.per_call is None or len(matching) <= self.per_call:
            return matching, False
        return matching[: self.per_call], True


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


def _stored_rows(tmp_path) -> int:
    con = duckdb.connect()
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tmp_path}/data/market_flows/*/*.parquet')"
    ).fetchone()[0]
    con.close()
    return n


def test_first_run_backfills_bounded_window_not_t0(tmp_path):
    morpho = StubMorpho(FIXTURE["items"])
    ctx = _ctx(tmp_path, morpho)
    summary = job_market_flows(ctx)

    assert morpho.calls == [NOW - BACKFILL_HOURS * 3600]  # bounded, not 0
    n_expected = len(morpho.market_transactions(999, NOW - BACKFILL_HOURS * 3600)[0])
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


def test_truncated_batches_walk_history_by_timestamp(tmp_path):
    # 3 events per call: one run must issue several since_ts-advancing calls
    # (skip resets each time) and commit every batch — the skip-cap regression.
    morpho = StubMorpho(FIXTURE["items"], per_call=3)
    ctx = _ctx(tmp_path, morpho)
    summary = job_market_flows(ctx)

    # every follow-up call starts at the previous batch's newest timestamp,
    # strictly advancing (skip resets to 0 at each seam)
    assert len(morpho.calls) > 1
    assert morpho.calls == sorted(set(morpho.calls))

    # all 9 fixture events land despite the 3-per-call window (seams overlap
    # on identical timestamps; the event key dedupes them)
    assert _stored_rows(tmp_path) == len(FIXTURE["items"])
    assert ctx.state.get_cursor("market_flows:999") == max(
        int(it["timestamp"]) for it in FIXTURE["items"]
    )
    assert "events" in summary


def test_batch_cap_stops_run_but_keeps_progress(tmp_path):
    # 2 events per call and more events than the cap can cover: the run must
    # stop at MAX_BATCHES with the cursor parked mid-history — every batch
    # committed — ready for the next run to resume.
    morpho = StubMorpho(FIXTURE["items"], per_call=2)
    ctx = _ctx(tmp_path, morpho)
    job_market_flows(ctx)

    assert len(morpho.calls) == MAX_BATCHES
    max_ts = max(int(it["timestamp"]) for it in FIXTURE["items"])
    assert ctx.state.get_cursor("market_flows:999") < max_ts
    assert _stored_rows(tmp_path) >= MAX_BATCHES  # each batch was committed

    # the next run resumes from the parked cursor and finishes the walk
    morpho.per_call = None
    job_market_flows(ctx)
    assert _stored_rows(tmp_path) == len(FIXTURE["items"])
    assert ctx.state.get_cursor("market_flows:999") == max_ts


def test_no_new_events_leaves_cursor_untouched(tmp_path):
    morpho = StubMorpho(FIXTURE["items"])
    ctx = _ctx(tmp_path, morpho)
    job_market_flows(ctx)
    cursor = ctx.state.get_cursor("market_flows:999")

    morpho.items = []  # API returns nothing new
    summary = job_market_flows(ctx)
    assert "0 events" in summary
    assert ctx.state.get_cursor("market_flows:999") == cursor
