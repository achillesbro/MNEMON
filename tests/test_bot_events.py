"""bot_events job + normalizers, against the recorded real JSONL sample."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from mnemon import normalize
from mnemon.jobs.bot_events import job_bot_events
from mnemon.schemas import BOT_EVENTS, BOT_SCORES
from mnemon.state import MnemonState
from mnemon.storage import Store

FIXTURES = Path(__file__).parent / "fixtures"


def _ctx(tmp_path: Path, event_dir: Path | None):
    cfg = SimpleNamespace(hegemon_event_dir=event_dir)
    return SimpleNamespace(
        cfg=cfg,
        store=Store(tmp_path / "data"),
        state=MnemonState(tmp_path / "state.json"),
    )


def _read(store: Store, spec):
    import duckdb

    con = duckdb.connect()
    glob = str(store.table_dir(spec) / "**" / "*.parquet")
    return con.execute(f"SELECT * FROM read_parquet('{glob}', hive_partitioning=1)").df()


@pytest.fixture()
def event_dir(tmp_path: Path) -> Path:
    d = tmp_path / "events"
    d.mkdir()
    (d / "events-2026-07-20.jsonl").write_text((FIXTURES / "bot_events_sample.jsonl").read_text())
    return d


def test_scores_normalization_real_sample():
    line = next(
        json.loads(ln)
        for ln in (FIXTURES / "bot_events_sample.jsonl").read_text().splitlines()
        if '"type":"scores"' in ln
    )
    rows = normalize.bot_scores_rows(line, "events-2026-07-20.jsonl")
    assert len(rows) == len(line["scores"]) >= 3
    r = rows[0]
    assert r["vault"] == "0xB851D568d123077E787860a34da286255249d983"
    assert r["tick_id"] == line["tickId"]
    assert 0 <= r["u"] <= 1 and r["apy"] > 0 and r["score"] >= 0
    assert isinstance(r["vault_assets"], int) and isinstance(r["total_assets"], int)
    assert r["ts"].second == 0  # floored to 60s bucket


def test_job_ingests_sample_and_is_idempotent(tmp_path, event_dir):
    ctx = _ctx(tmp_path, event_dir)
    summary = job_bot_events(ctx)
    assert "skipped" in summary  # the malformed + unknown-type lines

    scores = _read(ctx.store, BOT_SCORES)
    events = _read(ctx.store, BOT_EVENTS)
    assert len(scores) >= 3  # one per market in the real scores event
    # 10 well-formed lines, 1 scores (not in bot_events) => 9 event rows
    assert len(events) == 9
    assert set(events["type"]) == {
        "tick_start",
        "tick_skip",
        "tick_end",
        "plan_built",
        "plan_simulated",
        "tx_sent",
        "tx_confirmed",
    }
    confirmed = events[events["type"] == "tx_confirmed"].iloc[0]
    assert confirmed["tx_hash"].startswith("0x6ef4a473")
    assert int(confirmed["block_number"]) == 40712263
    assert json.loads(confirmed["payload"])["gas"]["gasUsed"] == "320828"

    # Second run: cursor consumed everything -> no new rows, same table sizes.
    summary2 = job_bot_events(ctx)
    assert summary2.startswith("0 score rows, 0 event rows")
    assert len(_read(ctx.store, BOT_EVENTS)) == 9


def test_partial_trailing_line_retried_not_half_ingested(tmp_path):
    d = tmp_path / "events"
    d.mkdir()
    f = d / "events-2026-07-20.jsonl"
    full = '{"ts":"2026-07-20T10:00:00.000Z","chainId":999,"type":"tick_start","tickId":"t1"}\n'
    partial = '{"ts":"2026-07-20T10:00:01.000Z","chainId":999,"type":"tick_end","tickId":"t1"'
    f.write_text(full + partial)

    ctx = _ctx(tmp_path, d)
    job_bot_events(ctx)
    assert len(_read(ctx.store, BOT_EVENTS)) == 1  # partial line not ingested

    # The write completes -> next run picks up exactly the completed line.
    with f.open("a") as fh:
        fh.write(',"durationMs":5}\n')
    job_bot_events(ctx)
    events = _read(ctx.store, BOT_EVENTS)
    assert len(events) == 2
    assert json.loads(events[events["type"] == "tick_end"].iloc[0]["payload"])["durationMs"] == 5


def test_missing_dir_is_a_noop(tmp_path):
    ctx = _ctx(tmp_path, tmp_path / "does-not-exist")
    assert "skipped" in job_bot_events(ctx)


def test_vault_v2_flow_rows_real_fixture():
    payload = json.loads((FIXTURES / "vault_v2_transactions.json").read_text())
    rows = normalize.vault_v2_flow_rows(payload["items"])
    assert len(rows) == 7
    known = next(r for r in rows if r["tx_hash"].startswith("0xcf80a842"))
    assert known["type"] == "Deposit"
    assert known["assets"] == 10_000_000
    assert known["log_index"] == 11 and known["block_number"] == 40956695
    assert known["on_behalf"] and known["receiver"] is None  # deposits have no receiver
    wd = next(r for r in rows if r["type"] == "Withdraw")
    assert wd["receiver"] is not None


def test_vault_v2_state_row_real_fixture():
    payload = json.loads((FIXTURES / "vault_v2_state.json").read_text())
    row = normalize.vault_v2_state_row(payload, normalize._dt(1784541110))
    assert row["chain_id"] == 999
    assert row["total_assets"] > 0 and row["total_supply"] > 10**18
    assert row["share_price"] > 0.9
