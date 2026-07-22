"""State-file scheduling and config loading."""

from pathlib import Path

from mnemon.config import load_config
from mnemon.state import MnemonState


def test_job_due_logic(tmp_path):
    state = MnemonState(tmp_path / "state.json")
    assert state.is_due("market_state", 900, now=1000)  # never ran -> due
    state.mark_success("market_state", 1000)
    assert not state.is_due("market_state", 900, now=1500)
    # 60s slack: an early cron tick at t+850 still counts as due
    assert state.is_due("market_state", 900, now=1850)


def test_state_round_trip(tmp_path):
    path = tmp_path / "state.json"
    state = MnemonState(path)
    state.mark_success("prices", 123.0)
    state.mark_backfilled("market:999:0xabc")
    state.cache_tracked([[999, "0xabc"]])
    state.save()

    reloaded = MnemonState(path)
    assert reloaded.last_success("prices") == 123.0
    assert reloaded.is_backfilled("market:999:0xabc")
    assert reloaded.cached_tracked() == [[999, "0xabc"]]


def test_load_repo_config():
    cfg = load_config(Path(__file__).parent.parent / "config.yaml")
    assert cfg.chain(999).llama_slug == "hyperliquid"
    assert len(cfg.vaults) == 2
    assert cfg.cadences.market_state == 300
    assert cfg.cadences.positions == 300  # bumped to the scheduler-tick floor 2026-07-22
    assert cfg.cadences.supplier_positions == 3600
    assert cfg.cadences.market_flows == 900
    assert cfg.market_flows_backfill_hours == 12000  # one-time: full-history first run
    assert cfg.data_dir.is_absolute()
