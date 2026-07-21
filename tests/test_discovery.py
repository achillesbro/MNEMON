"""Full-chain discovery + supply floors: the tracked set is vault allocations
+ extra_markets + every full-scan market above the supply floor, and the
positions job skips markets below its own (higher) floor."""

from __future__ import annotations

from types import SimpleNamespace

from mnemon.discovery import discover_markets
from mnemon.jobs.positions import job_positions


class FakeState:
    def __init__(self):
        self.saved = None

    def cached_tracked(self):
        return []

    def cache_tracked(self, rows):
        self.saved = rows


def _cfg(**kw):
    base = dict(extra_markets=[], vaults=[], full_scan_chains=[999], min_market_supply_usd=1000.0)
    base.update(kw)
    return SimpleNamespace(**base)


def test_full_scan_applies_floor_and_unions_vault_and_extra():
    vault = SimpleNamespace(address="0xvault", chain_id=999, label="v")
    extra = SimpleNamespace(chain_id=999, market_id="0xextra")
    morpho = SimpleNamespace(
        vault_allocations=lambda a, c: {"state": {"allocation": [{"market": {"marketId": "0xvaultmkt"}}]}},
        all_markets=lambda chain_id: [
            {"marketId": "0xbig", "state": {"supplyAssetsUsd": 50_000}},
            {"marketId": "0xsmall", "state": {"supplyAssetsUsd": 500}},      # below floor
            {"marketId": "0xvaultmkt", "state": {"supplyAssetsUsd": 100}},   # below floor but vault-held
        ],
    )
    markets, supply = discover_markets(_cfg(vaults=[vault], extra_markets=[extra]), morpho, FakeState())
    ids = {m for _, m in markets}

    assert "0xbig" in ids        # full-scan, above floor
    assert "0xsmall" not in ids  # full-scan, below floor, not held anywhere
    assert "0xvaultmkt" in ids   # below floor but a vault holds it (union wins)
    assert "0xextra" in ids      # extra_markets always tracked
    # supply map records every scanned market, even sub-floor ones.
    assert supply[(999, "0xbig")] == 50_000
    assert supply[(999, "0xsmall")] == 500


def test_no_full_scan_is_legacy_vault_only():
    vault = SimpleNamespace(address="0xvault", chain_id=999, label="v")
    morpho = SimpleNamespace(
        vault_allocations=lambda a, c: {"state": {"allocation": [{"market": {"marketId": "0xvaultmkt"}}]}},
        all_markets=lambda chain_id: (_ for _ in ()).throw(AssertionError("should not scan")),
    )
    markets, supply = discover_markets(_cfg(vaults=[vault], full_scan_chains=[]), morpho, FakeState())
    assert {m for _, m in markets} == {"0xvaultmkt"}
    assert supply == {}


def test_positions_job_skips_markets_below_floor():
    called = {}

    def fake_positions(ids, chains, max_pages):
        called["ids"] = list(ids)
        return []

    ctx = SimpleNamespace(
        now=0.0,
        cfg=SimpleNamespace(
            cadences=SimpleNamespace(positions=3600),
            positions_max_pages=50,
            positions_min_supply_usd=50_000,
        ),
        chain_ids=[999],
        market_ids=lambda c: ["0xbig", "0xsmall", "0xvault"],
        # 0xvault has no supply entry -> treated as +inf -> always pulled.
        market_supply_usd={(999, "0xbig"): 60_000, (999, "0xsmall"): 2_000},
        morpho=SimpleNamespace(positions=fake_positions),
        store=SimpleNamespace(upsert=lambda spec, rows: len(rows)),
    )
    job_positions(ctx)
    assert set(called["ids"]) == {"0xbig", "0xvault"}  # 0xsmall (below floor) skipped
