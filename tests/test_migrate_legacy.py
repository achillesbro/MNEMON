"""Legacy snapshot migration tests against the old TS fetcher's output shape."""

import json

from ingest.migrate_legacy import migrate, snapshot_rows
from ingest.schemas import LEGACY_SNAPSHOTS
from ingest.storage import Store

OLD_SNAPSHOT = {
    "timestamp": "2025-11-14T12:00:00.000Z",
    "chainId": 999,
    "vault": "0x4DC97f968B0Ba4Edd32D1b9B8Aaf54776c134d42",
    "vaults": {"usdt0": "0x4DC9...", "whype": "0x889d..."},
    "markets": [
        {
            "symbol": "USDT0–kHYPE",
            "marketId": "0xc5526286d537c890fdd879d17d80c4a22dc7196c1e1fff0dd6c853692a759c62",
            "loan": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
            "collateral": "0xfD739d4e423301CE9385c1fb8850539D657C296D",
            "utilisation": 0.65,
            "borrowAPY": 0.12,
            "supplyAPY": 0.08,
            "availableLiquidity": 1000000.5,
            "vaultAllocation": 500000.25,
        }
    ],
}


def test_snapshot_rows_maps_old_fields():
    rows = snapshot_rows(OLD_SNAPSHOT, "snapshot-2025-11-14.json")
    assert len(rows) == 1
    r = rows[0]
    assert r["ts"].isoformat().startswith("2025-11-14T12:00")
    assert r["utilization"] == 0.65  # old spelling "utilisation" handled
    assert r["loan_token"] == "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb"
    assert r["vault_allocation"] == 500000.25


def test_migrate_directory(tmp_path):
    snap_dir = tmp_path / "out"
    snap_dir.mkdir()
    (snap_dir / "snapshot-2025-11-14.json").write_text(json.dumps(OLD_SNAPSHOT))
    (snap_dir / "latest.json").write_text(json.dumps(OLD_SNAPSHOT))  # must be ignored
    (snap_dir / "snapshot-2025-11-15.json").write_text("{broken json")  # must not abort

    store = Store(tmp_path / "data")
    result = migrate(snap_dir, store)
    assert "migrated 2 snapshots, 1 rows" in result
    assert store.has_data(LEGACY_SNAPSHOTS)


def test_migrate_empty_dir(tmp_path):
    store = Store(tmp_path / "data")
    assert "no snapshot" in migrate(tmp_path, store)
