"""DuckDB query layer: a .duckdb file holding views over the Parquet globs.

Views are re-created after every run (cheap, and picks up new tables). Raw
tables are exposed 1:1; the derived `v_*` views (symbols, human units, metrics)
are defined once in `mnemon.views` and shared with the read API so the
persisted db and what consumers see over Parquet never drift.
"""

from __future__ import annotations

import logging

import duckdb

from mnemon.config import Config
from mnemon.schemas import ALL_TABLES
from mnemon.storage import Store
from mnemon.views import create_derived_views

log = logging.getLogger(__name__)


def register_raw_tables(con: duckdb.DuckDBPyConnection, store: Store) -> set[str]:
    """Expose each Parquet-backed table 1:1 as a view. Returns the set of
    tables that actually had data (shared with the read API)."""
    available: set[str] = set()
    for spec in ALL_TABLES.values():
        if not store.has_data(spec):
            continue
        hive = ", hive_partitioning = 1" if spec.partitioned else ""
        con.execute(
            f"CREATE OR REPLACE VIEW {spec.name} AS "
            f"SELECT * FROM read_parquet('{store.parquet_glob(spec)}'{hive})"
        )
        available.add(spec.name)
    return available


def refresh_views(cfg: Config, store: Store) -> None:
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.duckdb_path))
    try:
        available = register_raw_tables(con, store)
        create_derived_views(con, available)
        log.info("duckdb views refreshed: %s", ", ".join(sorted(available)) or "none")
    finally:
        con.close()
