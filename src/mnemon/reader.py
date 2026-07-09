"""Read-only access to a MNEMON data store, for use from any other project.

Point it at a MNEMON ``data/`` directory and get pandas DataFrames back. It
reads the Parquet files through its own private in-memory DuckDB connection, so
it never opens (or locks) the ingestion's ``mnemon.duckdb`` — safe to run
concurrently with the 15-minute cron, on the same host or on a synced copy.

Install into another project as a git dependency::

    uv add "mnemon @ git+https://github.com/achillesbro/MNEMON.git"

Then::

    from mnemon.reader import MnemonReader

    r = MnemonReader("/home/ubuntu/mnemon/data")   # or set $MNEMON_DATA
    r.tables()                       # what's available
    r.market_state_latest()          # newest state row per market
    r.liquidity_risk()               # per-market utilization risk profile
    r.vault_snapshot()               # current allocations, weights, cap usage
    r.sql("SELECT ... FROM v_prices")  # arbitrary SQL over any table/view

IMPORTANT — cadence: this is a 15-min analytics store, not a live feed. Use it
for backtesting, market/position scoring, and context. Do NOT use it as the
trigger for on-chain actions (liquidations, reallocations); validate those
against live chain state at execution time.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from mnemon.schemas import ALL_TABLES
from mnemon.storage import Store
from mnemon.views import DERIVED_VIEWS, create_derived_views


class MnemonReader:
    """Query a MNEMON store read-only. Cheap to construct; reuse one instance."""

    def __init__(self, data_dir: str | Path | None = None) -> None:
        data_dir = data_dir or os.environ.get("MNEMON_DATA")
        if not data_dir:
            raise ValueError(
                "No data directory: pass data_dir=... or set the MNEMON_DATA env var "
                "to a MNEMON 'data/' folder."
            )
        self.data_dir = Path(data_dir).expanduser().resolve()
        if not self.data_dir.exists():
            raise FileNotFoundError(f"MNEMON data dir not found: {self.data_dir}")

        self._store = Store(self.data_dir)
        self._con = duckdb.connect(":memory:")
        self.available = self._register()

    def _register(self) -> set[str]:
        available: set[str] = set()
        for spec in ALL_TABLES.values():
            if not self._store.has_data(spec):
                continue
            hive = ", hive_partitioning = 1" if spec.partitioned else ""
            self._con.execute(
                f"CREATE OR REPLACE VIEW {spec.name} AS "
                f"SELECT * FROM read_parquet('{self._store.parquet_glob(spec)}'{hive})"
            )
            available.add(spec.name)
        create_derived_views(self._con, available)
        return available

    # --- generic access ----------------------------------------------------

    def sql(self, query: str, params: list | None = None) -> pd.DataFrame:
        """Run arbitrary SQL against the store; returns a DataFrame. All raw
        tables and `v_*` views are in scope. Use `?` placeholders + `params`
        for user-supplied values."""
        return self._con.execute(query, params or []).df()

    def tables(self) -> list[str]:
        """Raw tables and derived views currently queryable."""
        views = {v.name for v in DERIVED_VIEWS if v.deps <= self.available}
        return sorted(self.available | views)

    def refresh(self) -> None:
        """Re-scan the data dir (call after new tables first appear on disk)."""
        self.available = self._register()

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> MnemonReader:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- typed convenience readers -----------------------------------------

    def market_state(
        self, loan: str | None = None, collateral: str | None = None, since: str | None = None
    ) -> pd.DataFrame:
        """market state timeseries (human units, utilization, apy_at_target,
        oracle_price), optionally filtered by loan/collateral symbol and since."""
        where, params = self._filters(loan_symbol=loan, collateral_symbol=collateral, since=since)
        return self.sql(f"SELECT * FROM v_market_state {where} ORDER BY market_id, ts", params)

    def market_state_latest(self) -> pd.DataFrame:
        """Newest state row per market."""
        return self.sql(
            "SELECT * FROM v_market_state "
            "QUALIFY ts = MAX(ts) OVER (PARTITION BY chain_id, market_id)"
        )

    def liquidity_risk(self) -> pd.DataFrame:
        """Per-market utilization-risk profile + current util/APY-at-target."""
        return self.sql("SELECT * FROM v_liquidity_risk ORDER BY pct_time_gt99 DESC")

    def vault_snapshot(self, vault: str | None = None) -> pd.DataFrame:
        """Current allocation per vault (weight %, cap used %)."""
        where, params = self._filters(vault=(vault.lower() if vault else None))
        return self.sql(f"SELECT * FROM v_vault_snapshot {where} ORDER BY vault, weight_pct DESC", params)

    def vault_allocations(self, vault: str | None = None, since: str | None = None) -> pd.DataFrame:
        """Allocation timeseries per vault (for drift analysis)."""
        where, params = self._filters(vault=(vault.lower() if vault else None), since=since)
        return self.sql(f"SELECT * FROM v_vault_allocations {where} ORDER BY vault, market_id, ts", params)

    def prices(self, symbol: str | None = None, since: str | None = None) -> pd.DataFrame:
        """USD price timeseries with token symbols attached."""
        where, params = self._filters(symbol=symbol, since=since)
        return self.sql(f"SELECT * FROM v_prices {where} ORDER BY token_address, ts", params)

    def positions(self, market_id: str | None = None) -> pd.DataFrame:
        """Daily borrower-position snapshots (accumulating forward)."""
        where, params = self._filters(market_id=market_id)
        return self.sql(f"SELECT * FROM positions {where} ORDER BY ts, borrow_shares DESC", params)

    def yield_pools(self, since: str | None = None) -> pd.DataFrame:
        """Competing venue yields (DefiLlama)."""
        where, params = self._filters(since=since)
        return self.sql(f"SELECT * FROM yield_pools {where} ORDER BY ts, tvl_usd DESC", params)

    @staticmethod
    def _filters(*, since: str | None = None, **eq: str | None) -> tuple[str, list]:
        """Build a parameterized WHERE clause from equality filters (+ ts>=since)."""
        clauses, params = [], []
        for col, val in eq.items():
            if val is not None:
                clauses.append(f"{col} = ?")
                params.append(val)
        if since is not None:
            clauses.append("ts >= ?")
            params.append(since)
        return (("WHERE " + " AND ".join(clauses)) if clauses else "", params)
