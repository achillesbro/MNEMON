"""Idempotent Parquet storage.

Layout: data/<table>/date=YYYY-MM-DD/part-0.parquet (hive-style, one file per
day). Upserts read the affected day, merge on the table's key columns (new
rows win), and atomically replace the file — so re-runs heal gaps and never
duplicate. Daily partitions stay small (thousands of rows), which keeps this
read-merge-write approach cheap and avoids any database dependency.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mnemon.schemas import TableSpec

log = logging.getLogger(__name__)


def _coerce(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """Make column types uniform before merging: parquet round-trips decimals
    as decimal.Decimal while fresh rows carry Python ints; normalize both to
    int (exact) so drop_duplicates and Arrow conversion behave."""
    out = pd.DataFrame({name: df.get(name) for name in schema.names})
    for field in schema:
        col = out[field.name]
        if pa.types.is_decimal(field.type):
            # Arrow can't convert numpy int64 columns to decimal128 directly;
            # go through Python Decimal (exact for arbitrarily large ints).
            out[field.name] = col.astype(object).map(
                lambda v: v if isinstance(v, Decimal) else Decimal(int(v)), na_action="ignore"
            )
        elif pa.types.is_timestamp(field.type):
            out[field.name] = pd.to_datetime(col, utc=True)
    return out


class Store:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    def table_dir(self, spec: TableSpec) -> Path:
        return self.data_dir / spec.name

    def parquet_glob(self, spec: TableSpec) -> str:
        if spec.partitioned:
            return str(self.table_dir(spec) / "*" / "*.parquet")
        return str(self.table_dir(spec) / "*.parquet")

    def has_data(self, spec: TableSpec) -> bool:
        d = self.table_dir(spec)
        return d.exists() and any(d.rglob("*.parquet"))

    def upsert(self, spec: TableSpec, rows: list[dict]) -> int:
        """Insert-or-replace rows keyed on spec.keys. Returns rows written."""
        self._write(spec, rows, keep="last")
        return len(rows)

    def insert_missing(self, spec: TableSpec, rows: list[dict]) -> int:
        """Insert only rows whose key is absent; existing rows always win.
        Used by healing: backfilled hourly rows must never clobber live rows
        (live rows carry oracle_price_raw, which history rows lack).
        Returns the number of rows actually added."""
        return self._write(spec, rows, keep="first")

    def _write(self, spec: TableSpec, rows: list[dict], keep: str) -> int:
        if not rows:
            return 0
        df = _coerce(pd.DataFrame(rows), spec.schema)
        if df["ts" if "ts" in spec.schema.names else spec.keys[0]].isna().any():
            raise ValueError(f"{spec.name}: null in ts/key column")

        if not spec.partitioned:
            return self._merge_file(spec, self.table_dir(spec) / "current.parquet", df, keep)

        # Split incoming rows by UTC day and merge each day file separately.
        added = 0
        for day, day_df in df.groupby(df["ts"].dt.strftime("%Y-%m-%d")):
            path = self.table_dir(spec) / f"date={day}" / "part-0.parquet"
            added += self._merge_file(spec, path, day_df, keep)
        return added

    def _merge_file(self, spec: TableSpec, path: Path, new_df: pd.DataFrame, keep: str) -> int:
        """Merge new rows into one day file. `keep`: 'last' = new rows replace
        existing keys (upsert), 'first' = existing keys win (insert-missing).
        Returns the net number of rows added to the file."""
        if path.exists():
            existing = _coerce(pq.read_table(path).to_pandas(), spec.schema)
            merged = pd.concat([existing, new_df], ignore_index=True)
        else:
            existing = None
            merged = new_df
        merged = merged.drop_duplicates(subset=spec.keys, keep=keep).sort_values(spec.keys)

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(merged, schema=spec.schema, preserve_index=False)
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp)
        tmp.replace(path)  # atomic on POSIX: readers never see a partial file
        return len(merged) - (len(existing) if existing is not None else 0)


def floor_ts(unix_ts: float, bucket_s: int) -> datetime:
    """Floor a unix timestamp to its cadence bucket, as tz-aware UTC."""
    return datetime.fromtimestamp(int(unix_ts) // bucket_s * bucket_s, tz=timezone.utc)
