"""Small JSON state file: last success per job, backfill flags, and a cache of
the last successfully discovered market set (so a transient API failure during
discovery doesn't blank out tracking for that run)."""

from __future__ import annotations

import json
import time
from pathlib import Path


class MnemonState:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict = {"jobs": {}, "backfilled": {}, "tracked": {}}
        if path.exists():
            self._data.update(json.loads(path.read_text()))

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._data, indent=1, sort_keys=True))
        tmp.replace(self._path)

    # --- job scheduling ---------------------------------------------------

    def last_success(self, job: str) -> float:
        return self._data["jobs"].get(job, 0.0)

    def mark_success(self, job: str, ts: float | None = None) -> None:
        self._data["jobs"][job] = ts if ts is not None else time.time()

    def is_due(self, job: str, cadence_s: int, now: float) -> bool:
        # 60s of slack so a cron tick that fires slightly early still runs.
        return now - self.last_success(job) >= cadence_s - 60

    # --- backfill flags -----------------------------------------------------

    def is_backfilled(self, key: str) -> bool:
        return bool(self._data["backfilled"].get(key))

    def mark_backfilled(self, key: str) -> None:
        self._data["backfilled"][key] = int(time.time())

    def clear_backfills(self) -> None:
        self._data["backfilled"] = {}

    # --- tracked-market cache ----------------------------------------------

    def cache_tracked(self, markets: list[list]) -> None:
        self._data["tracked"]["markets"] = markets

    def cached_tracked(self) -> list[list]:
        return self._data["tracked"].get("markets", [])
