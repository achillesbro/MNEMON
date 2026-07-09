"""Shared per-run context: config, clients, storage, state, and the discovered
market set (computed once per run, reused by every job)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from ingest.config import Config
from ingest.defillama import LlamaClient
from ingest.discovery import discover_markets
from ingest.http import HttpClient
from ingest.morpho_api import MorphoClient
from ingest.state import IngestState
from ingest.storage import Store


@dataclass
class Context:
    cfg: Config
    http: HttpClient
    morpho: MorphoClient
    llama: LlamaClient
    store: Store
    state: IngestState
    now: float = field(default_factory=time.time)
    _tracked: list[tuple[int, str]] | None = None

    @property
    def tracked_markets(self) -> list[tuple[int, str]]:
        if self._tracked is None:
            self._tracked = discover_markets(self.cfg, self.morpho, self.state)
        return self._tracked

    def market_ids(self, chain_id: int) -> list[str]:
        return [m for c, m in self.tracked_markets if c == chain_id]

    @property
    def chain_ids(self) -> list[int]:
        return sorted({c for c, _ in self.tracked_markets})

    def close(self) -> None:
        self.http.close()


def build_context(cfg: Config) -> Context:
    http = HttpClient(cfg.http)
    return Context(
        cfg=cfg,
        http=http,
        morpho=MorphoClient(http),
        llama=LlamaClient(http),
        store=Store(cfg.data_dir),
        state=IngestState(cfg.state_path),
    )
