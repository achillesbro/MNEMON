"""Config loading. See config.yaml for field documentation."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class HttpConfig(BaseModel):
    min_interval_ms: int = 300
    timeout_s: float = 30.0
    max_retries: int = 5


class ChainConfig(BaseModel):
    chain_id: int
    name: str
    llama_slug: str
    yields_chain: str | None = None
    rpc_url: str | None = None
    morpho_blue: str | None = None


class VaultConfig(BaseModel):
    address: str
    chain_id: int
    label: str


class ExtraMarket(BaseModel):
    chain_id: int
    market_id: str


class Cadences(BaseModel):
    """Seconds between runs of each job. Floor = scheduler tick (5 min)."""

    market_state: int = 300
    prices: int = 900
    vault_allocations: int = 900
    positions: int = 3600
    markets: int = 86400
    yield_pools: int = 21600
    heal: int = 86400
    bot_events: int = 900
    vault_v2_state: int = 3600
    vault_v2_flows: int = 3600
    export: int = 900


class Config(BaseModel):
    data_dir: Path
    chains: list[ChainConfig]
    vaults: list[VaultConfig]
    # Morpho Vault V2 vaults (served by the vaultV2* API entities).
    v2_vaults: list[VaultConfig] = Field(default_factory=list)
    # Directory of HEGEMON V2 bot JSONL event files (events-YYYY-MM-DD.jsonl),
    # the bind-mounted sink on the shared host. None disables the bot_events job.
    hegemon_event_dir: Path | None = None
    # Directory the `export` job writes FE-facing JSON snapshots to. Relative
    # paths resolve against the config file; None defaults to <data_dir>/export.
    export_dir: Path | None = None
    extra_markets: list[ExtraMarket] = Field(default_factory=list)
    # Chains to discover the FULL market universe on (not just vault
    # allocations). Empty = legacy behaviour (track only what the vaults hold).
    full_scan_chains: list[int] = Field(default_factory=list)
    # Supply floor (USD) for a full-scan market to be tracked — keeps empty/dust
    # markets out of the archive. Vault allocations + extra_markets are always
    # tracked regardless of this floor.
    min_market_supply_usd: float = 1000.0
    # Supply floor (USD) below which the (heavy) positions job skips a market —
    # borrower-risk is only worth pulling for markets of meaningful size.
    # 0 = pull positions for every tracked market. Markets with unknown supply
    # (vault/extra markets outside the full scan) are always included.
    positions_min_supply_usd: float = 0.0
    cadences: Cadences = Field(default_factory=Cadences)
    http: HttpConfig = Field(default_factory=HttpConfig)
    positions_max_pages: int = 50
    # How far back the daily heal job re-pulls hourly history to fill gaps
    # left by upstream outages. Must exceed the longest outage you want to
    # recover from automatically; wider windows are idempotent but cost calls.
    heal_lookback_hours: int = 48

    def chain(self, chain_id: int) -> ChainConfig:
        for c in self.chains:
            if c.chain_id == chain_id:
                return c
        raise KeyError(f"chain {chain_id} not in config")

    @property
    def duckdb_path(self) -> Path:
        return self.data_dir / "mnemon.duckdb"

    @property
    def state_path(self) -> Path:
        return self.data_dir / "mnemon_state.json"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"


def load_config(path: Path | str = "config.yaml") -> Config:
    path = Path(path).resolve()
    raw = yaml.safe_load(path.read_text())
    cfg = Config.model_validate(raw)
    # Resolve data_dir relative to the config file so cron's cwd doesn't matter.
    if not cfg.data_dir.is_absolute():
        cfg.data_dir = (path.parent / cfg.data_dir).resolve()
    # export_dir: default under data_dir; resolve a relative override vs config.
    if cfg.export_dir is None:
        cfg.export_dir = cfg.data_dir / "export"
    elif not cfg.export_dir.is_absolute():
        cfg.export_dir = (path.parent / cfg.export_dir).resolve()
    return cfg
