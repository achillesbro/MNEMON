"""Logging: console + rotating file under data/logs/."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(log_dir: Path, verbose: bool = False) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = RotatingFileHandler(log_dir / "ingest.log", maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.handlers = [file_handler, console]
    # httpx logs a line per request at INFO; keep the file readable.
    logging.getLogger("httpx").setLevel(logging.WARNING)
