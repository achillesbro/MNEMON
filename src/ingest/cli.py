"""CLI entrypoint: python -m ingest <command>.

  run             run whichever jobs are due (cron calls this every 15 min)
  backfill        force the backfill pass (--force re-pulls everything)
  check           data-quality report (gaps, null rates, last successes)
  discover        print the currently tracked market set
  migrate-legacy  convert old out/snapshot-*.json files into legacy_snapshots
  init-db         (re)create the DuckDB views without ingesting
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ingest.config import load_config
from ingest.logging_setup import setup_logging

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__)
    parser.add_argument("--config", default="config.yaml", help="path to config.yaml")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run due jobs")
    p_run.add_argument("--only", help="comma-separated job names, run regardless of cadence")

    p_backfill = sub.add_parser("backfill", help="backfill history for tracked entities")
    p_backfill.add_argument("--force", action="store_true", help="clear backfill flags and re-pull")

    sub.add_parser("check", help="data-quality report")
    sub.add_parser("discover", help="print tracked markets")
    sub.add_parser("init-db", help="refresh DuckDB views")

    p_migrate = sub.add_parser("migrate-legacy", help="import old out/*.json snapshots")
    p_migrate.add_argument("dir", type=Path, help="directory containing snapshot-YYYY-MM-DD.json files")

    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    setup_logging(cfg.log_dir, args.verbose)

    if args.command == "check":
        from ingest.check import run_check

        print(run_check(cfg))
        return 0

    if args.command == "init-db":
        from ingest.duck import refresh_views
        from ingest.storage import Store

        refresh_views(cfg, Store(cfg.data_dir))
        print(f"views refreshed in {cfg.duckdb_path}")
        return 0

    if args.command == "migrate-legacy":
        from ingest.duck import refresh_views
        from ingest.migrate_legacy import migrate
        from ingest.storage import Store

        store = Store(cfg.data_dir)
        print(migrate(args.dir.resolve(), store))
        refresh_views(cfg, store)
        return 0

    # Commands below need live API clients.
    from ingest.duck import refresh_views
    from ingest.jobs import run_due_jobs
    from ingest.jobs.context import build_context

    ctx = build_context(cfg)
    try:
        if args.command == "discover":
            for chain_id, market_id in ctx.tracked_markets:
                print(f"{chain_id} {market_id}")
            print(f"({len(ctx.tracked_markets)} markets)")
            return 0

        if args.command == "backfill":
            if args.force:
                ctx.state.clear_backfills()
            # The markets job refreshes the dimension and backfills anything
            # without a flag — exactly what an explicit backfill should do.
            results = run_due_jobs(ctx, only=["markets"])
        else:  # run
            only = args.only.split(",") if args.only else None
            results = run_due_jobs(ctx, only=only)

        for name, summary in results.items():
            print(f"{name}: {summary}")
        if not results:
            print("nothing due")
        refresh_views(cfg, ctx.store)
        return 1 if any("FAILED" in s for s in results.values()) else 0
    finally:
        ctx.state.save()
        ctx.close()
