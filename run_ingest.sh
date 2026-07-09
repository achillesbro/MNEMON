#!/bin/zsh
# Cron entrypoint. Add to crontab with:
#   */15 * * * * /path/to/morpho-daily/run_ingest.sh
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data/logs
# uv lives in /opt/homebrew/bin, which cron does not have on PATH.
export PATH="/opt/homebrew/bin:$PATH"
exec uv run python -m ingest run >> data/logs/cron.log 2>&1
