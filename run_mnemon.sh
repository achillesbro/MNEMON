#!/bin/sh
# Portable entrypoint for the MNEMON ingestion run (macOS + Linux).
#
# Schedulers (cron, systemd) start with a minimal PATH that usually omits
# where `uv` installed itself, so we prepend the known locations:
#   - $HOME/.local/bin   (uv standalone installer, Linux + macOS)
#   - /opt/homebrew/bin  (Homebrew on Apple Silicon)
#   - /usr/local/bin     (Homebrew on Intel, or manual installs)
#
# Output goes to stdout/stderr: systemd captures it to journald, and cron
# users should redirect it themselves, e.g.
#   */15 * * * * /path/to/mnemon/run_mnemon.sh >> /path/to/mnemon/data/logs/cron.log 2>&1
# (The app also writes its own rotating log to data/logs/mnemon.log.)
set -eu

cd "$(dirname "$0")"
mkdir -p data/logs

for d in "$HOME/.local/bin" /opt/homebrew/bin /usr/local/bin; do
    case ":$PATH:" in
        *":$d:"*) ;;                       # already present
        *) [ -d "$d" ] && PATH="$d:$PATH" ;;
    esac
done
export PATH

exec uv run python -m mnemon run
