#!/bin/zsh
set -euo pipefail
# (Optional) if you use nvm:
[[ -s "$HOME/.nvm/nvm.sh" ]] && source "$HOME/.nvm/nvm.sh" && nvm use --silent >/dev/null 2>&1 || true
cd "$HOME/morpho-daily"
mkdir -p out
npm run fetch >> out/fetch.log 2>&1
echo "ok"