# Deploying MNEMON on a Linux VPS (systemd)

MNEMON is a scheduled batch job, not an always-on service: a systemd timer
runs the ingestion every 15 minutes. No Docker, no web server. These steps
assume Ubuntu with user `ubuntu` and the repo at `/home/ubuntu/mnemon`;
adjust paths (and the `User`/`WorkingDirectory` in `systemd/mnemon.service`)
if yours differ.

## 1. Install uv

`uv` manages the Python version and the virtualenv. The standalone installer
downloads and runs a script from astral.sh and drops `uv` in `~/.local/bin`:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 2. Get the code and build the env

The repo is public, so an HTTPS clone needs no credentials:

```sh
git clone https://github.com/achillesbro/MNEMON.git ~/mnemon
cd ~/mnemon
~/.local/bin/uv sync
```

## 3. Seed history from your Mac (optional but recommended)

`data/` is gitignored. Without seeding, the first run backfills everything
from the APIs (~a few minutes, fully idempotent). To carry over the state
and history you already have, run this **on your Mac**:

```sh
rsync -avz --delete ~/mnemon/data/ ubuntu@<VPS_IP>:~/mnemon/data/
```

The state file (`data/mnemon_state.json`) travels with it, so the VPS won't
re-backfill.

## 4. Install the systemd timer

```sh
sudo cp ~/mnemon/systemd/mnemon.service ~/mnemon/systemd/mnemon.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mnemon.timer
```

Run once immediately to confirm the service itself works:

```sh
sudo systemctl start mnemon.service
```

## 5. Verify

```sh
systemctl list-timers mnemon.timer      # next/last fire time
journalctl -u mnemon -n 50 --no-pager   # last run's output
```

A healthy run ends with lines like `market_state: 26 rows @ ...` and
`duckdb views refreshed: ...`.

## Reading logs remotely (the "API")

journald is queryable over SSH — no extra infra:

```sh
ssh ubuntu@<VPS_IP> 'journalctl -u mnemon -n 100 --no-pager'                       # recent runs
ssh ubuntu@<VPS_IP> 'journalctl -u mnemon --since "1 hour ago" --no-pager'         # by time
ssh ubuntu@<VPS_IP> 'cd mnemon && ~/.local/bin/uv run python -m mnemon check'      # data-quality report
```

The app also writes a rotating file at `data/logs/mnemon.log`.

Optional push-style monitoring: a free https://healthchecks.io check gives a
dead-man's-switch (email/Slack if a run is ever missed) with a REST API. Ask
and it can be wired into the service via `ExecStartPost`/`OnFailure`.

## Updating later

```sh
cd ~/mnemon && git pull && ~/.local/bin/uv sync
# systemd unit files changed? re-copy them and: sudo systemctl daemon-reload
```
