"""bot_events job: tail the HEGEMON V2 bot's JSONL event sink into Parquet.

The bot appends every structured event to
`{hegemon_event_dir}/events-YYYY-MM-DD.jsonl` (UTC daily files, append-only;
see HEGEMON_V2_STRATEGY_SPEC.md §0 "Event stream & data pipeline"). This job
reads *new bytes only*, tracked by a per-file cursor in mnemon_state.json:

- cursor key `botlog:<filename>` -> {"offset": <bytes consumed>, "line": <count>}
- only complete lines (ending in \\n) are consumed; a truncated trailing line
  (bot crash mid-write, or a write racing our read) is left for the next run —
  never half-ingested.
- malformed or unknown-type COMPLETE lines are logged, skipped, and advanced
  past (retrying them forever would stall the cursor).

`scores` events fan out to bot_scores (one row per market, 60s-bucketed ts);
every other event becomes one bot_events row keyed (tick_id, seq) where seq is
the line index within its file — stable across re-runs because the cursor
also persists the line count.
"""

from __future__ import annotations

import json
import logging

from mnemon import normalize
from mnemon.jobs.context import Context
from mnemon.schemas import BOT_EVENTS, BOT_SCORES

log = logging.getLogger(__name__)

KNOWN_TYPES = {
    "tick_start",
    "tick_end",
    "tick_skip",
    "scores",
    "plan_built",
    "plan_simulated",
    "tx_sent",
    "tx_confirmed",
    "tx_reverted",
    "error",
}


def job_bot_events(ctx: Context) -> str:
    event_dir = ctx.cfg.hegemon_event_dir
    if event_dir is None or not event_dir.is_dir():
        return f"no event dir ({event_dir}), skipped"

    score_rows: list[dict] = []
    event_rows: list[dict] = []
    skipped = 0
    files = sorted(event_dir.glob("events-*.jsonl"))
    for path in files:
        cursor_key = f"botlog:{path.name}"
        cur = ctx.state.get_cursor(cursor_key, {"offset": 0, "line": 0})
        size = path.stat().st_size
        if size <= cur["offset"]:
            continue

        with path.open("rb") as f:
            f.seek(cur["offset"])
            chunk = f.read()

        # Consume complete lines only; a trailing partial line is retried later.
        end = chunk.rfind(b"\n")
        if end < 0:
            continue
        complete = chunk[: end + 1]

        line_no = cur["line"]
        for raw in complete.splitlines():
            seq = line_no
            line_no += 1
            try:
                event = json.loads(raw)
                etype = event.get("type")
                if etype not in KNOWN_TYPES:
                    raise ValueError(f"unknown event type {etype!r}")
                if etype == "scores":
                    score_rows.extend(normalize.bot_scores_rows(event, path.name))
                else:
                    event_rows.append(normalize.bot_event_row(event, seq, path.name))
            except (ValueError, KeyError, TypeError) as exc:
                skipped += 1
                log.warning("%s line %d skipped: %s", path.name, seq, exc)

        ctx.state.set_cursor(cursor_key, {"offset": cur["offset"] + len(complete), "line": line_no})

    n_scores = ctx.store.upsert(BOT_SCORES, score_rows)
    n_events = ctx.store.upsert(BOT_EVENTS, event_rows)
    extra = f", {skipped} skipped" if skipped else ""
    return f"{n_scores} score rows, {n_events} event rows from {len(files)} files{extra}"
