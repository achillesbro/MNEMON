# Plan: surface MNEMON data on myrmidons-strategies.com (tools section)

Decided 2026-07-21 with the operator. Execution is split into two sessions
(one per repo). This doc is the single source of truth — execution sessions
should read it plus each repo's CLAUDE.md and not re-litigate decisions.

## Decisions (locked)

- **Scope v1**: Market health monitor (primary) + utilization spells (as a
  per-market drill-down, not its own tab). HEGEMON benchmark and bot tick
  archive are explicitly deferred.
- **Data path**: static JSON snapshots. New MNEMON `export` job writes JSON
  files; the VPS's existing Caddy serves them on a dedicated subdomain
  `data.myrmidons-strategies.com`; the FE proxies through a Next.js API route
  with caching. No new runtime process anywhere.
- **DNS**: `data.myrmidons-strategies.com` is a new A record → `51.210.107.138`,
  added in the Vercel DNS panel (Vercel hosts this domain's DNS). NOTE: without
  an explicit record, `data.*` resolves to Vercel's catch-all IPs — the A
  record must be added to override it (this is exactly how `logs.*` works).
- **UI**: one new tool in the tools pane registry
  (`components/tools/fileGroups.ts`), id `mnemon`, title `MNEMON`. The pane
  shows a compact summary + link; the real UI is a dedicated route
  `/tools/mnemon` (tabbed page, MARKETS tab in v1).
- Freshness is 15 min by design (MNEMON is an archive, not a live feed).
  Label the data age in the UI (`generated_at`).

---

## Phase 1 — MNEMON repo (`github.com/achillesbro/MNEMON`)

### 1. New job `export` (`src/mnemon/jobs/export.py`)

Standard job machinery (see CLAUDE.md "Core machinery"): `def job_export(ctx)
-> str`, registered LAST in the `JOBS` dict (it reads views over tables the
other jobs write), cadence `export: 900` (15 min) in `Cadences` (`config.py`)
and `config.yaml`.

The job opens an in-memory DuckDB over the store (reuse `reader.py` /
`create_derived_views` — same path the `check` command uses), runs the
queries below, and writes JSON files to an export dir (new config key
`export_dir`, default `<data_dir>/export`). Writes are atomic tmp+rename in
the same dir (mirror `storage.py`). Return string = e.g.
`"export: 2 files, N markets"`.

JSON conventions: every file has `schema_version` (start at 1),
`generated_at` (UTC ISO-8601), `chain_id`. Timestamps as ISO-8601 strings.
Floats as numbers; nulls allowed. Keep files small (< ~200 KB) — they're
fetched whole by the FE.

#### `market_health.json`

Latest row per market from `v_market_health`, joined to `v_market_apy` (same
latest ts) and the `markets` dimension table, **only markets with data in the
trailing 48h** (drop delisted/dead series). Include broken markets — they're
the interesting part. Sort: `is_broken` ASC, then `supply_usd` DESC.

```json
{
  "schema_version": 1,
  "generated_at": "2026-07-21T14:30:00Z",
  "chain_id": 999,
  "markets": [
    {
      "market_id": "0x…",
      "loan_symbol": "USDT0",
      "collateral_symbol": "kHYPE",        // null for idle markets
      "lltv": 0.86,                         // lltv / 1e18
      "ts": "2026-07-21T14:25:00Z",        // snapshot ts of this row
      "utilization": 0.91,
      "supply_apy": 0.083,                  // v_market_apy.supply_apy
      "borrow_apy": 0.096,
      "apy_at_target": 0.062,               // v_market_health.apy_at_target
      "supply_usd": 1234567.0,              // null when unpriced
      "available_usd": 98765.0,
      "is_broken": false,
      "broken_reason": null,                // "rate_ratchet" | "pinned_util" | "dust" | null
      "history": [                          // 7d hourly sparkline, oldest first
        { "ts": "2026-07-14T15:00:00Z", "supply_apy": 0.071, "u": 0.88 }
      ]
    }
  ]
}
```

History: from `v_market_apy` where `EXTRACT(minute FROM ts) = 0` (hourly
sampling — the pattern `v_token_vol` already uses), trailing 7 days.

#### `util_spells.json`

From `v_util_spells`, trailing 30 days (`end_ts >= now - 30d`), both
thresholds. Mark a spell `open` when its `end_ts` is within 2h of that
market's latest `market_state` ts.

```json
{
  "schema_version": 1,
  "generated_at": "…",
  "chain_id": 999,
  "spells": [
    {
      "market_id": "0x…",
      "threshold": 0.95,
      "start_ts": "…",
      "end_ts": "…",
      "duration_min": 1440,
      "peak_u": 0.999,
      "open": true
    }
  ]
}
```

(`min_available_liquidity` is raw units — omit it in v1 rather than ship a
misleading number; the FE drill-down keys spells by `market_id` from
`market_health.json` for symbols.)

### 2. Tests

Fixture style per repo convention: build a `Store` in `tmp_path` with a few
markets (healthy / rate_ratchet / dust, priced + unpriced), run `job_export`,
assert file presence, `schema_version`, market count, a broken market's
reason, spell `open` flag, and that a re-run overwrites atomically. Pure-JSON
shaping helpers (row → dict) go in the job module and get direct unit tests.

### 3. Deploy + Caddy (on the VPS, `ubuntu@51.210.107.138`)

1. Push to main, `git pull` in `~/mnemon` (+ `uv sync` if deps changed).
   Scheduler picks the job up next tick. Force-run once:
   `uv run python -m mnemon run --only export`.
2. **DNS first** (Vercel panel): add A record `data` → `51.210.107.138`.
   Confirm `dig +short data.myrmidons-strategies.com` returns ONLY the VPS IP
   before touching Caddy — Caddy's Let's Encrypt HTTP-01 challenge (port 80,
   already open) only succeeds once DNS points at the box. Reloading before
   DNS resolves = failed cert issuance (retries, but noisy in journal).
3. Edit `/etc/caddy/Caddyfile` — add a NEW top-level vhost block (sibling of
   the `logs.*` block, not nested). Whole subdomain = the export dir, so no
   `handle_path` and no fighting the logs vhost's global `no-cache` header:

   ```caddy
   data.myrmidons-strategies.com {
     encode gzip
     header {
       Cache-Control "public, max-age=60"
       Access-Control-Allow-Origin "*"
     }
     root * /home/ubuntu/mnemon/data/export
     file_server
   }
   ```

   Then `sudo systemctl reload caddy`. Watch the first cert issuance:
   `journalctl -u caddy -f`.
4. **Permission check** (likely gotcha): Caddy runs as the `caddy` user;
   `/home/ubuntu` may be `750`. Verify with
   `sudo -u caddy cat /home/ubuntu/mnemon/data/export/market_health.json`.
   If denied: `chmod o+x /home/ubuntu /home/ubuntu/mnemon
   /home/ubuntu/mnemon/data` and `o+rx` on `export/` (or point `export_dir`
   at `/srv/mnemon-export` owned by ubuntu, `o+rx`).
5. Verify from outside:
   `curl -sI https://data.myrmidons-strategies.com/market_health.json`
   → `200`, `Cache-Control: public, max-age=60`, gzip.

---

## Phase 2 — myrmidons-os repo (FE)

Branch + PR per repo convention; `pnpm build` must pass (typecheck + lint;
escape apostrophes in JSX).

### 1. API proxy — `app/api/mnemon/[snapshot]/route.ts`

Whitelist `snapshot ∈ {market-health, util-spells}` → fetch
`${MNEMON_DATA_URL}/<market_health|util_spells>.json` with
`next: { revalidate: 120 }`; 404 on anything else, pass through
`generated_at`. Env `MNEMON_DATA_URL`, default
`https://data.myrmidons-strategies.com`. No token needed (data is
public). Add Zod schemas in `lib/mnemon/schemas.ts` + TanStack hooks in
`lib/mnemon/queries.ts` (mirror the `lib/morpho/*` layering).

### 2. Tools registry + pane

- `components/tools/fileGroups.ts`: add
  `{ id: "mnemon", title: "MNEMON", status: "ACTIVE", access: "Public" }`.
- `ToolsWindowContent.tsx`: pane content for `mnemon` = compact summary
  (markets tracked, broken count with reason breakdown, data age from
  `generated_at`) + `OPEN MNEMON →` link to `/tools/mnemon`. Note the current
  component hardcodes the SWAP viewport header/labels — generalize per
  selected tool.

### 3. Page — `app/tools/mnemon/page.tsx`

Tabbed layout like the vault pages (single MARKETS tab in v1, tabs kept for
the deferred benchmark). Terminal aesthetic per CLAUDE.md conventions
(font-mono, CSS vars, uppercase micro-labels, `border-l border-t` grid /
`border-r border-b` panels, `content-start` on sparse grids).

- **Header strip**: markets tracked / broken count / chain / `DATA AGE:
  Xm` (stale > 45 min → gold warning).
- **Market table**: one row per market — `COLLATERAL/LOAN`, utilization,
  supply APY, APY@target, supply USD, available USD, status dot + reason
  micro-label (`RATE_RATCHET` / `PINNED_UTIL` / `DUST`, red; healthy green).
  Broken rows keep full data (the 639% APY dust market *should* show its
  absurd number — that's the story). Wide table scrolls in its own
  `overflow-x auto` container.
- **Drill-down** (row click, inline expand or side panel): 7d
  supply-APY + utilization sparkline (recharts, `history` array) and the
  market's utilization spells from `util_spells.json` (threshold, start,
  duration, `OPEN` badge for ongoing). This is where spells live — no
  separate tab.
- Loading: `TerminalScrollLoader`; animated values: `GlitchTypeText`.

### 4. CLI plumbing (`app/page.tsx`)

Per CLAUDE.md checklist: add `MNEMON` under `TOOLS/` in `ls`/`dir`/`pwd`
outputs, extend `SUGGEST_POOL`, `HIGHLIGHT_TERMS`, the `tools` help topic,
and make `mnemon` a command that opens the tool (same mechanism as
`openTools("swap")` → hash `tool=mnemon`).

### 5. Vercel

No new env required (default URL baked in); optionally set `MNEMON_DATA_URL`.

---

## Verification checklist (end-to-end)

1. VPS: `curl -sI https://data.myrmidons-strategies.com/market_health.json`
   → 200, `Cache-Control: public, max-age=60`, gzip.
2. `generated_at` advances across two 15-min ticks.
3. FE dev: `/api/mnemon/market-health` returns the JSON; `/tools/mnemon`
   renders table with the ~6 currently-broken markets flagged and reasons
   matching `v_market_health` on the VPS.
4. Drill-down sparkline shows 7d of points; a pinned market shows an OPEN
   spell.
5. `pnpm build` green; PR for owner review.

## Deferred (v2 candidates)

- HEGEMON benchmark tab (`v_hegemon_benchmark`: eligible / investable / bot
  tiers, opportunity vs deployable gap chart).
- Bot tick archive (`bot_scores` history).
- APY spread explorer (`v_apy_spread`).
