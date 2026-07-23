# Morpho GraphQL API — introspection findings

Recorded 2026-07-09 against `https://blue-api.morpho.org/graphql` — since
renamed to `https://api.morpho.org/graphql` (same API; verified identical
responses 2026-07-16) — with live queries on HyperEVM (chainId 999). This is
the evidence behind the backfill design; if the API changes, re-verify here
first.

## Historical timeseries that actually exist

### `Market.historicalState` (type `MarketHistory`)

Each field takes `options: TimeseriesOptions { startTimestamp, endTimestamp,
interval }` and returns `[{x: Float (unix seconds), y}]`.

Raw-state series used for `market_state` backfill:

| API field       | our column           | notes                          |
|-----------------|----------------------|--------------------------------|
| `supplyAssets`  | total_supply_assets  | BigInt                         |
| `supplyShares`  | total_supply_shares  | BigInt                         |
| `borrowAssets`  | total_borrow_assets  | BigInt                         |
| `borrowShares`  | total_borrow_shares  | BigInt                         |
| `rateAtTarget`  | rate_at_target       | per-second WAD (AdaptiveCurveIRM) |
| `utilization`   | utilization          | Float                          |

Also available (derived, we intentionally don't store them): `supplyApy`,
`borrowApy`, `netSupplyApy`, `apyAtTarget`, `liquidityAssets`,
`collateralAssets`, USD variants, and daily/weekly/monthly/... APY averages.

**There is NO oracle price history in `MarketHistory`.** Only current
`MarketState.price` exists. Token USD price history comes from
`Asset.historicalPriceUsd` / DefiLlama instead; the raw oracle price is
captured live every 15 min going forward.

### `VaultHistory.allocation` (type `VaultAllocationHistory`)

Per market: `supplyAssets(options)` and `supplyCap(options)`. **No
`supplyShares` history** — backfilled vault_allocations rows have null shares.

### `Asset.historicalPriceUsd(options)`

Full hourly USD price history per token, served by the Morpho API itself
(one query per token). Verified populated for HyperEVM assets (kHYPE etc.).

### `MarketPosition.historicalState` (type `MarketPositionHistory`)

Exists per (user, market): collateral/borrowAssets/borrowShares/... series.
Not used: enumerating borrowers still requires the current-positions query,
so we snapshot current positions daily and let history accumulate forward.
If deep position history for a specific borrower is ever needed, this field
can serve it retroactively.

## Verified data coverage on HyperEVM (chain 999)

- `interval: HOUR` is the finest history granularity (enum: HOUR, DAY, WEEK,
  MONTH, QUARTER, YEAR). Hence: backfill is hourly, live sampling is 15-min.
- Full market lifetime coverage: the USDT0/kHYPE market (created 2025-07-11)
  returned 8,704 hourly points with zero gaps in one query.
- Vault allocation history: hourly, verified for MYRMIDONS USDT0 (20 markets).
- Current borrower positions: `marketPositions` with `borrowShares_gte: "1"`,
  paginated 100/page (`pageInfo.countTotal` for verification).

## Gotchas (all handled in code, kept here so nobody re-discovers them)

1. **`uniqueKey` vs `marketId`**: the Market *field* is `marketId`, but the
   *filter* is `uniqueKey_in` and the single-object query is
   `marketById(marketId: ...)`. Same 0x-hex value in all three places.
   (`uniqueKey` as a field only existed on a legacy REST-era endpoint.)
2. **Complexity budget**: every query response carries
   `extensions.complexity`, capped at 1,000,000. List queries pay a huge
   fixed cost per timeseries field (~1M for one series via `markets(...)`),
   while single-object queries are cheap:
   - `marketById` with all 6 series over a full year: ~60k
   - `vaultByAddress` allocation history, 20 markets x 2 series x 30d: ~30k
   All history fetching therefore goes through single-object queries,
   windowed (365d markets / 180d vaults) for safety.
3. **BigInt serialization**: JSON number when small, string when large
   (e.g. `price`: `"68996150867968122500000000"`). `normalize.as_int`
   accepts both; Python's json parses big ints exactly.
4. **Timeseries point order**: not guaranteed ascending; the final point is
   "now", not bucket-aligned. Normalizers floor x to the hour bucket and let
   the later point win within a bucket.
5. **Out-of-range windows return empty lists** (not errors) — e.g. asking
   for history before market creation. An empty backfill is not proof of a
   dead market.
6. **Oracle price scale**: `MarketState.price` is scaled by
   `10^(36 + loanDecimals - collateralDecimals)`. Can exceed 38 digits, so
   it's stored as a string and converted in the `v_market_state` view.
7. **Idle markets** have `collateralAsset: null` (and a zero oracle) —
   dimension columns are nullable for that reason.

## DefiLlama endpoints (free tier only)

- `coins.llama.fi/prices/current/{keys}` — batch current prices.
  Coin key prefix for HyperEVM is **`hyperliquid:`** (e.g.
  `hyperliquid:0x5555...5555` = WHYPE). Includes `confidence`.
- `coins.llama.fi/chart/{key}?start=&span=&period=1h` — history, max ~500
  points per call. Used only as fallback: Morpho's `historicalPriceUsd`
  serves the same history in one query, which is kinder to both APIs.
- `yields.llama.fi/pools` — full pool dump; HyperEVM pools have
  `chain == "Hyperliquid L1"` (46 morpho-blue pools at time of writing).

## Reference addresses (chain 999)

- Morpho Blue: `0x68e37dE8d93d3496ae143F2E900490f6280C57cD`
- AdaptiveCurveIRM: `0xD4a426F010986dCad727e8dd6eed44cA4A9b7483`
- MYRMIDONS USDT0 vault: `0x4DC97f968B0Ba4Edd32D1b9B8Aaf54776c134d42`
- MYRMIDONS WHYPE vault: `0x889d35426F44A06EE89adF1eC4E5A4C9EB50a4f1`

## vaultV2transactions (introspected 2026-07-20, chain 999)

Verified live for vault `0xB851D568d123077E787860a34da286255249d983`: full
history from the first deposit (2026-07-17) is served — unlike bot logs, this
source supports backfill.

- Query: `vaultV2transactions(where, first, skip, orderBy, orderDirection)`.
  **`orderBy` enum is `Time | Shares`** (not `Timestamp`). Filters:
  `vaultAddress_in`, `userAddress_in`, `type_in`, `chainId_in`,
  `timestamp_gte/lte`, `assets/shares_gte/lte`, `hash`, `cursor`.
- Item fields: `txHash`, `logIndex`, `txIndex`, `blockNumber`, `timestamp`,
  `type` (`Deposit | Withdraw | Transfer`), `assets`, `shares`,
  `vault { address chain { id } }`, and a `data` union:
  `VaultV2DepositData { assets sender onBehalf }` (no receiver — onBehalf
  receives the shares) / `VaultV2WithdrawData { assets sender receiver
  onBehalf }` / `VaultV2TransferData`.
- Companion entities: `vaultV2ByAddress` (state: `totalAssets`, `idleAssets`,
  `totalSupply`, `sharePrice`, `totalAssetsUsd` — fields sit directly on the
  vault, no `state` wrapper) and `vaultV2AllocationTransactions` (unused so
  far; candidate for a future `reallocations` table).

## marketTransactions (introspected 2026-07-22, chain 999)

Per-market Morpho Blue events — source of the `market_flows` table. The old
`Query.transactions` entity is **deprecated (2026-04-23)** in favor of
`marketTransactions` / `vaultV1Transactions`; the replacement renames nearly
everything, so don't copy shapes from the old entity:

- Enum `MarketTransactionType` drops the `Market` prefix:
  `Supply | Withdraw | Borrow | Repay | SupplyCollateral | WithdrawCollateral
  | Liquidation` (old: `MarketSupply`, `MarketLiquidation`, ...).
- Item field is `txHash` (old entity used `hash`); also `logIndex`, `txIndex`,
  `blockNumber`, `timestamp`, `type`, `user { address }`,
  `market { marketId chain { id } }`.
- `data` union types are `MarketTransactionTransferData { assets shares }`
  (loan units), `MarketTransactionCollateralTransferData { assets }`
  (collateral units, **no shares**), `MarketTransactionLiquidationData
  { liquidator repaidAssets repaidShares seizedAssets badDebtAssets
  badDebtShares }` — old names were `MarketTransferTransactionData` etc.
  (`Transaction` and the qualifier swap places).
- Filters: `chainId_in`, `marketUniqueKey_in`, `userAddress_in`, `type_in`,
  `timestamp_gte/lte`, `assets_gte/lte`, `liquidatorAddress_in`, ...;
  `orderBy: Timestamp` works here (unlike vaultV2's `Time`).
- Volume: ~1.6M events all-history on chain 999 (~3.5k/day). That's why the
  `market_flows` job's first run backfills only `market_flows_backfill_hours`
  (default 7d ≈ 250 pages) instead of t=0 (~16k pages ≈ 1.4h at the 300ms
  throttle). Widen the config before the first run if more history is wanted.
- **`skip` is capped at 10,000** (BAD_USER_INPUT above it — discovered live
  2026-07-22 when the backfill stalled). Assume the same cap on every
  paginated entity (`marketPositions`, `vaultV2transactions`) — they just
  haven't hit it yet at current data sizes.
- **Pages come back SHORT of `first`** (discovered live 2026-07-23 when the
  catch-up crawled at one page per run): `first: 100` returns ~98-99 items
  mid-history — rows are dropped server-side AFTER the LIMIT, so `count <
  first` does NOT mean end-of-data, and stride paging (skip += 100) silently
  loses the dropped rows at every page boundary. Verified: walking the same
  range with 100-item vs 10-item pages by timestamp yields identical event
  sets, so the dropped tail rows reappear when the next window re-queries
  from the last timestamp seen. Hence `market_transactions` never trusts
  skip or page fullness: skip=0 always, advance `timestamp_gte` to the newest
  event seen, stop on an EMPTY page (the event key dedupes window seams);
  skip is only a bounded fallback within a single-second flood. The
  `market_flows` job commits each batch (upsert + cursor + state save) so an
  error mid-walk never loses fetched data.

Supplier positions: the same `marketPositions` entity that serves the borrower
book also serves lenders — filter `supplyShares_gte: "1"`, order by
`SupplyShares`, and fetch `state { supplyShares supplyAssets }`. The borrower
and supplier filters cannot be combined in one query (where-clauses AND
together), hence the separate `supplier_positions` job. Only ~900 supplier
positions exist chain-wide (2026-07-22) — the lender book is far lighter than
the borrower book.

## v_market_apy vs bot_scores.apy (cross-check)

`v_market_apy` reimplements the bot's supply-APY derivation in SQL
(AdaptiveCurveIRM `utilizationToRate`, steepness 4, target 0.9; 3-term Taylor
compounding), verified to 1e-12 against a Python port in
`tests/test_views_v2.py`. One systematic gap: `market_state` does not store
the market `fee`, so the view assumes fee = 0 while the bot multiplies by
`(1 − fee)`. Tracked HyperEVM markets currently run fee = 0, so the series
agree; if a market enables a fee, the view overstates its supply APY by
1/(1−fee) until a `fee` column is added. Residual sub-bp differences vs
`bot_scores.apy` at the same wall-clock time are sampling skew (the bot reads
the API at tick time; market_state samples on MNEMON's cadence).

## Broken-market classification (v_market_health, added 2026-07-21)

Operator-tuned fixed rules (algebra only; data-driven thresholds would belong
to the myrmidons library):
- **rate_ratchet**: `apy_at_target > 50%` enters broken, `< 25%` exits. The
  AdaptiveCurveIRM ratchets rateAtTarget ~2x per ~5 days pinned at u=1 and
  decays symmetrically, so this threshold is inherently time-integrated —
  the IRM is the hysteresis.
- **pinned_util**: u ≥ 0.999 across the entire trailing 24h enters, 48h fully
  below 0.95 exits (span guards against data holes).
- **dust**: supply < $1k USD (ASOF-joined `prices`) — broken unconditionally.
- **Thin exemption**: ratchet/pinned only classify when supply < $25k USD; a
  deep market sustaining a high rate is an opportunity, not a defect.
  Unpriced markets: treated as thin (rules apply) but never dust.

Live calibration (2026-07-21): broken set = PT-hbUSDT ($1, 639% ratchet),
UBTC-dust ($8, 639%), kHYPE-old ($93, 258%, 84% of 30d pinned), wstHYPE
($3.6k, 72%), beHYPE/WHYPE ($3 supply, pinned) — nothing legitimate sits
between 22% and 72% apy_at_target. `v_hegemon_benchmark` aggregates three tiers: the eligible universe, the
**investable** subset (eligible AND available liquidity ≥ $10k USD — mirrors
the bot's minAvailableLiquidity floor; `v_market_health.available_usd`), and
the bot's scored set. `opportunity_gap_apy` (universe) is the echo-chamber
antidote; `deployable_gap_apy` (investable) is the actionable version — a
market paying 40% with $79 of depth inflates the former but not the latter.
