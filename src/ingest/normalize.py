"""Pure normalization: raw API payloads -> table rows.

Everything here is side-effect free and unit-tested against recorded fixture
responses (tests/fixtures/). All timestamps are floored to the cadence bucket
they belong to, because the bucket is part of the upsert key.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingest.storage import floor_ts

HOUR = 3600
DAY = 86400


def as_int(v: Any) -> int | None:
    """The API serializes BigInt as a JSON number when small and a string when
    large; accept both (and None)."""
    if v is None:
        return None
    return int(v)


def _dt(unix_ts: float) -> datetime:
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc)


# --- market_state ---------------------------------------------------------


def market_state_rows_live(items: list[dict], ts: datetime) -> list[dict]:
    """Rows from the live `markets { state }` query, stamped with the current
    15-min bucket."""
    rows = []
    for it in items:
        st = it.get("state")
        if st is None:
            continue
        rows.append(
            {
                "ts": ts,
                "chain_id": it["chain"]["id"],
                "market_id": it["marketId"],
                "total_supply_assets": as_int(st["supplyAssets"]),
                "total_supply_shares": as_int(st["supplyShares"]),
                "total_borrow_assets": as_int(st["borrowAssets"]),
                "total_borrow_shares": as_int(st["borrowShares"]),
                "rate_at_target": as_int(st.get("rateAtTarget")),
                "utilization": st.get("utilization"),
                "oracle_price_raw": str(st["price"]) if st.get("price") is not None else None,
                "api_timestamp": _dt(st["timestamp"]) if st.get("timestamp") else None,
                "source": "live",
            }
        )
    return rows


def market_state_rows_history(market_id: str, chain_id: int, hist: dict) -> list[dict]:
    """Merge the six historicalState series into hourly rows.

    Points arrive unordered and the final point sits at "now" rather than on
    an hour boundary, so each x is floored to its hour bucket; when two points
    land in one bucket the later one wins. Oracle price has no history series
    in the API, so backfilled rows leave it null.
    """
    series_to_col = {
        "supplyAssets": "total_supply_assets",
        "supplyShares": "total_supply_shares",
        "borrowAssets": "total_borrow_assets",
        "borrowShares": "total_borrow_shares",
        "rateAtTarget": "rate_at_target",
        "utilization": "utilization",
    }
    int_cols = {c for c in series_to_col.values() if c != "utilization"}

    buckets: dict[datetime, dict] = {}
    last_x: dict[tuple[datetime, str], float] = {}
    for series_name, col in series_to_col.items():
        for point in hist.get(series_name) or []:
            bucket = floor_ts(point["x"], HOUR)
            row = buckets.setdefault(bucket, {})
            if point["x"] >= last_x.get((bucket, col), -1.0):
                last_x[(bucket, col)] = point["x"]
                row[col] = as_int(point["y"]) if col in int_cols else point["y"]

    return [
        {
            "ts": bucket,
            "chain_id": chain_id,
            "market_id": market_id,
            "total_supply_assets": row.get("total_supply_assets"),
            "total_supply_shares": row.get("total_supply_shares"),
            "total_borrow_assets": row.get("total_borrow_assets"),
            "total_borrow_shares": row.get("total_borrow_shares"),
            "rate_at_target": row.get("rate_at_target"),
            "utilization": row.get("utilization"),
            "oracle_price_raw": None,
            "api_timestamp": None,
            "source": "backfill",
        }
        for bucket, row in sorted(buckets.items())
    ]


# --- markets (dimension) ----------------------------------------------------


def markets_dim_rows(items: list[dict], fetched_at: datetime) -> list[dict]:
    rows = []
    for it in items:
        loan = it.get("loanAsset") or {}
        coll = it.get("collateralAsset") or {}  # null for idle markets
        rows.append(
            {
                "chain_id": it["chain"]["id"],
                "market_id": it["marketId"],
                "loan_token": _lower(loan.get("address")),
                "loan_symbol": loan.get("symbol"),
                "loan_decimals": int(loan["decimals"]) if loan.get("decimals") is not None else None,
                "collateral_token": _lower(coll.get("address")),
                "collateral_symbol": coll.get("symbol"),
                "collateral_decimals": int(coll["decimals"]) if coll.get("decimals") is not None else None,
                "oracle": _lower(it.get("oracleAddress")),
                "irm": _lower(it.get("irmAddress")),
                "lltv": as_int(it.get("lltv")),
                "creation_ts": _dt(it["creationTimestamp"]) if it.get("creationTimestamp") else None,
                "listed": it.get("listed"),
                "fetched_at": fetched_at,
            }
        )
    return rows


def _lower(addr: str | None) -> str | None:
    return addr.lower() if addr else None


# --- vault_allocations -------------------------------------------------------


def vault_allocation_rows(vault: dict, chain_id: int, ts: datetime) -> list[dict]:
    rows = []
    for alloc in (vault.get("state") or {}).get("allocation") or []:
        rows.append(
            {
                "ts": ts,
                "chain_id": chain_id,
                "vault": vault["address"].lower(),
                "market_id": alloc["market"]["marketId"],
                "supply_assets": as_int(alloc["supplyAssets"]),
                "supply_shares": as_int(alloc.get("supplyShares")),
                "supply_cap": as_int(alloc.get("supplyCap")),
                "source": "live",
            }
        )
    return rows


def vault_allocation_history_rows(vault: dict, chain_id: int) -> list[dict]:
    """Hourly allocation history. The API's history exposes supplyAssets and
    supplyCap per market but not shares — those stay null in backfilled rows."""
    rows: dict[tuple[datetime, str], dict] = {}
    vault_addr = vault["address"].lower()
    for alloc in (vault.get("historicalState") or {}).get("allocation") or []:
        market_id = alloc["market"]["marketId"]
        for field, col in [("supplyAssets", "supply_assets"), ("supplyCap", "supply_cap")]:
            for point in alloc.get(field) or []:
                bucket = floor_ts(point["x"], HOUR)
                row = rows.setdefault(
                    (bucket, market_id),
                    {
                        "ts": bucket,
                        "chain_id": chain_id,
                        "vault": vault_addr,
                        "market_id": market_id,
                        "supply_assets": None,
                        "supply_shares": None,
                        "supply_cap": None,
                        "source": "backfill",
                    },
                )
                row[col] = as_int(point["y"])
    return [rows[k] for k in sorted(rows)]


# --- positions ---------------------------------------------------------------


def position_rows(items: list[dict], ts: datetime) -> list[dict]:
    rows = []
    for it in items:
        st = it.get("state") or {}
        rows.append(
            {
                "ts": ts,
                "chain_id": it["market"]["chain"]["id"],
                "market_id": it["market"]["marketId"],
                "borrower": it["user"]["address"].lower(),
                "collateral": as_int(st.get("collateral")),
                "borrow_shares": as_int(st.get("borrowShares")),
                "borrow_assets": as_int(st.get("borrowAssets")),
                "supply_shares": as_int(st.get("supplyShares")),
                "health_factor": it.get("healthFactor"),
            }
        )
    return rows


# --- prices ------------------------------------------------------------------


def price_rows_llama_current(
    coins: dict[str, dict], slug_to_chain: dict[str, int], ts: datetime
) -> list[dict]:
    """coins.llama.fi current-price response -> rows. Keys are 'slug:address'."""
    rows = []
    for key, coin in coins.items():
        slug, _, address = key.partition(":")
        if slug not in slug_to_chain or coin.get("price") is None:
            continue
        rows.append(
            {
                "ts": ts,
                "chain_id": slug_to_chain[slug],
                "token_address": address.lower(),
                "price_usd": float(coin["price"]),
                "source": "llama",
                "confidence": coin.get("confidence"),
            }
        )
    return rows


def price_rows_llama_chart(chain_id: int, address: str, points: list[dict]) -> list[dict]:
    """Historical chart points -> hourly rows (later point in a bucket wins)."""
    by_bucket: dict[datetime, dict] = {}
    for p in points:
        if p.get("price") is None:
            continue
        bucket = floor_ts(p["timestamp"], HOUR)
        by_bucket[bucket] = {
            "ts": bucket,
            "chain_id": chain_id,
            "token_address": address.lower(),
            "price_usd": float(p["price"]),
            "source": "llama_chart",
            "confidence": p.get("confidence"),
        }
    return [by_bucket[k] for k in sorted(by_bucket)]


def price_rows_morpho(items: list[dict], ts: datetime) -> list[dict]:
    """Fallback: Morpho API current priceUsd for tokens DefiLlama doesn't cover."""
    rows = []
    for it in items:
        if it.get("priceUsd") is None:
            continue
        rows.append(
            {
                "ts": ts,
                "chain_id": it["chain"]["id"],
                "token_address": it["address"].lower(),
                "price_usd": float(it["priceUsd"]),
                "source": "morpho",
                "confidence": None,
            }
        )
    return rows


def price_rows_morpho_history(chain_id: int, address: str, series: list[dict]) -> list[dict]:
    by_bucket: dict[datetime, dict] = {}
    for p in series or []:
        if p.get("y") is None:
            continue
        bucket = floor_ts(p["x"], HOUR)
        by_bucket[bucket] = {
            "ts": bucket,
            "chain_id": chain_id,
            "token_address": address.lower(),
            "price_usd": float(p["y"]),
            "source": "morpho_history",
            "confidence": None,
        }
    return [by_bucket[k] for k in sorted(by_bucket)]


# --- yield_pools --------------------------------------------------------------


def yield_pool_rows(pools: list[dict], chains: set[str], ts: datetime) -> list[dict]:
    return [
        {
            "ts": ts,
            "pool_id": p["pool"],
            "chain": p["chain"],
            "project": p["project"],
            "symbol": p.get("symbol"),
            "tvl_usd": p.get("tvlUsd"),
            "apy": p.get("apy"),
            "apy_base": p.get("apyBase"),
            "apy_reward": p.get("apyReward"),
        }
        for p in pools
        if p.get("chain") in chains
    ]
