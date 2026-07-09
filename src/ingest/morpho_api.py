"""Morpho GraphQL API client (https://blue-api.morpho.org/graphql).

Field-naming gotchas discovered by introspection (see docs/SCHEMA_NOTES.md):
- The Market id field is `marketId`, but the *filter* is `uniqueKey_in` and the
  single-object query is `marketById(marketId: ...)`. Same 0x-hex value everywhere.
- BigInt scalars are serialized as JSON numbers when small and strings when
  large — normalizers must accept both.
- Timeseries points come back in no guaranteed order and the final point is
  "now" rather than bucket-aligned.
- Queries have a complexity budget of 1,000,000. List queries (`markets(...)`)
  carry a huge fixed cost per timeseries field; single-object queries
  (`marketById`, `vaultByAddress`) are cheap (~60k for 6 series over a year),
  so all history fetching goes through single-object queries.
"""

from __future__ import annotations

import logging
from typing import Any

from ingest.http import HttpClient

log = logging.getLogger(__name__)

API_URL = "https://blue-api.morpho.org/graphql"


class MorphoApiError(Exception):
    pass


# --- queries ------------------------------------------------------------------

Q_VAULT_ALLOCATIONS = """
query VaultAllocations($address: String!, $chainId: Int!) {
  vaultByAddress(address: $address, chainId: $chainId) {
    address
    name
    state {
      timestamp
      allocation {
        supplyAssets
        supplyShares
        supplyCap
        market { marketId }
      }
    }
  }
}
"""

Q_MARKETS_META = """
query MarketsMeta($ids: [String!], $chainIds: [Int!]) {
  markets(first: 100, where: { uniqueKey_in: $ids, chainId_in: $chainIds }) {
    items {
      marketId
      chain { id }
      lltv
      oracleAddress
      irmAddress
      creationTimestamp
      listed
      loanAsset { address symbol decimals }
      collateralAsset { address symbol decimals }
    }
    pageInfo { countTotal }
  }
}
"""

Q_MARKETS_LIVE_STATE = """
query MarketsLiveState($ids: [String!], $chainIds: [Int!]) {
  markets(first: 100, where: { uniqueKey_in: $ids, chainId_in: $chainIds }) {
    items {
      marketId
      chain { id }
      state {
        timestamp
        supplyAssets
        supplyShares
        borrowAssets
        borrowShares
        rateAtTarget
        utilization
        price
      }
    }
    pageInfo { countTotal }
  }
}
"""

# All six raw-state series in one call: ~60k complexity for a year of hourly
# data, safely under the 1M budget even for multi-year markets.
Q_MARKET_HISTORY = """
query MarketHistory($id: String!, $chainId: Int!, $opts: TimeseriesOptions) {
  marketById(marketId: $id, chainId: $chainId) {
    marketId
    creationTimestamp
    historicalState {
      supplyAssets(options: $opts) { x y }
      supplyShares(options: $opts) { x y }
      borrowAssets(options: $opts) { x y }
      borrowShares(options: $opts) { x y }
      rateAtTarget(options: $opts) { x y }
      utilization(options: $opts) { x y }
    }
  }
}
"""

Q_VAULT_ALLOCATION_HISTORY = """
query VaultAllocationHistory($address: String!, $chainId: Int!, $opts: TimeseriesOptions) {
  vaultByAddress(address: $address, chainId: $chainId) {
    address
    historicalState {
      allocation {
        market { marketId }
        supplyAssets(options: $opts) { x y }
        supplyCap(options: $opts) { x y }
      }
    }
  }
}
"""

Q_POSITIONS_PAGE = """
query PositionsPage($ids: [String!], $chainIds: [Int!], $first: Int!, $skip: Int!) {
  marketPositions(
    first: $first
    skip: $skip
    orderBy: BorrowShares
    orderDirection: Desc
    where: { marketUniqueKey_in: $ids, chainId_in: $chainIds, borrowShares_gte: "1" }
  ) {
    items {
      user { address }
      market { marketId chain { id } }
      healthFactor
      state { collateral borrowShares borrowAssets supplyShares }
    }
    pageInfo { countTotal count }
  }
}
"""

Q_ASSET_PRICES = """
query AssetPrices($addresses: [String!], $chainIds: [Int!]) {
  assets(where: { address_in: $addresses, chainId_in: $chainIds }) {
    items { address chain { id } symbol decimals priceUsd }
  }
}
"""

Q_ASSET_PRICE_HISTORY = """
query AssetPriceHistory($address: String!, $chainId: Int!, $opts: TimeseriesOptions) {
  assetByAddress(address: $address, chainId: $chainId) {
    address
    historicalPriceUsd(options: $opts) { x y }
  }
}
"""


class MorphoClient:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def query(self, query: str, variables: dict[str, Any] | None = None) -> dict:
        payload = self._http.post_json(API_URL, {"query": query, "variables": variables or {}})
        if payload.get("errors"):
            raise MorphoApiError(str(payload["errors"][:3]))
        return payload["data"]

    # --- typed fetchers (return raw payload dicts; normalization lives in
    # normalize.py so it can be unit-tested against recorded fixtures) --------

    def vault_allocations(self, address: str, chain_id: int) -> dict | None:
        return self.query(Q_VAULT_ALLOCATIONS, {"address": address, "chainId": chain_id})["vaultByAddress"]

    def markets_meta(self, market_ids: list[str], chain_ids: list[int]) -> list[dict]:
        return self._paged_markets(Q_MARKETS_META, market_ids, chain_ids)

    def markets_live_state(self, market_ids: list[str], chain_ids: list[int]) -> list[dict]:
        return self._paged_markets(Q_MARKETS_LIVE_STATE, market_ids, chain_ids)

    def _paged_markets(self, query: str, market_ids: list[str], chain_ids: list[int]) -> list[dict]:
        # `first: 100` covers current needs; chunk the id list to stay safe.
        items: list[dict] = []
        for i in range(0, len(market_ids), 100):
            data = self.query(query, {"ids": market_ids[i : i + 100], "chainIds": chain_ids})
            items.extend(data["markets"]["items"])
        return items

    def market_history(self, market_id: str, chain_id: int, start_ts: int, end_ts: int) -> dict | None:
        opts = {"startTimestamp": start_ts, "endTimestamp": end_ts, "interval": "HOUR"}
        return self.query(Q_MARKET_HISTORY, {"id": market_id, "chainId": chain_id, "opts": opts})["marketById"]

    def vault_allocation_history(self, address: str, chain_id: int, start_ts: int, end_ts: int) -> dict | None:
        opts = {"startTimestamp": start_ts, "endTimestamp": end_ts, "interval": "HOUR"}
        return self.query(
            Q_VAULT_ALLOCATION_HISTORY, {"address": address, "chainId": chain_id, "opts": opts}
        )["vaultByAddress"]

    def positions(self, market_ids: list[str], chain_ids: list[int], max_pages: int) -> list[dict]:
        """Current positions with debt. The API only serves *current* positions;
        history accumulates forward via daily snapshots."""
        items: list[dict] = []
        for page in range(max_pages):
            data = self.query(
                Q_POSITIONS_PAGE,
                {"ids": market_ids, "chainIds": chain_ids, "first": 100, "skip": page * 100},
            )["marketPositions"]
            items.extend(data["items"])
            if data["pageInfo"]["count"] < 100:
                return items
        log.warning("positions truncated at %d pages (%d rows)", max_pages, len(items))
        return items

    def asset_prices(self, addresses: list[str], chain_ids: list[int]) -> list[dict]:
        return self.query(Q_ASSET_PRICES, {"addresses": addresses, "chainIds": chain_ids})["assets"]["items"]

    def asset_price_history(self, address: str, chain_id: int, start_ts: int, end_ts: int) -> dict | None:
        opts = {"startTimestamp": start_ts, "endTimestamp": end_ts, "interval": "HOUR"}
        return self.query(
            Q_ASSET_PRICE_HISTORY, {"address": address, "chainId": chain_id, "opts": opts}
        )["assetByAddress"]
