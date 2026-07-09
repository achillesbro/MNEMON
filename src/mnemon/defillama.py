"""DefiLlama free endpoints only: coins.llama.fi (prices) and
yields.llama.fi/pools (venue yields). Nothing behind pro-api.llama.fi."""

from __future__ import annotations

import logging

from mnemon.http import HttpClient

log = logging.getLogger(__name__)

COINS_URL = "https://coins.llama.fi"
YIELDS_URL = "https://yields.llama.fi"

# coins.llama.fi caps a /chart response; 500 points per coin per call is the
# documented free-tier maximum, so backfills walk the range in 500h windows.
CHART_MAX_SPAN = 500


def coin_key(llama_slug: str, address: str) -> str:
    return f"{llama_slug}:{address}"


class LlamaClient:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def current_prices(self, coin_keys: list[str]) -> dict[str, dict]:
        """Batched current prices. Returns {coin_key: {price, symbol, timestamp, confidence}}."""
        out: dict[str, dict] = {}
        # Keep URLs reasonable: 25 coins per request.
        for i in range(0, len(coin_keys), 25):
            batch = ",".join(coin_keys[i : i + 25])
            data = self._http.get_json(f"{COINS_URL}/prices/current/{batch}")
            out.update(data.get("coins", {}))
        return out

    def price_chart(self, key: str, start_ts: int, span: int = CHART_MAX_SPAN, period: str = "1h") -> list[dict]:
        """Historical prices for one coin: `span` points of `period` from start_ts.
        Returns [{timestamp, price}] (may be shorter than span, or empty)."""
        data = self._http.get_json(
            f"{COINS_URL}/chart/{key}",
            params={"start": start_ts, "span": span, "period": period},
        )
        coin = data.get("coins", {}).get(key)
        return coin["prices"] if coin else []

    def yield_pools(self) -> list[dict]:
        """All pools from yields.llama.fi; caller filters by chain."""
        data = self._http.get_json(f"{YIELDS_URL}/pools")
        return data.get("data", [])
