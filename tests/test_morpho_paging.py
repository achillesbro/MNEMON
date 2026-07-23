"""market_transactions pagination against the API's real quirks: pages come
back short of `first` (server drops rows after LIMIT — stride paging loses
them), and skip is capped at 10,000. The fetcher must walk by timestamp and
still see every event."""

from __future__ import annotations

from mnemon.morpho_api import MorphoClient


class FakeServer(MorphoClient):
    """Serves synthetic events; returns at most `page_cap` rows per request
    (page_cap=99 mimics the live short-page behaviour for first: 100)."""

    def __init__(self, timestamps: list[int], page_cap: int = 99) -> None:
        # no super().__init__: query() is fully overridden, no http needed
        self.events = [
            {"txHash": f"0x{i:04x}", "logIndex": i, "timestamp": t}
            for i, t in enumerate(sorted(timestamps))
        ]
        self.page_cap = page_cap
        self.requests = 0

    def query(self, q: str, variables: dict) -> dict:
        self.requests += 1
        since, skip, first = variables["sinceTs"], variables["skip"], variables["first"]
        matching = [e for e in self.events if e["timestamp"] >= since]
        page = matching[skip : skip + min(first, self.page_cap)]
        return {"marketTransactions": {"items": page, "pageInfo": {"count": len(page)}}}


def _keys(items: list[dict]) -> set:
    return {(it["txHash"], it["logIndex"]) for it in items}


def test_short_pages_do_not_end_the_walk_or_lose_rows():
    # 250 events, 99 per page: naive "count < first -> done" would stop at 99
    # and stride paging would drop the boundary rows. The ts-walk finds all.
    server = FakeServer(list(range(1000, 1250)))
    items, truncated = server.market_transactions(999, since_ts=0)
    assert not truncated
    assert _keys(items) == _keys(server.events)  # nothing lost at the seams


def test_request_budget_truncates_and_resumes_cleanly():
    server = FakeServer(list(range(1000, 1250)))
    items, truncated = server.market_transactions(999, since_ts=0, max_requests=2)
    assert truncated and 0 < len(_keys(items)) < 250

    resume_ts = max(int(it["timestamp"]) for it in items)
    more, truncated2 = server.market_transactions(999, since_ts=resume_ts)
    assert not truncated2
    assert _keys(items) | _keys(more) == _keys(server.events)


def test_same_second_flood_pages_by_skip_without_looping():
    # 150 events in ONE second: ts cannot advance, so the fetcher must fall
    # back to bounded skip within the window instead of spinning forever.
    server = FakeServer([1_700_000_000] * 150, page_cap=100)
    items, truncated = server.market_transactions(999, since_ts=0)
    assert not truncated
    assert _keys(items) == _keys(server.events)
    assert server.requests < 10  # bounded, not a spin
