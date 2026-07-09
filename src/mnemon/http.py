"""Shared HTTP layer: one client for all APIs so rate limiting is global.

Being a good API citizen is a hard requirement of this project, so every
outgoing request goes through `HttpClient.request`, which enforces a minimum
delay between requests and retries transient failures with exponential
backoff (+ jitter, honouring Retry-After on 429).
"""

from __future__ import annotations

import logging
import random
import time

import httpx

from mnemon.config import HttpConfig

log = logging.getLogger(__name__)

# Statuses worth retrying: rate limit + transient server errors.
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class HttpError(Exception):
    """Request failed after all retries."""


class HttpClient:
    def __init__(self, cfg: HttpConfig) -> None:
        self._cfg = cfg
        self._client = httpx.Client(
            timeout=cfg.timeout_s,
            headers={"User-Agent": "mnemon/2.0 (personal research; low volume)"},
        )
        self._last_request_at = 0.0

    def close(self) -> None:
        self._client.close()

    def _throttle(self) -> None:
        wait = self._cfg.min_interval_ms / 1000 - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def request(
        self,
        method: str,
        url: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self._cfg.max_retries + 1):
            self._throttle()
            try:
                resp = self._client.request(method, url, json=json_body, params=params)
            except httpx.HTTPError as e:
                last_error = e
                self._sleep_before_retry(attempt, None, url)
                continue

            if resp.status_code < 400:
                return resp
            if resp.status_code not in RETRYABLE_STATUSES:
                raise HttpError(f"{method} {url} -> {resp.status_code}: {resp.text[:300]}")
            last_error = HttpError(f"{method} {url} -> {resp.status_code}")
            self._sleep_before_retry(attempt, resp, url)

        raise HttpError(f"{method} {url} failed after {self._cfg.max_retries + 1} attempts") from last_error

    def _sleep_before_retry(self, attempt: int, resp: httpx.Response | None, url: str) -> None:
        if attempt >= self._cfg.max_retries:
            return
        # Exponential backoff with full jitter, capped at 60s.
        delay = min(60.0, (2.0**attempt) * random.uniform(0.5, 1.5))
        if resp is not None and resp.headers.get("Retry-After", "").isdigit():
            delay = max(delay, float(resp.headers["Retry-After"]))
        log.warning("retrying %s in %.1fs (attempt %d)", url, delay, attempt + 1)
        time.sleep(delay)

    def get_json(self, url: str, params: dict | None = None) -> dict | list:
        return self.request("GET", url, params=params).json()

    def post_json(self, url: str, body: dict) -> dict:
        return self.request("POST", url, json_body=body).json()
