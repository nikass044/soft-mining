from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)


class RateLimitController:
    def __init__(self, max_retry_delay: int = 60):
        self._max_retry_delay = max_retry_delay
        self._rest_remaining: int | None = None
        self._rest_reset: float | None = None
        self._graphql_remaining: int | None = None
        self._graphql_reset: float | None = None

    def before_request(self, kind: str) -> None:
        remaining, reset_at = self._budget_for(kind)
        if remaining is not None and remaining <= 0 and reset_at is not None:
            wait = max(0, reset_at - time.time()) + 1
            logger.info("Rate budget exhausted for %s, waiting %.1fs", kind, wait)
            time.sleep(wait)

    def after_response(self, kind: str, headers: dict, body: dict | list | None = None) -> None:
        if kind == "rest":
            self._update_rest_from_headers(headers)
        elif kind == "graphql" and isinstance(body, dict):
            self._update_graphql_from_body(body)

    def handle_error(self, retry_after: float | None, attempt: int) -> float:
        if retry_after is not None:
            delay = min(retry_after, self._max_retry_delay)
        else:
            delay = min(2 ** attempt, self._max_retry_delay)

        logger.info("Retry attempt %d, waiting %.1fs", attempt, delay)
        return delay

    def _budget_for(self, kind: str) -> tuple[int | None, float | None]:
        if kind == "rest":
            return self._rest_remaining, self._rest_reset
        if kind == "graphql":
            return self._graphql_remaining, self._graphql_reset
        return None, None

    def _update_rest_from_headers(self, headers: dict) -> None:
        remaining = headers.get("x-ratelimit-remaining")
        reset_val = headers.get("x-ratelimit-reset")
        if remaining is not None:
            self._rest_remaining = int(remaining)
        if reset_val is not None:
            self._rest_reset = float(reset_val)

    def _update_graphql_from_body(self, body: dict) -> None:
        rate_limit = body.get("data", {}).get("rateLimit") if isinstance(body.get("data"), dict) else None
        if rate_limit is None:
            rate_limit = body.get("rateLimit")
        if rate_limit and isinstance(rate_limit, dict):
            if "remaining" in rate_limit:
                self._graphql_remaining = rate_limit["remaining"]
