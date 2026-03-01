from __future__ import annotations

import logging
import time

import httpx

from pr_digger.api_client import GitHubApiClient
from pr_digger.errors import RateLimitError, TransientError
from pr_digger.rate_limit import RateLimitController

logger = logging.getLogger(__name__)


class RetryingGitHubApiClient(GitHubApiClient):
    def __init__(self, inner: GitHubApiClient, controller: RateLimitController):
        self._inner = inner
        self._controller = controller

    def get_rest(self, path: str, params: dict | None = None) -> dict | list:
        return self._with_retry("rest", lambda: self._inner.get_rest(path, params))

    def post_graphql(self, query: str, variables: dict) -> dict:
        return self._with_retry("graphql", lambda: self._inner.post_graphql(query, variables))

    def get_rate_limit(self) -> dict:
        return self._inner.get_rate_limit()

    def _with_retry(self, kind: str, call: callable) -> dict | list:
        attempt = 0
        while True:
            self._controller.before_request(kind)
            try:
                result = call()
                headers = {}
                if kind == "graphql" and isinstance(result, dict):
                    self._controller.after_response(kind, headers, body=result)
                return result
            except RateLimitError as e:
                delay = self._controller.handle_error(e.retry_after, attempt)
                logger.warning("Rate limit hit on %s: %s (retry in %.1fs)", kind, e, delay)
                time.sleep(delay)
                attempt += 1
            except TransientError as e:
                delay = self._controller.handle_error(None, attempt)
                logger.warning("Transient error on %s: %s (retry in %.1fs)", kind, e, delay)
                time.sleep(delay)
                attempt += 1
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                delay = self._controller.handle_error(None, attempt)
                logger.warning("Network error on %s: %s (retry in %.1fs)", kind, e, delay)
                time.sleep(delay)
                attempt += 1
