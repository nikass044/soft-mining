from __future__ import annotations

from abc import ABC, abstractmethod

from pr_digger.errors import (
    PermanentError,
    RateLimitError,
    TransientError,
)
from pr_digger.transport import HttpResponse, HttpTransport


class GitHubApiClient(ABC):
    @abstractmethod
    def get_rest(self, path: str, params: dict | None = None) -> dict | list:
        ...

    @abstractmethod
    def post_graphql(self, query: str, variables: dict) -> dict:
        ...

    @abstractmethod
    def get_rate_limit(self) -> dict:
        ...


class BaseGitHubApiClient(GitHubApiClient):
    def __init__(self, transport: HttpTransport):
        self._transport = transport

    def get_rest(self, path: str, params: dict | None = None) -> dict | list:
        response = self._transport.request("GET", path, params=params)
        self._raise_for_status(response)
        return response.json()

    def post_graphql(self, query: str, variables: dict) -> dict:
        response = self._transport.request(
            "POST", "/graphql", json_body={"query": query, "variables": variables}
        )
        self._raise_for_status(response)
        data = response.json()
        if isinstance(data, dict) and "errors" in data:
            self._raise_for_graphql_errors(data["errors"])
        return data

    def get_rate_limit(self) -> dict:
        response = self._transport.request("GET", "/rate_limit")
        self._raise_for_status(response)
        return response.json()

    def _raise_for_status(self, response: HttpResponse) -> None:
        code = response.status_code
        if code < 400:
            return

        body_text = response.body.decode("utf-8", errors="replace")

        if code == 429:
            retry_after = self._parse_retry_after(response.headers)
            raise RateLimitError(code, "Rate limited", retry_after=retry_after, body=body_text)

        if code == 403 and "rate limit" in body_text.lower():
            retry_after = self._parse_retry_after(response.headers)
            raise RateLimitError(code, "Secondary rate limit", retry_after=retry_after, body=body_text)

        if code >= 500 or code == 408:
            raise TransientError(code, f"Server error: {code}", body=body_text)

        raise PermanentError(code, f"Client error: {code}", body=body_text)

    @staticmethod
    def _raise_for_graphql_errors(errors: list[dict]) -> None:
        for err in errors:
            err_type = err.get("type", "")
            if err_type == "RATE_LIMITED" or err.get("code") == "graphql_rate_limit":
                raise RateLimitError(200, f"GraphQL rate limit: {err.get('message', '')}")
        raise PermanentError(200, f"GraphQL errors: {errors}")

    @staticmethod
    def _parse_retry_after(headers: dict[str, str]) -> float | None:
        val = headers.get("retry-after") or headers.get("Retry-After")
        if val is None:
            return None
        try:
            return float(val)
        except ValueError:
            return None
