import json

import pytest

from pr_digger.api_client import BaseGitHubApiClient
from pr_digger.errors import PermanentError, RateLimitError, TransientError
from pr_digger.transport import HttpResponse


class FakeTransport:
    def __init__(self, responses: list[HttpResponse]):
        self._responses = list(responses)
        self.requests: list[tuple] = []

    def request(self, method, url, params=None, json_body=None):
        self.requests.append((method, url, params, json_body))
        return self._responses.pop(0)


def make_response(status=200, body=None, headers=None):
    if body is None:
        body = {}
    return HttpResponse(
        status_code=status,
        headers=headers or {},
        body=json.dumps(body).encode(),
    )


class TestBaseGitHubApiClient:
    def test_get_rest_success(self):
        transport = FakeTransport([make_response(body=[{"id": 1}])])
        client = BaseGitHubApiClient(transport)

        result = client.get_rest("/repos/facebook/react/pulls", {"state": "all"})
        assert result == [{"id": 1}]
        assert transport.requests[0][:2] == ("GET", "/repos/facebook/react/pulls")

    def test_post_graphql_success(self):
        body = {"data": {"repository": {"pullRequest": {"number": 1}}}}
        transport = FakeTransport([make_response(body=body)])
        client = BaseGitHubApiClient(transport)

        result = client.post_graphql("query { }", {"owner": "facebook"})
        assert result["data"]["repository"]["pullRequest"]["number"] == 1

    def test_post_graphql_with_errors_raises_permanent(self):
        body = {"errors": [{"message": "bad query"}]}
        transport = FakeTransport([make_response(body=body)])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(PermanentError, match="GraphQL errors"):
            client.post_graphql("bad query", {})

    def test_post_graphql_rate_limit_raises_rate_limit_error(self):
        body = {"errors": [{"type": "RATE_LIMITED", "code": "graphql_rate_limit",
                            "message": "API rate limit already exceeded"}]}
        transport = FakeTransport([make_response(body=body)])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(RateLimitError, match="GraphQL rate limit"):
            client.post_graphql("query { }", {})

    def test_429_raises_rate_limit_error(self):
        transport = FakeTransport([
            make_response(status=429, body={"message": "rate limited"}, headers={"retry-after": "30"})
        ])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(RateLimitError) as exc_info:
            client.get_rest("/rate_limit")
        assert exc_info.value.retry_after == 30.0

    def test_403_with_rate_limit_body_raises_rate_limit_error(self):
        transport = FakeTransport([
            make_response(status=403, body={"message": "API rate limit exceeded"})
        ])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(RateLimitError):
            client.get_rest("/some/path")

    def test_500_raises_transient_error(self):
        transport = FakeTransport([make_response(status=500)])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(TransientError):
            client.get_rest("/some/path")

    def test_404_raises_permanent_error(self):
        transport = FakeTransport([make_response(status=404)])
        client = BaseGitHubApiClient(transport)

        with pytest.raises(PermanentError):
            client.get_rest("/nonexistent")
