from unittest.mock import MagicMock, patch

import pytest

from pr_digger.errors import PermanentError, RateLimitError, TransientError
from pr_digger.rate_limit import RateLimitController
from pr_digger.retrying_client import RetryingGitHubApiClient


class TestRetryingClient:
    def _make_client(self):
        inner = MagicMock()
        controller = RateLimitController(max_retry_delay=1)
        client = RetryingGitHubApiClient(inner, controller)
        return client, inner

    def test_success_on_first_try(self):
        client, inner = self._make_client()
        inner.get_rest.return_value = [{"id": 1}]

        result = client.get_rest("/repos/facebook/react/pulls")
        assert result == [{"id": 1}]
        assert inner.get_rest.call_count == 1

    @patch("pr_digger.retrying_client.time.sleep")
    def test_retries_on_transient_error(self, mock_sleep):
        client, inner = self._make_client()
        inner.get_rest.side_effect = [
            TransientError(500, "Server error"),
            [{"id": 1}],
        ]

        result = client.get_rest("/some/path")
        assert result == [{"id": 1}]
        assert inner.get_rest.call_count == 2
        mock_sleep.assert_called_once()

    @patch("pr_digger.retrying_client.time.sleep")
    def test_retries_on_rate_limit_error(self, mock_sleep):
        client, inner = self._make_client()
        inner.get_rest.side_effect = [
            RateLimitError(429, "Rate limited", retry_after=1.0),
            [{"id": 1}],
        ]

        result = client.get_rest("/some/path")
        assert result == [{"id": 1}]
        assert inner.get_rest.call_count == 2

    def test_permanent_error_not_retried(self):
        client, inner = self._make_client()
        inner.get_rest.side_effect = PermanentError(404, "Not found")

        with pytest.raises(PermanentError):
            client.get_rest("/nonexistent")
        assert inner.get_rest.call_count == 1

    @patch("pr_digger.retrying_client.time.sleep")
    def test_retries_indefinitely_until_success(self, mock_sleep):
        client, inner = self._make_client()
        inner.get_rest.side_effect = [
            *[TransientError(500, "Server error") for _ in range(5)],
            [{"id": 1}],
        ]

        result = client.get_rest("/some/path")
        assert result == [{"id": 1}]
        assert inner.get_rest.call_count == 6
        assert mock_sleep.call_count == 5

    @patch("pr_digger.retrying_client.time.sleep")
    def test_graphql_retry(self, mock_sleep):
        client, inner = self._make_client()
        inner.post_graphql.side_effect = [
            TransientError(502, "Bad gateway"),
            {"data": {"repository": {}}},
        ]

        result = client.post_graphql("query { }", {})
        assert "data" in result
        assert inner.post_graphql.call_count == 2

    @patch("pr_digger.retrying_client.time.sleep")
    def test_graphql_rate_limit_retried(self, mock_sleep):
        client, inner = self._make_client()
        inner.post_graphql.side_effect = [
            RateLimitError(200, "GraphQL rate limit: API rate limit already exceeded"),
            {"data": {"repository": {"pullRequest": {"files": {"nodes": []}}}}},
        ]

        result = client.post_graphql("query { }", {})
        assert "data" in result
        assert inner.post_graphql.call_count == 2
        mock_sleep.assert_called_once()
