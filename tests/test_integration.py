import os

import pytest
from dotenv import load_dotenv

load_dotenv()

from pr_digger.api_client import BaseGitHubApiClient
from pr_digger.parser import PayloadParser
from pr_digger.phases.phase2_pr_files import PR_FILES_QUERY
from pr_digger.transport import HttpTransport

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
pytestmark = pytest.mark.skipif(not GITHUB_TOKEN, reason="GITHUB_TOKEN not set")


@pytest.fixture(scope="module")
def api_client():
    transport = HttpTransport(GITHUB_TOKEN)
    client = BaseGitHubApiClient(transport)
    yield client
    transport.close()


class TestIntegrationSmoke:
    def test_rest_fetch_prs(self, api_client):
        payload = api_client.get_rest(
            "/repos/facebook/react/pulls",
            params={"state": "all", "per_page": 3, "page": 1},
        )
        assert isinstance(payload, list)
        assert len(payload) == 3
        assert "number" in payload[0]
        assert "user" in payload[0]

    def test_graphql_fetch_pr_files(self, api_client):
        payload = api_client.post_graphql(
            PR_FILES_QUERY,
            {"owner": "facebook", "name": "react", "number": 1, "after": None},
        )
        parser = PayloadParser()
        batch = parser.parse_pr_files(payload, repo_id=0, pull_request_id=0)
        assert len(batch.file_paths) > 0

    def test_rest_fetch_reviews(self, api_client):
        payload = api_client.get_rest(
            "/repos/facebook/react/pulls/2/reviews",
            params={"per_page": 5, "page": 1},
        )
        assert isinstance(payload, list)
