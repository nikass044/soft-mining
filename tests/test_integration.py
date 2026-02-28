import os

import pytest

from pr_digger.api_client import BaseGitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases.phase1_pr_metadata import Phase1PRMetadata
from pr_digger.phases.phase2_pr_files import Phase2PRFiles
from pr_digger.phases.phase3_pr_reviews import Phase3PRReviews
from pr_digger.repository import RepoRecord, Repository
from pr_digger.transport import HttpTransport

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
pytestmark = pytest.mark.skipif(not GITHUB_TOKEN, reason="GITHUB_TOKEN not set")


@pytest.fixture(scope="module")
def api_client():
    transport = HttpTransport(GITHUB_TOKEN)
    client = BaseGitHubApiClient(transport)
    yield client
    transport.close()


@pytest.fixture
def db_repo(tmp_path):
    repo = Repository(tmp_path / "integration.db")
    yield repo
    repo.close()


class TestIntegrationSmoke:
    def test_phase1_fetches_first_page(self, api_client, db_repo, tmp_path):
        checkpoint = FileCheckpointStore(tmp_path / "ckpt")

        phase = Phase1PRMetadata(
            repos=["facebook/react"],
            api_client=api_client,
            repository=db_repo,
            parser=PayloadParser(),
            checkpoint=checkpoint,
            per_page=5,
        )
        phase.execute()

        count = db_repo.connection.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        assert count == 5

    def test_phase2_fetches_files_for_one_pr(self, api_client, db_repo):
        repo_id = db_repo.upsert_repository(RepoRecord("facebook", "react"))
        from pr_digger.repository import UserRecord, PullRequestRecord
        user_id = db_repo.upsert_user(UserRecord(1, "test"))
        db_repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))
        db_repo.commit()

        phase = Phase2PRFiles(
            api_client=api_client,
            repository=db_repo,
            parser=PayloadParser(),
        )
        phase.execute()

        file_count = db_repo.connection.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        assert file_count > 0
        assert len(db_repo.list_prs_pending_files()) == 0

    def test_phase3_fetches_reviews_for_one_pr(self, api_client, db_repo):
        repo_id = db_repo.upsert_repository(RepoRecord("facebook", "react"))
        from pr_digger.repository import UserRecord, PullRequestRecord
        user_id = db_repo.upsert_user(UserRecord(1, "test"))
        # PR #2 on facebook/react is known to have reviews
        db_repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=2, author_user_id=user_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))
        db_repo.commit()

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=db_repo,
            parser=PayloadParser(),
        )
        phase.execute()

        assert len(db_repo.list_prs_pending_reviews()) == 0
