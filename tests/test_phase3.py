from unittest.mock import MagicMock

from pr_digger.parser import PayloadParser
from pr_digger.phases.phase3_pr_reviews import Phase3PRReviews
from pr_digger.repository import (
    PullRequestRecord,
    RepoRecord,
    Repository,
    UserRecord,
)


def make_review_payload(review_id, user_id=77, login="reviewer", state="APPROVED"):
    return {
        "id": review_id,
        "state": state,
        "submitted_at": "2024-01-05T00:00:00Z",
        "user": {"id": user_id, "login": login},
    }


class TestPhase3PRReviews:
    def _setup_repo_with_pr(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()
        return repo

    def test_ingests_reviews_for_pending_pr(self, tmp_path):
        repo = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.get_rest.return_value = [
            make_review_payload(501, user_id=10, login="reviewer1", state="APPROVED"),
            make_review_payload(502, user_id=20, login="reviewer2", state="CHANGES_REQUESTED"),
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
        )
        phase.execute()

        reviews = repo.connection.execute(
            "SELECT state FROM pull_request_reviews ORDER BY github_review_id"
        ).fetchall()
        assert [r[0] for r in reviews] == ["APPROVED", "CHANGES_REQUESTED"]

        assert len(repo.list_prs_pending_reviews()) == 0
        repo.close()

    def test_paginates_reviews(self, tmp_path):
        repo = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            [make_review_payload(i) for i in range(1, 4)],
            [],
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            per_page=3,
        )
        phase.execute()

        assert api_client.get_rest.call_count == 2
        repo.close()

    def test_skips_already_synced_prs(self, tmp_path):
        repo = self._setup_repo_with_pr(tmp_path)
        pr_id = repo.connection.execute("SELECT id FROM pull_requests").fetchone()[0]
        repo.mark_pr_reviews_synced(pr_id)
        repo.commit()

        api_client = MagicMock()
        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
        )
        phase.execute()

        api_client.get_rest.assert_not_called()
        repo.close()
