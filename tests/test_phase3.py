import logging
from unittest.mock import MagicMock

from pr_digger.errors import PermanentError
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
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="closed",
            created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()
        return repo, 100

    def test_ingests_reviews_for_pending_pr(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.get_rest.return_value = [
            make_review_payload(501, user_id=10, login="reviewer1", state="APPROVED"),
            make_review_payload(502, user_id=20, login="reviewer2", state="CHANGES_REQUESTED"),
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
        )
        phase.execute()

        reviews = repo.connection.execute(
            "SELECT state FROM pull_request_reviews ORDER BY github_review_id"
        ).fetchall()
        assert [r[0] for r in reviews] == ["APPROVED", "CHANGES_REQUESTED"]

        assert len(repo.list_prs_pending_reviews(github_repo_id)) == 0
        repo.close()

    def test_paginates_reviews(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            [make_review_payload(i) for i in range(1, 4)],
            [],
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
            per_page=3,
        )
        phase.execute()

        assert api_client.get_rest.call_count == 2
        repo.close()

    def test_skips_already_synced_prs(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        repo.mark_pr_reviews_synced(5000)
        repo.commit()

        api_client = MagicMock()
        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
        )
        phase.execute()

        api_client.get_rest.assert_not_called()
        repo.close()

    def test_progress_reflects_global_state(self, tmp_path, caplog):
        repo = Repository(tmp_path / "test.db")
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_user(UserRecord(77, "reviewer"))

        for i in range(1, 4):
            repo.upsert_pull_request(PullRequestRecord(
                github_pr_id=5000 + i, github_repo_id=100, number=i,
                author_github_user_id=1, state="closed",
                created_at=None, merged_at=None, closed_at=None,
            ))
        repo.mark_pr_reviews_synced(5001)
        repo.mark_pr_reviews_synced(5002)
        repo.commit()

        api_client = MagicMock()
        api_client.get_rest.return_value = [
            make_review_payload(900),
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=100,
        )
        with caplog.at_level(logging.INFO):
            phase.execute()

        assert "2/3 synced, 1 pending" in caplog.text
        assert "3/3 100%" in caplog.text
        repo.close()

    def test_skips_failing_pr_and_continues(self, tmp_path, caplog):
        repo = Repository(tmp_path / "test.db")
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))

        for i in range(1, 3):
            repo.upsert_pull_request(PullRequestRecord(
                github_pr_id=5000 + i, github_repo_id=100, number=i,
                author_github_user_id=1, state="closed",
                created_at=None, merged_at=None, closed_at=None,
            ))
        repo.commit()

        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            PermanentError(404, "Not Found"),
            [make_review_payload(900)],
        ]

        phase = Phase3PRReviews(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=100,
        )
        with caplog.at_level(logging.INFO):
            phase.execute()

        assert "failed on facebook/react#1, skipping" in caplog.text
        assert len(repo.list_prs_pending_reviews(100)) == 0
        repo.close()
