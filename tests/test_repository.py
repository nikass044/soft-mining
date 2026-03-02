import pytest

from pr_digger.repository import (
    Repository,
    RepoRecord,
    UserRecord,
    PullRequestRecord,
    FileRecord,
    PullRequestFileRecord,
    ReviewRecord,
)


@pytest.fixture
def repo(tmp_path):
    r = Repository(tmp_path / "test.db")
    yield r
    r.close()


class TestUpserts:
    def test_upsert_repository_returns_stable_id(self, repo):
        id1 = repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        id2 = repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        assert id1 == id2 == 100

    def test_upsert_user_updates_login(self, repo):
        repo.upsert_user(UserRecord(42, "old_login"))
        repo.upsert_user(UserRecord(42, "new_login"))
        repo.commit()

        row = repo.connection.execute(
            "SELECT login FROM users WHERE github_user_id=42"
        ).fetchone()
        assert row[0] == "new_login"

    def test_upsert_pull_request_idempotent(self, repo):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))

        pr_rec = PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=100,
            author_github_user_id=1, state="open",
            created_at="2024-01-01T00:00:00Z", merged_at=None, closed_at=None,
        )
        repo.upsert_pull_request(pr_rec)
        repo.upsert_pull_request(pr_rec)
        repo.commit()

        count = repo.connection.execute(
            "SELECT COUNT(*) FROM pull_requests"
        ).fetchone()[0]
        assert count == 1

    def test_upsert_file_and_pr_file(self, repo):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="closed",
            created_at=None, merged_at=None, closed_at=None,
        ))
        repo.upsert_file(FileRecord(100, "src/index.js"))

        repo.upsert_pull_request_file(PullRequestFileRecord(5000, "src/index.js"))
        repo.upsert_pull_request_file(PullRequestFileRecord(5000, "src/index.js"))
        repo.commit()

        count = repo.connection.execute(
            "SELECT COUNT(*) FROM pull_request_files"
        ).fetchone()[0]
        assert count == 1

    def test_upsert_review_idempotent(self, repo):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_user(UserRecord(2, "reviewer"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="closed",
            created_at=None, merged_at=None, closed_at=None,
        ))

        review = ReviewRecord(
            github_review_id=999, github_pr_id=5000,
            reviewer_github_user_id=2, state="APPROVED",
            submitted_at="2024-01-02T00:00:00Z",
        )
        repo.upsert_review(review)
        repo.upsert_review(review)
        repo.commit()

        count = repo.connection.execute(
            "SELECT COUNT(*) FROM pull_request_reviews"
        ).fetchone()[0]
        assert count == 1


class TestPendingQueries:
    def test_pending_files_and_sync(self, repo):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="open",
            created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()

        pending = repo.list_prs_pending_files(100)
        assert len(pending) == 1
        assert pending[0].github_pr_id == 5000

        repo.mark_pr_files_synced(5000)
        repo.commit()
        assert len(repo.list_prs_pending_files(100)) == 0

    def test_pending_reviews_and_sync(self, repo):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "author"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="open",
            created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()

        pending = repo.list_prs_pending_reviews(100)
        assert len(pending) == 1

        repo.mark_pr_reviews_synced(5000)
        repo.commit()
        assert len(repo.list_prs_pending_reviews(100)) == 0
