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
        id1 = repo.upsert_repository(RepoRecord("facebook", "react"))
        id2 = repo.upsert_repository(RepoRecord("facebook", "react"))
        assert id1 == id2

    def test_upsert_user_updates_login(self, repo):
        repo.upsert_user(UserRecord(42, "old_login"))
        repo.upsert_user(UserRecord(42, "new_login"))
        repo.commit()

        row = repo.connection.execute(
            "SELECT login FROM users WHERE github_user_id=42"
        ).fetchone()
        assert row[0] == "new_login"

    def test_upsert_pull_request_idempotent(self, repo):
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "author"))

        pr_rec = PullRequestRecord(
            repo_id=repo_id, number=100, author_user_id=user_id,
            state="open", created_at="2024-01-01T00:00:00Z",
            merged_at=None, closed_at=None,
        )
        id1 = repo.upsert_pull_request(pr_rec)
        id2 = repo.upsert_pull_request(pr_rec)
        assert id1 == id2

    def test_upsert_file_and_pr_file(self, repo):
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "author"))
        pr_id = repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))
        file_id = repo.upsert_file(FileRecord(repo_id, "src/index.js"))

        repo.upsert_pull_request_file(PullRequestFileRecord(pr_id, file_id))
        repo.upsert_pull_request_file(PullRequestFileRecord(pr_id, file_id))
        repo.commit()

        count = repo.connection.execute(
            "SELECT COUNT(*) FROM pull_request_files"
        ).fetchone()[0]
        assert count == 1

    def test_upsert_review_idempotent(self, repo):
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        author_id = repo.upsert_user(UserRecord(1, "author"))
        reviewer_id = repo.upsert_user(UserRecord(2, "reviewer"))
        pr_id = repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=author_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))

        review = ReviewRecord(
            github_review_id=999, pull_request_id=pr_id,
            reviewer_user_id=reviewer_id, state="APPROVED",
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
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "author"))
        pr_id = repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="open", created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()

        pending = repo.list_prs_pending_files(repo_id)
        assert len(pending) == 1
        assert pending[0].pr_id == pr_id

        repo.mark_pr_files_synced(pr_id)
        repo.commit()
        assert len(repo.list_prs_pending_files(repo_id)) == 0

    def test_pending_reviews_and_sync(self, repo):
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "author"))
        pr_id = repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="open", created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()

        pending = repo.list_prs_pending_reviews(repo_id)
        assert len(pending) == 1

        repo.mark_pr_reviews_synced(pr_id)
        repo.commit()
        assert len(repo.list_prs_pending_reviews(repo_id)) == 0
