from unittest.mock import MagicMock

from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases.phase1_pr_metadata import Phase1PRMetadata
from pr_digger.repository import (
    PullRequestRecord,
    RepoRecord,
    Repository,
    UserRecord,
)

REPO_INFO = {"id": 100, "full_name": "facebook/react"}


def make_pr_payload(number, user_id=1, login="testuser", state="closed",
                    created_at="2024-01-01T00:00:00Z", pr_id=None):
    return {
        "id": pr_id or (number * 1000),
        "number": number,
        "state": state,
        "created_at": created_at,
        "merged_at": "2024-01-02T00:00:00Z" if state == "closed" else None,
        "closed_at": "2024-01-02T00:00:00Z" if state == "closed" else None,
        "user": {"id": user_id, "login": login},
    }


def _make_phase(repo, checkpoint, api_client, per_page=100, earliest_date=None):
    return Phase1PRMetadata(
        repo_full_name="facebook/react",
        api_client=api_client,
        repository=repo,
        parser=PayloadParser(),
        checkpoint=checkpoint,
        per_page=per_page,
        earliest_date=earliest_date,
    )


class TestPhase1FullIngest:
    def test_ingests_single_page_of_prs(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(1, user_id=10, login="alice"),
             make_pr_payload(2, user_id=20, login="bob")],
        ]

        _make_phase(repo, checkpoint, api_client).execute()

        prs = repo.connection.execute("SELECT number, state FROM pull_requests ORDER BY number").fetchall()
        assert prs == [(1, "closed"), (2, "closed")]

        users = repo.connection.execute("SELECT login FROM users ORDER BY github_user_id").fetchall()
        assert [u[0] for u in users] == ["alice", "bob"]
        repo.close()

    def test_paginates_until_empty(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(i) for i in range(1, 4)],
            [],
        ]

        _make_phase(repo, checkpoint, api_client, per_page=3).execute()

        assert api_client.get_rest.call_count == 3
        pr_count = repo.connection.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        assert pr_count == 3
        repo.close()

    def test_resumes_from_checkpoint(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        checkpoint.save("phase1", {"facebook/react": 3})
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [],
        ]

        _make_phase(repo, checkpoint, api_client).execute()

        pr_page_call = api_client.get_rest.call_args_list[1]
        assert pr_page_call[1]["params"]["page"] == 3
        repo.close()

    def test_skips_prs_before_earliest_date(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(1, user_id=10, login="old_user", created_at="2016-06-15T00:00:00Z"),
             make_pr_payload(2, user_id=20, login="new_user", created_at="2017-03-01T00:00:00Z"),
             make_pr_payload(3, user_id=30, login="newer_user", created_at="2018-01-01T00:00:00Z")],
        ]

        _make_phase(repo, checkpoint, api_client, earliest_date="2017-01-01T00:00:00Z").execute()

        prs = repo.connection.execute("SELECT number FROM pull_requests ORDER BY number").fetchall()
        assert [p[0] for p in prs] == [2, 3]
        repo.close()


class TestPhase1IncrementalIngest:
    def _seed_existing_prs(self, repo, numbers):
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "existing"))
        for n in numbers:
            repo.upsert_pull_request(PullRequestRecord(
                github_pr_id=n * 1000, github_repo_id=100, number=n,
                author_github_user_id=1, state="closed",
                created_at=f"2024-01-{n:02d}T00:00:00Z",
                merged_at=None, closed_at=None,
            ))
        repo.commit()

    def test_skips_already_fetched_prs(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        self._seed_existing_prs(repo, [1, 2, 3])

        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(3, created_at="2024-01-03T00:00:00Z"),
             make_pr_payload(2, created_at="2024-01-02T00:00:00Z"),
             make_pr_payload(1, created_at="2024-01-01T00:00:00Z")],
        ]

        _make_phase(repo, checkpoint, api_client).execute()

        assert api_client.get_rest.call_count == 2
        pr_count = repo.connection.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        assert pr_count == 3
        repo.close()

    def test_fetches_only_new_prs(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        self._seed_existing_prs(repo, [1, 2])

        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(4, user_id=40, login="new_user", created_at="2024-01-04T00:00:00Z"),
             make_pr_payload(3, user_id=30, login="another", created_at="2024-01-03T00:00:00Z"),
             make_pr_payload(2, created_at="2024-01-02T00:00:00Z")],
        ]

        _make_phase(repo, checkpoint, api_client).execute()

        pr_count = repo.connection.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        assert pr_count == 4
        repo.close()

    def test_uses_desc_direction_for_incremental(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        self._seed_existing_prs(repo, [1])

        api_client = MagicMock()
        api_client.get_rest.side_effect = [
            REPO_INFO,
            [make_pr_payload(1, created_at="2024-01-01T00:00:00Z")],
        ]

        _make_phase(repo, checkpoint, api_client).execute()

        pr_page_call = api_client.get_rest.call_args_list[1]
        assert pr_page_call[1]["params"]["direction"] == "desc"
        repo.close()
