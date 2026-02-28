from unittest.mock import MagicMock

from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases.phase1_pr_metadata import Phase1PRMetadata
from pr_digger.repository import Repository


def make_pr_payload(number, user_id=1, login="testuser", state="closed"):
    return {
        "number": number,
        "state": state,
        "created_at": "2024-01-01T00:00:00Z",
        "merged_at": "2024-01-02T00:00:00Z" if state == "closed" else None,
        "closed_at": "2024-01-02T00:00:00Z" if state == "closed" else None,
        "user": {"id": user_id, "login": login},
    }


class TestPhase1PRMetadata:
    def test_ingests_single_page_of_prs(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        api_client = MagicMock()
        api_client.get_rest.return_value = [
            make_pr_payload(1, user_id=10, login="alice"),
            make_pr_payload(2, user_id=20, login="bob"),
        ]

        phase = Phase1PRMetadata(
            repos=["facebook/react"],
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            checkpoint=checkpoint,
            per_page=100,
        )
        phase.execute()

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
            [make_pr_payload(i) for i in range(1, 4)],
            [],
        ]

        phase = Phase1PRMetadata(
            repos=["facebook/react"],
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            checkpoint=checkpoint,
            per_page=3,
        )
        phase.execute()

        assert api_client.get_rest.call_count == 2
        pr_count = repo.connection.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        assert pr_count == 3

        repo.close()

    def test_resumes_from_checkpoint(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        checkpoint = FileCheckpointStore(tmp_path / "checkpoints")
        checkpoint.save("phase1", {"facebook/react": 3})
        api_client = MagicMock()
        api_client.get_rest.return_value = []

        phase = Phase1PRMetadata(
            repos=["facebook/react"],
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            checkpoint=checkpoint,
        )
        phase.execute()

        call_params = api_client.get_rest.call_args
        assert call_params[1]["params"]["page"] == 3

        repo.close()
