import logging
from unittest.mock import MagicMock

from pr_digger.parser import PayloadParser
from pr_digger.phases.phase2_pr_files import Phase2PRFiles
from pr_digger.repository import (
    PullRequestRecord,
    RepoRecord,
    Repository,
    UserRecord,
)


def make_graphql_files_response(paths, has_next=False, end_cursor=None):
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "number": 1,
                    "author": {"__typename": "User", "id": "U1", "login": "alice"},
                    "files": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                        "nodes": [{"path": p} for p in paths],
                    },
                }
            },
            "rateLimit": {"cost": 1, "remaining": 4999, "resetAt": "2024-01-01T01:00:00Z"},
        }
    }


class TestPhase2PRFiles:
    def _setup_repo_with_pr(self, tmp_path):
        repo = Repository(tmp_path / "test.db")
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "alice"))
        repo.upsert_pull_request(PullRequestRecord(
            github_pr_id=5000, github_repo_id=100, number=1,
            author_github_user_id=1, state="closed",
            created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()
        return repo, 100

    def test_ingests_files_for_pending_pr(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.post_graphql.return_value = make_graphql_files_response(
            ["src/index.js", "README.md"]
        )

        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
        )
        phase.execute()

        files = repo.connection.execute("SELECT path FROM files ORDER BY path").fetchall()
        assert [f[0] for f in files] == ["README.md", "src/index.js"]

        pr_files = repo.connection.execute("SELECT COUNT(*) FROM pull_request_files").fetchone()[0]
        assert pr_files == 2

        assert len(repo.list_prs_pending_files(github_repo_id)) == 0
        repo.close()

    def test_handles_graphql_pagination(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.post_graphql.side_effect = [
            make_graphql_files_response(["file1.js"], has_next=True, end_cursor="cursor1"),
            make_graphql_files_response(["file2.js"], has_next=False),
        ]

        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
        )
        phase.execute()

        assert api_client.post_graphql.call_count == 2
        files = repo.connection.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        assert files == 2
        repo.close()

    def test_skips_already_synced_prs(self, tmp_path):
        repo, github_repo_id = self._setup_repo_with_pr(tmp_path)
        repo.mark_pr_files_synced(5000)
        repo.commit()

        api_client = MagicMock()
        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            github_repo_id=github_repo_id,
        )
        phase.execute()

        api_client.post_graphql.assert_not_called()
        repo.close()

    def test_progress_reflects_global_state(self, tmp_path, caplog):
        repo = Repository(tmp_path / "test.db")
        repo.upsert_repository(RepoRecord(100, "facebook", "react"))
        repo.upsert_user(UserRecord(1, "alice"))

        for i in range(1, 4):
            repo.upsert_pull_request(PullRequestRecord(
                github_pr_id=5000 + i, github_repo_id=100, number=i,
                author_github_user_id=1, state="closed",
                created_at=None, merged_at=None, closed_at=None,
            ))
        repo.mark_pr_files_synced(5001)
        repo.mark_pr_files_synced(5002)
        repo.commit()

        api_client = MagicMock()
        api_client.post_graphql.return_value = make_graphql_files_response(["a.js"])

        phase = Phase2PRFiles(
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
