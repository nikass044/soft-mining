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
        repo_id = repo.upsert_repository(RepoRecord("facebook", "react"))
        user_id = repo.upsert_user(UserRecord(1, "alice"))
        repo.upsert_pull_request(PullRequestRecord(
            repo_id=repo_id, number=1, author_user_id=user_id,
            state="closed", created_at=None, merged_at=None, closed_at=None,
        ))
        repo.commit()
        return repo, repo_id

    def test_ingests_files_for_pending_pr(self, tmp_path):
        repo, repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.post_graphql.return_value = make_graphql_files_response(
            ["src/index.js", "README.md"]
        )

        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            repo_id=repo_id,
        )
        phase.execute()

        files = repo.connection.execute("SELECT path FROM files ORDER BY path").fetchall()
        assert [f[0] for f in files] == ["README.md", "src/index.js"]

        pr_files = repo.connection.execute("SELECT COUNT(*) FROM pull_request_files").fetchone()[0]
        assert pr_files == 2

        assert len(repo.list_prs_pending_files(repo_id)) == 0
        repo.close()

    def test_handles_graphql_pagination(self, tmp_path):
        repo, repo_id = self._setup_repo_with_pr(tmp_path)
        api_client = MagicMock()
        api_client.post_graphql.side_effect = [
            make_graphql_files_response(["file1.js"], has_next=True, end_cursor="cursor1"),
            make_graphql_files_response(["file2.js"], has_next=False),
        ]

        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            repo_id=repo_id,
        )
        phase.execute()

        assert api_client.post_graphql.call_count == 2
        files = repo.connection.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        assert files == 2
        repo.close()

    def test_skips_already_synced_prs(self, tmp_path):
        repo, repo_id = self._setup_repo_with_pr(tmp_path)
        pr_id = repo.connection.execute("SELECT id FROM pull_requests").fetchone()[0]
        repo.mark_pr_files_synced(pr_id)
        repo.commit()

        api_client = MagicMock()
        phase = Phase2PRFiles(
            api_client=api_client,
            repository=repo,
            parser=PayloadParser(),
            repo_id=repo_id,
        )
        phase.execute()

        api_client.post_graphql.assert_not_called()
        repo.close()
