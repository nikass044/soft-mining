import threading
from unittest.mock import MagicMock, patch

from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.orchestrator import MiningOrchestrator
from pr_digger.parser import PayloadParser
from pr_digger.repository import (
    PullRequestRecord,
    RepoRecord,
    Repository,
    UserRecord,
)


def _seed_repo_with_pr(db_path):
    repo = Repository(db_path)
    repo.upsert_repository(RepoRecord(100, "test", "repo"))
    repo.upsert_user(UserRecord(1, "alice"))
    repo.upsert_pull_request(PullRequestRecord(
        github_pr_id=5000, github_repo_id=100, number=1,
        author_github_user_id=1, state="closed",
        created_at="2024-01-01T00:00:00Z", merged_at=None, closed_at=None,
    ))
    repo.commit()
    repo.close()
    return 100


def _make_orchestrator(tmp_path, base_client=None):
    db_path = tmp_path / "test.db"
    return MiningOrchestrator(
        repos=["test/repo"],
        base_client=base_client or MagicMock(),
        parser=PayloadParser(),
        checkpoint=FileCheckpointStore(tmp_path / "checkpoints"),
        db_path=db_path,
        per_page=100,
        max_retry_delay=1,
    ), db_path


class TestMiningOrchestrator:
    def test_runs_pr_mining_before_others(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        execution_order = []

        def tracking_run_phase(key, **kwargs):
            execution_order.append(key)
            if key == "prs":
                _seed_repo_with_pr(db_path)

        with patch.object(orchestrator, "_run_phase", side_effect=tracking_run_phase):
            with patch.object(orchestrator, "_run_parallel") as mock_parallel:
                orchestrator.run(["prs", "files", "reviews"])

        assert execution_order == ["prs"]
        mock_parallel.assert_called_once()
        assert mock_parallel.call_args[0][0] == ["files", "reviews"]

    def test_files_and_reviews_run_in_parallel(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        github_repo_id = _seed_repo_with_pr(db_path)

        thread_names = []

        def tracking_run_phase(key, **kwargs):
            thread_names.append(threading.current_thread().name)

        with patch.object(orchestrator, "_run_phase", side_effect=tracking_run_phase):
            orchestrator._run_parallel(["files", "reviews"], github_repo_id)

        assert len(thread_names) == 2
        assert thread_names[0] != thread_names[1]

    def test_single_phase_runs_sequentially(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        github_repo_id = _seed_repo_with_pr(db_path)

        with patch.object(orchestrator, "_run_phase") as mock_run:
            orchestrator.run(["files"])

        mock_run.assert_called_once_with("files", github_repo_id=github_repo_id)

    def test_parallel_error_propagated(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        github_repo_id = _seed_repo_with_pr(db_path)

        def failing_run_phase(key, **kwargs):
            if key == "files":
                raise RuntimeError("GraphQL failure")

        with patch.object(orchestrator, "_run_phase", side_effect=failing_run_phase):
            try:
                orchestrator._run_parallel(["files", "reviews"], github_repo_id)
                assert False, "Should have raised"
            except RuntimeError as exc:
                assert "GraphQL failure" in str(exc)

    def test_parallel_both_threads_finish_even_on_error(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        github_repo_id = _seed_repo_with_pr(db_path)
        completed = []

        def tracking_run_phase(key, **kwargs):
            if key == "files":
                raise RuntimeError("fail")
            completed.append(key)

        with patch.object(orchestrator, "_run_phase", side_effect=tracking_run_phase):
            try:
                orchestrator._run_parallel(["files", "reviews"], github_repo_id)
            except RuntimeError:
                pass

        assert "reviews" in completed

    def test_each_phase_gets_own_repository(self, tmp_path):
        orchestrator, db_path = _make_orchestrator(tmp_path)
        _seed_repo_with_pr(db_path)

        repo_ids = []
        original_create = orchestrator._create_resources

        def tracking_create():
            repo, client = original_create()
            repo_ids.append(id(repo))
            return repo, client

        noop_phase = MagicMock()

        with patch.object(orchestrator, "_create_resources", side_effect=tracking_create):
            with patch.object(orchestrator, "_build_phase", return_value=noop_phase):
                orchestrator.run(["prs", "files"])

        assert len(repo_ids) == 2
        assert repo_ids[0] != repo_ids[1]
