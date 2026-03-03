from __future__ import annotations

import logging
import threading
from pathlib import Path

from pr_digger.api_client import GitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases.phase1_pr_metadata import Phase1PRMetadata
from pr_digger.phases.phase2_pr_files import Phase2PRFiles
from pr_digger.phases.phase3_pr_reviews import Phase3PRReviews
from pr_digger.rate_limit import RateLimitController
from pr_digger.repository import Repository
from pr_digger.retrying_client import RetryingGitHubApiClient

logger = logging.getLogger(__name__)

PHASE_NAMES = {
    "prs": "pr mining",
    "files": "file mining",
    "reviews": "review mining",
}


class MiningOrchestrator:
    def __init__(
        self,
        repos: list[str],
        base_client: GitHubApiClient,
        parser: PayloadParser,
        checkpoint: FileCheckpointStore,
        db_path: Path,
        per_page: int = 100,
        pr_earliest_date: str | None = None,
        max_retry_delay: int = 60,
    ):
        self._repos = repos
        self._base_client = base_client
        self._parser = parser
        self._checkpoint = checkpoint
        self._db_path = db_path
        self._per_page = per_page
        self._pr_earliest_date = pr_earliest_date
        self._max_retry_delay = max_retry_delay

    def run(self, phases: list[str]) -> None:
        want_prs = "prs" in phases
        parallel = [p for p in phases if p in ("files", "reviews")]

        if want_prs:
            for repo_full_name in self._repos:
                self._run_phase("prs", repo_full_name=repo_full_name)

        if not parallel:
            return

        for repo_full_name in self._repos:
            github_repo_id = self._lookup_repo(repo_full_name)
            if github_repo_id is None:
                logger.warning("Repo %s not found in DB, skipping file/review mining", repo_full_name)
                continue

            logger.info("Processing %s", repo_full_name)
            if len(parallel) > 1:
                self._run_parallel(parallel, github_repo_id)
            else:
                self._run_phase(parallel[0], github_repo_id=github_repo_id)

    def _lookup_repo(self, repo_full_name: str) -> int | None:
        repo = Repository(self._db_path)
        try:
            owner, name = repo_full_name.split("/", 1)
            return repo.get_repository_id(owner, name)
        finally:
            repo.close()

    def _create_resources(self) -> tuple[Repository, RetryingGitHubApiClient]:
        repo = Repository(self._db_path)
        controller = RateLimitController(self._max_retry_delay)
        client = RetryingGitHubApiClient(self._base_client, controller)
        return repo, client

    def _build_phase(
        self, key: str, repo: Repository, client: GitHubApiClient,
        repo_full_name: str | None = None, github_repo_id: int | None = None,
    ) -> object:
        if key == "prs":
            return Phase1PRMetadata(
                repo_full_name=repo_full_name or "",
                api_client=client,
                repository=repo,
                parser=self._parser,
                checkpoint=self._checkpoint,
                per_page=self._per_page,
                earliest_date=self._pr_earliest_date,
            )
        if key == "files":
            return Phase2PRFiles(
                api_client=client,
                repository=repo,
                parser=self._parser,
                github_repo_id=github_repo_id or 0,
            )
        if key == "reviews":
            return Phase3PRReviews(
                api_client=client,
                repository=repo,
                parser=self._parser,
                github_repo_id=github_repo_id or 0,
                per_page=self._per_page,
            )
        raise ValueError(f"Unknown phase: {key}")

    def _run_phase(self, key: str, repo_full_name: str | None = None, github_repo_id: int | None = None) -> None:
        name = PHASE_NAMES[key]
        repo, client = self._create_resources()
        try:
            logger.info("Starting %s", name)
            phase = self._build_phase(key, repo, client, repo_full_name=repo_full_name, github_repo_id=github_repo_id)
            phase.execute()
            logger.info("Finished %s", name)
        finally:
            repo.close()

    def _run_parallel(self, keys: list[str], github_repo_id: int) -> None:
        errors: list[tuple[str, Exception]] = []

        def target(key: str) -> None:
            try:
                self._run_phase(key, github_repo_id=github_repo_id)
            except Exception as exc:
                logger.error("%s failed: %s", PHASE_NAMES[key], exc, exc_info=True)
                errors.append((key, exc))

        threads = [threading.Thread(target=target, args=(k,), name=PHASE_NAMES[k]) for k in keys]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            for key, exc in errors:
                logger.error("%s failed: %s", PHASE_NAMES[key], exc)
            raise errors[0][1]
