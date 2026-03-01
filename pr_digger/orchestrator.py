from __future__ import annotations

import logging
import threading
from pathlib import Path

from pr_digger.api_client import BaseGitHubApiClient, GitHubApiClient
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
        if "prs" in phases:
            self._run_phase("prs")

        parallel = [p for p in phases if p in ("files", "reviews")]
        if len(parallel) > 1:
            self._run_parallel(parallel)
        elif parallel:
            self._run_phase(parallel[0])

    def _create_resources(self) -> tuple[Repository, RetryingGitHubApiClient]:
        repo = Repository(self._db_path)
        controller = RateLimitController(self._max_retry_delay)
        client = RetryingGitHubApiClient(self._base_client, controller)
        return repo, client

    def _build_phase(self, key: str, repo: Repository, client: GitHubApiClient) -> object:
        if key == "prs":
            return Phase1PRMetadata(
                repos=self._repos,
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
            )
        if key == "reviews":
            return Phase3PRReviews(
                api_client=client,
                repository=repo,
                parser=self._parser,
                per_page=self._per_page,
            )
        raise ValueError(f"Unknown phase: {key}")

    def _run_phase(self, key: str) -> None:
        name = PHASE_NAMES[key]
        repo, client = self._create_resources()
        try:
            logger.info("Starting %s", name)
            phase = self._build_phase(key, repo, client)
            phase.execute()
            logger.info("Finished %s", name)
        finally:
            repo.close()

    def _run_parallel(self, keys: list[str]) -> None:
        errors: list[tuple[str, Exception]] = []

        def target(key: str) -> None:
            try:
                self._run_phase(key)
            except Exception as exc:
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
