from __future__ import annotations

import logging

from pr_digger.api_client import GitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases import MiningPhase
from pr_digger.phases.phase1_pr_metadata import Phase1PRMetadata
from pr_digger.phases.phase2_pr_files import Phase2PRFiles
from pr_digger.phases.phase3_pr_reviews import Phase3PRReviews
from pr_digger.repository import Repository

logger = logging.getLogger(__name__)


class PhaseOrchestrator:
    def __init__(
        self,
        repos: list[str],
        api_client: GitHubApiClient,
        repository: Repository,
        parser: PayloadParser,
        checkpoint: FileCheckpointStore,
        per_page: int = 100,
    ):
        self._phase_builders: dict[str, callable] = {
            "1": lambda: Phase1PRMetadata(
                repos=repos,
                api_client=api_client,
                repository=repository,
                parser=parser,
                checkpoint=checkpoint,
                per_page=per_page,
            ),
            "2": lambda: Phase2PRFiles(
                api_client=api_client,
                repository=repository,
                parser=parser,
            ),
            "3": lambda: Phase3PRReviews(
                api_client=api_client,
                repository=repository,
                parser=parser,
                per_page=per_page,
            ),
        }

    def run(self, phases: list[str]) -> None:
        for phase_key in phases:
            builder = self._phase_builders.get(phase_key)
            if builder is None:
                logger.warning("Unknown phase: %s, skipping", phase_key)
                continue

            logger.info("Starting phase %s", phase_key)
            phase: MiningPhase = builder()
            phase.execute()
            logger.info("Finished phase %s", phase_key)
