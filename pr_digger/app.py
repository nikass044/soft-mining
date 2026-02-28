from __future__ import annotations

import argparse
import logging
import sys

from pr_digger.api_client import BaseGitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.config import Config
from pr_digger.orchestrator import PhaseOrchestrator
from pr_digger.parser import PayloadParser
from pr_digger.rate_limit import RateLimitController
from pr_digger.repository import Repository
from pr_digger.retrying_client import RetryingGitHubApiClient
from pr_digger.transport import HttpTransport


def parse_args(argv: list[str] | None = None) -> list[str]:
    parser = argparse.ArgumentParser(description="Mine GitHub PR data into SQLite")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run all phases (1, 2, 3)")
    group.add_argument("--phase1", action="store_true", help="Phase 1: PR metadata")
    group.add_argument("--phase2", action="store_true", help="Phase 2: PR files (GraphQL)")
    group.add_argument("--phase3", action="store_true", help="Phase 3: PR reviews")

    args = parser.parse_args(argv)

    if args.all:
        return ["1", "2", "3"]
    if args.phase1:
        return ["1"]
    if args.phase2:
        return ["2"]
    return ["3"]


def main(argv: list[str] | None = None) -> int:
    phases = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    config = Config.load()
    if not config.github_token:
        logger.error("GITHUB_TOKEN is required")
        return 1

    transport = HttpTransport(config.github_token)
    base_client = BaseGitHubApiClient(transport)
    controller = RateLimitController(
        max_retry_delay=config.max_retry_delay,
        max_retries=config.max_retries,
    )
    api_client = RetryingGitHubApiClient(base_client, controller)

    repository = Repository(config.db_path)
    checkpoint = FileCheckpointStore(config.checkpoint_dir)
    parser = PayloadParser()

    orchestrator = PhaseOrchestrator(
        repos=config.repos,
        api_client=api_client,
        repository=repository,
        parser=parser,
        checkpoint=checkpoint,
        per_page=config.rest_per_page,
    )

    try:
        logger.info("Starting pr-digger with repos=%s phases=%s", config.repos, phases)
        orchestrator.run(phases)
        logger.info("All phases complete")
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception:
        logger.exception("Fatal error")
        return 1
    finally:
        repository.close()
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
