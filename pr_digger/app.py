from __future__ import annotations

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


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    config = Config.from_env()
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
        logger.info("Starting pr-digger with repos=%s phases=%s", config.repos, config.phases)
        orchestrator.run(config.phases)
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
