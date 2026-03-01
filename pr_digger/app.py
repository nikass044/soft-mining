from __future__ import annotations

import argparse
import logging
import sys

from pr_digger.api_client import BaseGitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.config import Config
from pr_digger.orchestrator import MiningOrchestrator
from pr_digger.parser import PayloadParser
from pr_digger.transport import HttpTransport


def parse_args(argv: list[str] | None = None) -> list[str]:
    parser = argparse.ArgumentParser(description="Mine GitHub PR data into SQLite")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run all mining tasks")
    group.add_argument("--prs", action="store_true", help="Mine PR metadata and users")
    group.add_argument("--files", action="store_true", help="Mine PR files (GraphQL)")
    group.add_argument("--reviews", action="store_true", help="Mine PR reviews")

    args = parser.parse_args(argv)

    if args.all:
        return ["prs", "files", "reviews"]
    if args.prs:
        return ["prs"]
    if args.files:
        return ["files"]
    return ["reviews"]


def main(argv: list[str] | None = None) -> int:
    phases = parse_args(argv)

    log_format = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt="%H:%M:%S",
    )

    file_handler = logging.FileHandler("pr_digger.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S"))
    logging.getLogger().addHandler(file_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    config = Config.load()
    if not config.github_token:
        logger.error("GITHUB_TOKEN is required")
        return 1

    transport = HttpTransport(config.github_token)
    base_client = BaseGitHubApiClient(transport)
    checkpoint = FileCheckpointStore(config.checkpoint_dir)
    parser = PayloadParser()

    orchestrator = MiningOrchestrator(
        repos=config.repos,
        base_client=base_client,
        parser=parser,
        checkpoint=checkpoint,
        db_path=config.db_path,
        per_page=config.rest_per_page,
        pr_earliest_date=config.pr_earliest_date,
        max_retry_delay=config.max_retry_delay,
    )

    try:
        logger.info("Starting pr-digger with repos=%s tasks=%s", config.repos, phases)
        orchestrator.run(phases)
        logger.info("All tasks complete")
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception:
        logger.exception("Fatal error")
        return 1
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
