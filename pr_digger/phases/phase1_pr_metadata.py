from __future__ import annotations

import logging

from pr_digger.api_client import GitHubApiClient
from pr_digger.checkpoint import FileCheckpointStore
from pr_digger.parser import PayloadParser
from pr_digger.phases import MiningPhase
from pr_digger.repository import RepoRecord, Repository

logger = logging.getLogger(__name__)


class Phase1PRMetadata(MiningPhase):
    def __init__(
        self,
        repo_full_name: str,
        api_client: GitHubApiClient,
        repository: Repository,
        parser: PayloadParser,
        checkpoint: FileCheckpointStore,
        per_page: int = 100,
        earliest_date: str | None = None,
    ):
        self._repo_full_name = repo_full_name
        self._api_client = api_client
        self._repository = repository
        self._parser = parser
        self._checkpoint = checkpoint
        self._per_page = per_page
        self._earliest_date = earliest_date

    def execute(self) -> None:
        state = self._checkpoint.load("phase1") or {}

        owner, name = self._repo_full_name.split("/", 1)
        repo_info = self._api_client.get_rest(f"/repos/{owner}/{name}")
        github_repo_id = repo_info["id"]
        self._repository.upsert_repository(RepoRecord(github_repo_id, owner, name))
        self._repository.commit()

        start_page = state.get(self._repo_full_name, 1)
        logger.info("pr mining: %s starting at page %d", self._repo_full_name, start_page)
        self._ingest_repo(github_repo_id, start_page, state)

        self._checkpoint.clear("phase1")
        logger.info("pr mining: %s complete", self._repo_full_name)

    def _ingest_repo(self, github_repo_id: int, start_page: int, state: dict) -> None:
        latest = self._repository.get_latest_pr_created_at(github_repo_id)
        if latest:
            logger.info("pr mining: %s has PRs up to %s, fetching newest first", self._repo_full_name, latest)
            self._ingest_incremental(github_repo_id, latest)
        else:
            self._ingest_full(github_repo_id, start_page, state)

    def _filter_by_earliest_date(self, payload: list) -> list:
        if not self._earliest_date:
            return payload
        return [pr for pr in payload if pr.get("created_at", "") >= self._earliest_date]

    def _ingest_full(self, github_repo_id: int, start_page: int, state: dict) -> None:
        page = start_page
        while True:
            payload = self._fetch_page(page, direction="asc")
            if not payload:
                break

            filtered = self._filter_by_earliest_date(payload)
            if filtered:
                batch = self._parser.parse_pr_list(filtered, github_repo_id)
                self._persist_batch(batch)

            state[self._repo_full_name] = page + 1
            self._checkpoint.save("phase1", state)

            if len(payload) < self._per_page:
                break
            page += 1

    def _ingest_incremental(self, github_repo_id: int, latest: str) -> None:
        page = 1
        while True:
            payload = self._fetch_page(page, direction="desc")
            if not payload:
                break

            new_prs = [pr for pr in payload if not self._repository.pr_exists(github_repo_id, pr["number"])]
            if not new_prs:
                logger.info("pr mining: %s caught up at page %d", self._repo_full_name, page)
                break

            batch = self._parser.parse_pr_list(new_prs, github_repo_id)
            self._persist_batch(batch)

            if len(new_prs) < len(payload):
                break
            page += 1

    def _fetch_page(self, page: int, direction: str = "asc") -> list:
        logger.info("pr mining: fetching %s page %d (%s)", self._repo_full_name, page, direction)
        return self._api_client.get_rest(
            f"/repos/{self._repo_full_name}/pulls",
            params={
                "state": "all",
                "sort": "created",
                "direction": direction,
                "per_page": self._per_page,
                "page": page,
            },
        )

    def _persist_batch(self, batch) -> None:
        for user_rec, pr_rec in zip(batch.users, batch.pull_requests):
            self._repository.upsert_user(user_rec)
            self._repository.upsert_pull_request(pr_rec)
        self._repository.commit()
