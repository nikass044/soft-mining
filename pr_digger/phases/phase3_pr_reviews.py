from __future__ import annotations

import logging

from pr_digger.api_client import GitHubApiClient
from pr_digger.parser import PayloadParser
from pr_digger.phases import MiningPhase
from pr_digger.repository import Repository

logger = logging.getLogger(__name__)


class Phase3PRReviews(MiningPhase):
    def __init__(
        self,
        api_client: GitHubApiClient,
        repository: Repository,
        parser: PayloadParser,
        github_repo_id: int,
        per_page: int = 100,
        batch_size: int = 100,
    ):
        self._api_client = api_client
        self._repository = repository
        self._parser = parser
        self._github_repo_id = github_repo_id
        self._per_page = per_page
        self._batch_size = batch_size

    def execute(self) -> None:
        total = self._repository.count_prs(self._github_repo_id)
        pending = self._repository.count_prs_pending_reviews(self._github_repo_id)
        done = total - pending
        logger.info("review mining: %d/%d synced, %d pending", done, total, pending)

        while True:
            batch = self._repository.list_prs_pending_reviews(self._github_repo_id, limit=self._batch_size)
            if not batch:
                break

            for pr in batch:
                done += 1
                pct = done * 100 // total if total else 0
                logger.info("review mining: %s/%s#%d (%d/%d %d%%)", pr.repo_owner, pr.repo_name, pr.number, done, total, pct)
                try:
                    self._ingest_reviews_for_pr(pr.github_pr_id, pr.repo_owner, pr.repo_name, pr.number)
                except Exception:
                    logger.exception("review mining: failed on %s/%s#%d, skipping", pr.repo_owner, pr.repo_name, pr.number)
                    try:
                        with self._repository.transaction():
                            self._repository.mark_pr_reviews_synced(pr.github_pr_id)
                    except Exception:
                        logger.exception("review mining: failed to mark %s/%s#%d as synced", pr.repo_owner, pr.repo_name, pr.number)

        logger.info("review mining: complete")

    def _ingest_reviews_for_pr(self, github_pr_id: int, owner: str, name: str, number: int) -> None:
        page = 1
        while True:
            payload = self._api_client.get_rest(
                f"/repos/{owner}/{name}/pulls/{number}/reviews",
                params={"per_page": self._per_page, "page": page},
            )
            if not payload:
                break

            batch = self._parser.parse_pr_reviews(payload, github_pr_id)
            with self._repository.transaction():
                for user_rec, review_rec in zip(batch.users, batch.reviews):
                    self._repository.upsert_user(user_rec)
                    self._repository.upsert_review(review_rec)

            if len(payload) < self._per_page:
                break
            page += 1

        with self._repository.transaction():
            self._repository.mark_pr_reviews_synced(github_pr_id)
