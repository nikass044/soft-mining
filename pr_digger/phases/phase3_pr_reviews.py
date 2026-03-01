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
        per_page: int = 100,
        batch_size: int = 100,
    ):
        self._api_client = api_client
        self._repository = repository
        self._parser = parser
        self._per_page = per_page
        self._batch_size = batch_size

    def execute(self) -> None:
        total = self._repository.count_prs_pending_reviews()
        done = 0
        logger.info("review mining: %d PRs pending", total)

        while True:
            pending = self._repository.list_prs_pending_reviews(limit=self._batch_size)
            if not pending:
                break

            for pr in pending:
                done += 1
                pct = done * 100 // total if total else 0
                logger.info("review mining: %s/%s#%d (%d/%d %d%%)", pr.repo_owner, pr.repo_name, pr.number, done, total, pct)
                self._ingest_reviews_for_pr(pr.pr_id, pr.repo_owner, pr.repo_name, pr.number)

        logger.info("review mining: complete")

    def _ingest_reviews_for_pr(self, pr_id: int, owner: str, name: str, number: int) -> None:
        page = 1
        while True:
            payload = self._api_client.get_rest(
                f"/repos/{owner}/{name}/pulls/{number}/reviews",
                params={"per_page": self._per_page, "page": page},
            )
            if not payload:
                break

            batch = self._parser.parse_pr_reviews(payload, pr_id)
            for user_rec, review_rec in zip(batch.users, batch.reviews):
                user_id = self._repository.upsert_user(user_rec)
                review_rec.reviewer_user_id = user_id
                self._repository.upsert_review(review_rec)

            if len(payload) < self._per_page:
                break
            page += 1

        self._repository.mark_pr_reviews_synced(pr_id)
        self._repository.commit()
