from __future__ import annotations

import logging

from pr_digger.api_client import GitHubApiClient
from pr_digger.parser import PayloadParser
from pr_digger.phases import MiningPhase
from pr_digger.repository import FileRecord, PullRequestFileRecord, Repository

logger = logging.getLogger(__name__)

PR_FILES_QUERY = """
query($owner:String!, $name:String!, $number:Int!, $after:String) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
      number
      author {
        __typename
        ... on User { id login }
        ... on Bot { id login }
        ... on Mannequin { id login }
      }
      files(first:100, after:$after) {
        pageInfo { hasNextPage endCursor }
        nodes { path }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}
"""


class Phase2PRFiles(MiningPhase):
    def __init__(
        self,
        api_client: GitHubApiClient,
        repository: Repository,
        parser: PayloadParser,
        github_repo_id: int,
        batch_size: int = 100,
    ):
        self._api_client = api_client
        self._repository = repository
        self._parser = parser
        self._github_repo_id = github_repo_id
        self._batch_size = batch_size

    def execute(self) -> None:
        total = self._repository.count_prs(self._github_repo_id)
        pending = self._repository.count_prs_pending_files(self._github_repo_id)
        done = total - pending
        logger.info("file mining: %d/%d synced, %d pending", done, total, pending)

        while True:
            batch = self._repository.list_prs_pending_files(self._github_repo_id, limit=self._batch_size)
            if not batch:
                break

            for pr in batch:
                done += 1
                pct = done * 100 // total if total else 0
                logger.info("file mining: %s/%s#%d (%d/%d %d%%)", pr.repo_owner, pr.repo_name, pr.number, done, total, pct)
                try:
                    self._ingest_files_for_pr(pr.github_pr_id, pr.github_repo_id, pr.repo_owner, pr.repo_name, pr.number)
                except Exception:
                    logger.exception("file mining: failed on %s/%s#%d, skipping", pr.repo_owner, pr.repo_name, pr.number)
                    try:
                        with self._repository.transaction():
                            self._repository.mark_pr_files_synced(pr.github_pr_id)
                    except Exception:
                        logger.exception("file mining: failed to mark %s/%s#%d as synced", pr.repo_owner, pr.repo_name, pr.number)

        logger.info("file mining: complete")

    def _ingest_files_for_pr(
        self, github_pr_id: int, github_repo_id: int, owner: str, name: str, number: int
    ) -> None:
        cursor = None
        while True:
            payload = self._api_client.post_graphql(
                PR_FILES_QUERY,
                {"owner": owner, "name": name, "number": number, "after": cursor},
            )

            batch = self._parser.parse_pr_files(payload)
            with self._repository.transaction():
                for path in batch.file_paths:
                    self._repository.upsert_file(FileRecord(github_repo_id, path))
                    self._repository.upsert_pull_request_file(PullRequestFileRecord(github_pr_id, path))

            has_next, cursor = self._parser.parse_pr_files_page_info(payload)
            if not has_next:
                break

        with self._repository.transaction():
            self._repository.mark_pr_files_synced(github_pr_id)
