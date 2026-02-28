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
        batch_size: int = 100,
    ):
        self._api_client = api_client
        self._repository = repository
        self._parser = parser
        self._batch_size = batch_size

    def execute(self) -> None:
        while True:
            pending = self._repository.list_prs_pending_files(limit=self._batch_size)
            if not pending:
                break

            for pr in pending:
                logger.info("Phase2: fetching files for %s/%s#%d", pr.repo_owner, pr.repo_name, pr.number)
                self._ingest_files_for_pr(pr.pr_id, pr.repo_id, pr.repo_owner, pr.repo_name, pr.number)

        logger.info("Phase2: complete")

    def _ingest_files_for_pr(
        self, pr_id: int, repo_id: int, owner: str, name: str, number: int
    ) -> None:
        cursor = None
        while True:
            payload = self._api_client.post_graphql(
                PR_FILES_QUERY,
                {"owner": owner, "name": name, "number": number, "after": cursor},
            )

            batch = self._parser.parse_pr_files(payload, repo_id, pr_id)
            for path in batch.file_paths:
                file_id = self._repository.upsert_file(FileRecord(repo_id, path))
                self._repository.upsert_pull_request_file(PullRequestFileRecord(pr_id, file_id))

            has_next, cursor = self._parser.parse_pr_files_page_info(payload)
            if not has_next:
                break

        self._repository.mark_pr_files_synced(pr_id)
        self._repository.commit()
