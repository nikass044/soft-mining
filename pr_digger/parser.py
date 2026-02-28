from __future__ import annotations

from dataclasses import dataclass

from pr_digger.repository import (
    PullRequestRecord,
    ReviewRecord,
    UserRecord,
)


@dataclass
class ParsedPRBatch:
    users: list[UserRecord]
    pull_requests: list[PullRequestRecord]


@dataclass
class ParsedFilesBatch:
    file_paths: list[str]


@dataclass
class ParsedReviewsBatch:
    users: list[UserRecord]
    reviews: list[ReviewRecord]


class PayloadParser:
    def parse_pr_list(self, payload: list[dict], repo_id: int) -> ParsedPRBatch:
        users: list[UserRecord] = []
        prs: list[PullRequestRecord] = []

        for item in payload:
            user_data = item.get("user") or {}
            github_user_id = user_data.get("id", 0)
            login = user_data.get("login", "unknown")

            users.append(UserRecord(github_user_id=github_user_id, login=login))
            prs.append(PullRequestRecord(
                repo_id=repo_id,
                number=item["number"],
                author_user_id=github_user_id,
                state=item["state"],
                created_at=item.get("created_at"),
                merged_at=item.get("merged_at"),
                closed_at=item.get("closed_at"),
            ))

        return ParsedPRBatch(users=users, pull_requests=prs)

    def parse_pr_files(self, payload: dict, repo_id: int, pull_request_id: int) -> ParsedFilesBatch:
        pr_node = payload.get("data", {}).get("repository", {}).get("pullRequest", {})
        files_conn = pr_node.get("files", {})
        nodes = files_conn.get("nodes", [])
        return ParsedFilesBatch(file_paths=[n["path"] for n in nodes])

    def parse_pr_files_page_info(self, payload: dict) -> tuple[bool, str | None]:
        pr_node = payload.get("data", {}).get("repository", {}).get("pullRequest", {})
        page_info = pr_node.get("files", {}).get("pageInfo", {})
        return page_info.get("hasNextPage", False), page_info.get("endCursor")

    def parse_pr_reviews(self, payload: list[dict], pull_request_id: int) -> ParsedReviewsBatch:
        users: list[UserRecord] = []
        reviews: list[ReviewRecord] = []

        for item in payload:
            user_data = item.get("user") or {}
            github_user_id = user_data.get("id", 0)
            login = user_data.get("login", "unknown")

            users.append(UserRecord(github_user_id=github_user_id, login=login))
            reviews.append(ReviewRecord(
                github_review_id=item["id"],
                pull_request_id=pull_request_id,
                reviewer_user_id=github_user_id,
                state=item.get("state", "COMMENTED"),
                submitted_at=item.get("submitted_at"),
            ))

        return ParsedReviewsBatch(users=users, reviews=reviews)
