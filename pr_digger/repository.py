from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS repositories (
    github_repo_id INTEGER PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    UNIQUE(owner, name)
);

CREATE TABLE IF NOT EXISTS users (
    github_user_id INTEGER PRIMARY KEY,
    login TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pull_requests (
    github_pr_id INTEGER PRIMARY KEY,
    github_repo_id INTEGER NOT NULL,
    number INTEGER NOT NULL,
    author_github_user_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT,
    merged_at TEXT,
    closed_at TEXT,
    files_synced INTEGER NOT NULL DEFAULT 0,
    reviews_synced INTEGER NOT NULL DEFAULT 0,
    UNIQUE(github_repo_id, number),
    FOREIGN KEY (github_repo_id) REFERENCES repositories(github_repo_id),
    FOREIGN KEY (author_github_user_id) REFERENCES users(github_user_id)
);

CREATE TABLE IF NOT EXISTS files (
    github_repo_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (github_repo_id, path),
    FOREIGN KEY (github_repo_id) REFERENCES repositories(github_repo_id)
);

CREATE TABLE IF NOT EXISTS pull_request_files (
    github_pr_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    PRIMARY KEY (github_pr_id, file_path),
    FOREIGN KEY (github_pr_id) REFERENCES pull_requests(github_pr_id)
);

CREATE TABLE IF NOT EXISTS pull_request_reviews (
    github_review_id INTEGER PRIMARY KEY,
    github_pr_id INTEGER NOT NULL,
    reviewer_github_user_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    submitted_at TEXT,
    FOREIGN KEY (github_pr_id) REFERENCES pull_requests(github_pr_id),
    FOREIGN KEY (reviewer_github_user_id) REFERENCES users(github_user_id)
);
"""


@dataclass
class RepoRecord:
    github_repo_id: int
    owner: str
    name: str


@dataclass
class UserRecord:
    github_user_id: int
    login: str


@dataclass
class PullRequestRecord:
    github_pr_id: int
    github_repo_id: int
    number: int
    author_github_user_id: int
    state: str
    created_at: str | None
    merged_at: str | None
    closed_at: str | None


@dataclass
class FileRecord:
    github_repo_id: int
    path: str


@dataclass
class PullRequestFileRecord:
    github_pr_id: int
    file_path: str


@dataclass
class ReviewRecord:
    github_review_id: int
    github_pr_id: int
    reviewer_github_user_id: int
    state: str
    submitted_at: str | None


@dataclass
class PendingPR:
    github_pr_id: int
    github_repo_id: int
    number: int
    repo_owner: str
    repo_name: str


class Repository:
    def __init__(self, db_path: Path, write_lock: threading.Lock | None = None):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()
        self._write_lock = write_lock or threading.Lock()

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def transaction(self):
        with self._write_lock:
            try:
                yield
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def upsert_repository(self, rec: RepoRecord) -> int:
        self._conn.execute(
            "INSERT INTO repositories (github_repo_id, owner, name, full_name) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(github_repo_id) DO UPDATE SET"
            " owner=excluded.owner, name=excluded.name, full_name=excluded.full_name",
            (rec.github_repo_id, rec.owner, rec.name, f"{rec.owner}/{rec.name}"),
        )
        return rec.github_repo_id

    def get_repository_id(self, owner: str, name: str) -> int | None:
        row = self._conn.execute(
            "SELECT github_repo_id FROM repositories WHERE owner=? AND name=?",
            (owner, name),
        ).fetchone()
        return row[0] if row else None

    def upsert_user(self, rec: UserRecord) -> None:
        self._conn.execute(
            "INSERT INTO users (github_user_id, login) VALUES (?, ?)"
            " ON CONFLICT(github_user_id) DO UPDATE SET login=excluded.login",
            (rec.github_user_id, rec.login),
        )

    def upsert_pull_request(self, rec: PullRequestRecord) -> None:
        self._conn.execute(
            "INSERT INTO pull_requests"
            " (github_pr_id, github_repo_id, number, author_github_user_id,"
            " state, created_at, merged_at, closed_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(github_pr_id) DO UPDATE SET"
            " state=excluded.state,"
            " created_at=excluded.created_at,"
            " merged_at=excluded.merged_at,"
            " closed_at=excluded.closed_at",
            (rec.github_pr_id, rec.github_repo_id, rec.number,
             rec.author_github_user_id, rec.state,
             rec.created_at, rec.merged_at, rec.closed_at),
        )

    def upsert_file(self, rec: FileRecord) -> None:
        self._conn.execute(
            "INSERT INTO files (github_repo_id, path) VALUES (?, ?)"
            " ON CONFLICT(github_repo_id, path) DO NOTHING",
            (rec.github_repo_id, rec.path),
        )

    def upsert_pull_request_file(self, rec: PullRequestFileRecord) -> None:
        self._conn.execute(
            "INSERT INTO pull_request_files (github_pr_id, file_path) VALUES (?, ?)"
            " ON CONFLICT(github_pr_id, file_path) DO NOTHING",
            (rec.github_pr_id, rec.file_path),
        )

    def upsert_review(self, rec: ReviewRecord) -> None:
        self._conn.execute(
            "INSERT INTO pull_request_reviews"
            " (github_review_id, github_pr_id, reviewer_github_user_id, state, submitted_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(github_review_id) DO UPDATE SET"
            " state=excluded.state,"
            " submitted_at=excluded.submitted_at",
            (rec.github_review_id, rec.github_pr_id,
             rec.reviewer_github_user_id, rec.state, rec.submitted_at),
        )

    def list_prs_pending_files(self, github_repo_id: int, limit: int = 100) -> list[PendingPR]:
        rows = self._conn.execute(
            "SELECT pr.github_pr_id, pr.github_repo_id, pr.number, r.owner, r.name"
            " FROM pull_requests pr"
            " JOIN repositories r ON r.github_repo_id = pr.github_repo_id"
            " WHERE pr.files_synced = 0 AND pr.github_repo_id = ?"
            " ORDER BY pr.github_pr_id"
            " LIMIT ?",
            (github_repo_id, limit),
        ).fetchall()
        return [PendingPR(*row) for row in rows]

    def list_prs_pending_reviews(self, github_repo_id: int, limit: int = 100) -> list[PendingPR]:
        rows = self._conn.execute(
            "SELECT pr.github_pr_id, pr.github_repo_id, pr.number, r.owner, r.name"
            " FROM pull_requests pr"
            " JOIN repositories r ON r.github_repo_id = pr.github_repo_id"
            " WHERE pr.reviews_synced = 0 AND pr.github_repo_id = ?"
            " ORDER BY pr.github_pr_id"
            " LIMIT ?",
            (github_repo_id, limit),
        ).fetchall()
        return [PendingPR(*row) for row in rows]

    def count_prs(self, github_repo_id: int) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pull_requests WHERE github_repo_id = ?",
            (github_repo_id,),
        ).fetchone()
        return row[0]

    def count_prs_pending_files(self, github_repo_id: int) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pull_requests WHERE files_synced = 0 AND github_repo_id = ?",
            (github_repo_id,),
        ).fetchone()
        return row[0]

    def count_prs_pending_reviews(self, github_repo_id: int) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pull_requests WHERE reviews_synced = 0 AND github_repo_id = ?",
            (github_repo_id,),
        ).fetchone()
        return row[0]

    def get_latest_pr_created_at(self, github_repo_id: int) -> str | None:
        row = self._conn.execute(
            "SELECT MAX(created_at) FROM pull_requests WHERE github_repo_id = ?",
            (github_repo_id,),
        ).fetchone()
        return row[0] if row else None

    def pr_exists(self, github_repo_id: int, number: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM pull_requests WHERE github_repo_id = ? AND number = ?",
            (github_repo_id, number),
        ).fetchone()
        return row is not None

    def mark_pr_files_synced(self, github_pr_id: int) -> None:
        self._conn.execute(
            "UPDATE pull_requests SET files_synced = 1 WHERE github_pr_id = ?",
            (github_pr_id,),
        )

    def mark_pr_reviews_synced(self, github_pr_id: int) -> None:
        self._conn.execute(
            "UPDATE pull_requests SET reviews_synced = 1 WHERE github_pr_id = ?",
            (github_pr_id,),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
