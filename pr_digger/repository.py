from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    UNIQUE(owner, name)
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    github_user_id INTEGER NOT NULL,
    login TEXT NOT NULL,
    UNIQUE(github_user_id)
);

CREATE TABLE IF NOT EXISTS pull_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id INTEGER NOT NULL,
    number INTEGER NOT NULL,
    author_user_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT,
    merged_at TEXT,
    closed_at TEXT,
    files_synced INTEGER NOT NULL DEFAULT 0,
    reviews_synced INTEGER NOT NULL DEFAULT 0,
    UNIQUE(repo_id, number),
    FOREIGN KEY (repo_id) REFERENCES repositories(id),
    FOREIGN KEY (author_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    UNIQUE(repo_id, path),
    FOREIGN KEY (repo_id) REFERENCES repositories(id)
);

CREATE TABLE IF NOT EXISTS pull_request_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pull_request_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    UNIQUE(pull_request_id, file_id),
    FOREIGN KEY (pull_request_id) REFERENCES pull_requests(id),
    FOREIGN KEY (file_id) REFERENCES files(id)
);

CREATE TABLE IF NOT EXISTS pull_request_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    github_review_id INTEGER NOT NULL,
    pull_request_id INTEGER NOT NULL,
    reviewer_user_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    submitted_at TEXT,
    UNIQUE(github_review_id),
    FOREIGN KEY (pull_request_id) REFERENCES pull_requests(id),
    FOREIGN KEY (reviewer_user_id) REFERENCES users(id)
);
"""


@dataclass
class RepoRecord:
    owner: str
    name: str


@dataclass
class UserRecord:
    github_user_id: int
    login: str


@dataclass
class PullRequestRecord:
    repo_id: int
    number: int
    author_user_id: int
    state: str
    created_at: str | None
    merged_at: str | None
    closed_at: str | None


@dataclass
class FileRecord:
    repo_id: int
    path: str


@dataclass
class PullRequestFileRecord:
    pull_request_id: int
    file_id: int


@dataclass
class ReviewRecord:
    github_review_id: int
    pull_request_id: int
    reviewer_user_id: int
    state: str
    submitted_at: str | None


@dataclass
class PendingPR:
    pr_id: int
    repo_id: int
    number: int
    repo_owner: str
    repo_name: str


class Repository:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def upsert_repository(self, rec: RepoRecord) -> int:
        self._conn.execute(
            "INSERT INTO repositories (owner, name, full_name) VALUES (?, ?, ?)"
            " ON CONFLICT(owner, name) DO UPDATE SET full_name=excluded.full_name",
            (rec.owner, rec.name, f"{rec.owner}/{rec.name}"),
        )
        row = self._conn.execute(
            "SELECT id FROM repositories WHERE owner=? AND name=?",
            (rec.owner, rec.name),
        ).fetchone()
        return row[0]

    def upsert_user(self, rec: UserRecord) -> int:
        self._conn.execute(
            "INSERT INTO users (github_user_id, login) VALUES (?, ?)"
            " ON CONFLICT(github_user_id) DO UPDATE SET login=excluded.login",
            (rec.github_user_id, rec.login),
        )
        row = self._conn.execute(
            "SELECT id FROM users WHERE github_user_id=?",
            (rec.github_user_id,),
        ).fetchone()
        return row[0]

    def upsert_pull_request(self, rec: PullRequestRecord) -> int:
        self._conn.execute(
            "INSERT INTO pull_requests (repo_id, number, author_user_id, state, created_at, merged_at, closed_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(repo_id, number) DO UPDATE SET"
            " author_user_id=excluded.author_user_id,"
            " state=excluded.state,"
            " created_at=excluded.created_at,"
            " merged_at=excluded.merged_at,"
            " closed_at=excluded.closed_at",
            (rec.repo_id, rec.number, rec.author_user_id, rec.state,
             rec.created_at, rec.merged_at, rec.closed_at),
        )
        row = self._conn.execute(
            "SELECT id FROM pull_requests WHERE repo_id=? AND number=?",
            (rec.repo_id, rec.number),
        ).fetchone()
        return row[0]

    def upsert_file(self, rec: FileRecord) -> int:
        self._conn.execute(
            "INSERT INTO files (repo_id, path) VALUES (?, ?)"
            " ON CONFLICT(repo_id, path) DO NOTHING",
            (rec.repo_id, rec.path),
        )
        row = self._conn.execute(
            "SELECT id FROM files WHERE repo_id=? AND path=?",
            (rec.repo_id, rec.path),
        ).fetchone()
        return row[0]

    def upsert_pull_request_file(self, rec: PullRequestFileRecord) -> None:
        self._conn.execute(
            "INSERT INTO pull_request_files (pull_request_id, file_id) VALUES (?, ?)"
            " ON CONFLICT(pull_request_id, file_id) DO NOTHING",
            (rec.pull_request_id, rec.file_id),
        )

    def upsert_review(self, rec: ReviewRecord) -> None:
        self._conn.execute(
            "INSERT INTO pull_request_reviews"
            " (github_review_id, pull_request_id, reviewer_user_id, state, submitted_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(github_review_id) DO UPDATE SET"
            " state=excluded.state,"
            " submitted_at=excluded.submitted_at",
            (rec.github_review_id, rec.pull_request_id,
             rec.reviewer_user_id, rec.state, rec.submitted_at),
        )

    def list_prs_pending_files(self, limit: int = 100) -> list[PendingPR]:
        rows = self._conn.execute(
            "SELECT pr.id, pr.repo_id, pr.number, r.owner, r.name"
            " FROM pull_requests pr"
            " JOIN repositories r ON r.id = pr.repo_id"
            " WHERE pr.files_synced = 0"
            " ORDER BY pr.id"
            " LIMIT ?",
            (limit,),
        ).fetchall()
        return [PendingPR(*row) for row in rows]

    def list_prs_pending_reviews(self, limit: int = 100) -> list[PendingPR]:
        rows = self._conn.execute(
            "SELECT pr.id, pr.repo_id, pr.number, r.owner, r.name"
            " FROM pull_requests pr"
            " JOIN repositories r ON r.id = pr.repo_id"
            " WHERE pr.reviews_synced = 0"
            " ORDER BY pr.id"
            " LIMIT ?",
            (limit,),
        ).fetchall()
        return [PendingPR(*row) for row in rows]

    def mark_pr_files_synced(self, pr_id: int) -> None:
        self._conn.execute(
            "UPDATE pull_requests SET files_synced = 1 WHERE id = ?",
            (pr_id,),
        )

    def mark_pr_reviews_synced(self, pr_id: int) -> None:
        self._conn.execute(
            "UPDATE pull_requests SET reviews_synced = 1 WHERE id = ?",
            (pr_id,),
        )

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
