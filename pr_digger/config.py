from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

GITHUB_TOKEN = ""
DB_PATH = Path("data/pr_digger.db")
CHECKPOINT_DIR = Path("data/checkpoints")
REPOS = ["facebook/react"]


@dataclass(frozen=True)
class Config:
    repos: list[str]
    github_token: str
    db_path: Path
    checkpoint_dir: Path
    rest_per_page: int = 100
    max_retry_delay: int = 60
    max_retries: int = 10

    @classmethod
    def load(cls) -> Config:
        return cls(
            repos=list(REPOS),
            github_token=GITHUB_TOKEN,
            db_path=DB_PATH,
            checkpoint_dir=CHECKPOINT_DIR,
        )
