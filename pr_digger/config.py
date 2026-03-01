from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

DB_PATH = Path("data/pr_digger.db")
CHECKPOINT_DIR = Path("data/checkpoints")
REPOS = [
    # "facebook/react",
    "neovim/neovim",
    "freeCodeCamp/freeCodeCamp",
    "vuejs/vue",
    "tensorflow/tensorflow",
    "microsoft/vscode",
    "flutter/flutter",
    "twbs/bootstrap",
    "vercel/next.js",
    "golang/go",
    "facebook/react-native",
    "electron/electron",
    "kubernetes/kubernetes",
    "nodejs/node",
]
GITHUB_TOKEN = "GITHUB_TOKEN"
PR_EARLIEST_DATE = "2017-01-01T00:00:00Z"


@dataclass(frozen=True)
class Config:
    repos: list[str]
    github_token: str
    db_path: Path
    checkpoint_dir: Path
    pr_earliest_date: str = PR_EARLIEST_DATE
    rest_per_page: int = 100
    max_retry_delay: int = 60

    @classmethod
    def load(cls) -> Config:
        load_dotenv()

        return cls(
            repos=list(REPOS),
            github_token=os.environ.get(GITHUB_TOKEN, ""),
            db_path=DB_PATH,
            checkpoint_dir=CHECKPOINT_DIR,
        )
