from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

DB_PATH = Path("data/pr_digger.db")
CHECKPOINT_DIR = Path("data/checkpoints")
REPOS = [
    # # --- Chunk 1 (sum 152,481) --- Nik
    # "flutter/flutter",
    # "nodejs/node",
    # "electron/electron",
    # "facebook/react",
    # "twbs/bootstrap",
    #
    # # --- Chunk 2 (sum 152,507) --- Julian
    # "microsoft/vscode",
    # "freeCodeCamp/freeCodeCamp",
    # "vercel/next.js",
    # "facebook/react-native",
    # "golang/go",
    #
    # # --- Chunk 3 (sum 152,860) --- Jasper
    # "tensorflow/tensorflow",
    # "kubernetes/kubernetes",
    # "neovim/neovim",
    # "vuejs/vue",
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
