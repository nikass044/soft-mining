from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    repos: list[str]
    github_token: str
    db_path: Path
    checkpoint_dir: Path
    phases: list[str]
    rest_per_page: int = 100
    max_retry_delay: int = 60
    max_retries: int = 10

    @classmethod
    def from_env(cls) -> Config:
        repos_raw = os.environ.get("PR_DIGGER_REPOS", "facebook/react")
        repos = [r.strip() for r in repos_raw.split(",") if r.strip()]

        github_token = os.environ.get("GITHUB_TOKEN", "")

        db_path = Path(os.environ.get("PR_DIGGER_DB_PATH", "data/pr_digger.db"))
        checkpoint_dir = Path(os.environ.get("PR_DIGGER_CHECKPOINT_DIR", "data/checkpoints"))

        phases_raw = os.environ.get("PR_DIGGER_PHASES", "1,2,3")
        phases = [p.strip() for p in phases_raw.split(",") if p.strip()]

        return cls(
            repos=repos,
            github_token=github_token,
            db_path=db_path,
            checkpoint_dir=checkpoint_dir,
            phases=phases,
        )
