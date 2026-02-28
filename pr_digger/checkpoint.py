from __future__ import annotations

import json
from pathlib import Path


class FileCheckpointStore:
    def __init__(self, checkpoint_dir: Path):
        self._dir = checkpoint_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, phase: str) -> Path:
        return self._dir / f"{phase}.json"

    def load(self, phase: str) -> dict | None:
        path = self._path_for(phase)
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def save(self, phase: str, state: dict) -> None:
        path = self._path_for(phase)
        path.write_text(json.dumps(state))

    def clear(self, phase: str) -> None:
        path = self._path_for(phase)
        if path.exists():
            path.unlink()
