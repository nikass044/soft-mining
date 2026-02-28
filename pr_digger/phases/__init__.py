from __future__ import annotations

from abc import ABC, abstractmethod


class MiningPhase(ABC):
    @abstractmethod
    def execute(self) -> None:
        ...
