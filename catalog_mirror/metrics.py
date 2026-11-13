"""Tiny dependency-free metrics port used by the service and the exercise fakes."""

from collections import Counter
from threading import Lock
from typing import Mapping, Protocol


class Metrics(Protocol):
    def increment(self, name: str, value: int = 1) -> None: ...


class InMemoryMetrics:
    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = Lock()

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._values[name] += value

    def snapshot(self) -> Mapping[str, int]:
        with self._lock:
            return dict(self._values)

