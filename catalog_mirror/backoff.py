"""Retry scheduling kept separate from transport and persistence."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ExponentialBackoff:
    base_seconds: int = 5
    maximum_seconds: int = 300

    def __post_init__(self) -> None:
        if self.base_seconds <= 0 or self.maximum_seconds <= 0:
            raise ValueError("backoff values must be positive")

    def delay_seconds(self, attempt: int) -> int:
        if attempt <= 0:
            raise ValueError("attempt must be positive")
        return min(self.maximum_seconds, self.base_seconds * (2 ** (attempt - 1)))

    def retry_at(self, now: float, attempt: int, retry_after: int | None) -> float:
        provider_delay = max(0, retry_after or 0)
        return now + max(self.delay_seconds(attempt), provider_delay)

