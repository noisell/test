from collections import deque
from typing import Callable, Iterable, Sequence

from catalog_mirror.domain import GatewayResult, LeasedOperation, ResultKind


class FakeClock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class ScriptedGateway:
    """A deterministic fake; each call consumes one result-producing script."""

    def __init__(self, scripts: Iterable[Sequence[GatewayResult] | Exception | Callable]):
        self.scripts = deque(scripts)
        self.calls: list[tuple[LeasedOperation, ...]] = []

    def publish(self, operations: Sequence[LeasedOperation]) -> Sequence[GatewayResult]:
        copied = tuple(operations)
        self.calls.append(copied)
        action = self.scripts.popleft()
        if isinstance(action, Exception):
            raise action
        if callable(action):
            return action(copied)
        return list(action)


class OrderedAcceptingGateway:
    """Legacy test double which happens to preserve request order."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def publish(self, operations: Sequence[LeasedOperation]) -> Sequence[GatewayResult]:
        self.calls.append(tuple(op.operation_id for op in operations))
        return [GatewayResult(op.operation_id, ResultKind.ACCEPTED) for op in operations]
