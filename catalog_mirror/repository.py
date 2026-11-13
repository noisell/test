"""Repository contract and deterministic, atomic in-memory implementation."""

from dataclasses import dataclass
from threading import RLock
from typing import Dict, Iterable, Mapping, Optional, Protocol, Sequence
from uuid import uuid4

from .domain import (
    CatalogChange, Finish, FinishKind, FinishSummary, LeaseBatch,
    LeasedOperation, OperationState,
)


class LeaseLost(RuntimeError):
    """The caller no longer owns every operation in the claimed batch."""


class StorageError(RuntimeError):
    pass


class CatalogRepository(Protocol):
    def claim_due(self, now: float, lease_seconds: int, limit: int) -> LeaseBatch: ...
    def finish_batch(self, lease_token: str, finishes: Sequence[Finish], now: float) -> FinishSummary: ...


@dataclass
class _Row:
    change: CatalogChange
    state: OperationState = OperationState.PENDING
    attempt: int = 0
    due_at: float = 0.0
    lease_token: Optional[str] = None
    leased_until: float = 0.0
    reason: Optional[str] = None


class InMemoryCatalogRepository:
    def __init__(self, changes: Iterable[CatalogChange] = ()) -> None:
        self._rows: Dict[str, _Row] = {}
        self._latest_version: Dict[str, int] = {}
        self._published_version: Dict[str, int] = {}
        self._lock = RLock()
        self.fail_next_finish = False
        for change in changes:
            self.enqueue(change)

    def enqueue(self, change: CatalogChange) -> None:
        with self._lock:
            if change.operation_id in self._rows:
                raise ValueError("operation_id must be unique")
            current = self._latest_version.get(change.item_id, -1)
            if change.version <= current:
                raise ValueError("item versions must increase")
            self._latest_version[change.item_id] = change.version
            self._rows[change.operation_id] = _Row(change=change)

    def claim_due(self, now: float, lease_seconds: int, limit: int) -> LeaseBatch:
        if lease_seconds <= 0 or limit <= 0:
            raise ValueError("lease_seconds and limit must be positive")
        with self._lock:
            candidates = [
                row for row in self._rows.values()
                if (row.state is OperationState.PENDING and row.due_at <= now)
                or (row.state is OperationState.LEASED and row.leased_until <= now)
            ]
            candidates.sort(key=lambda row: (row.due_at, row.change.operation_id))
            selected = candidates[:limit]
            token = uuid4().hex
            for row in selected:
                row.state = OperationState.LEASED
                row.lease_token = token
                row.leased_until = now + lease_seconds
                row.attempt += 1
            operations = tuple(
                LeasedOperation(
                    row.change.operation_id, row.change.item_id,
                    row.change.version, dict(row.change.fields), row.attempt,
                )
                for row in selected
            )
            return LeaseBatch(token, operations)

    def finish_one(self, lease_token: str, finish: Finish, now: float) -> FinishSummary:
        """Compatibility API for the legacy single-operation caller."""
        return self.finish_batch(lease_token, [finish], now)

    def finish_batch(self, lease_token: str, finishes: Sequence[Finish], now: float) -> FinishSummary:
        with self._lock:
            operation_ids = [finish.operation_id for finish in finishes]
            if len(operation_ids) != len(set(operation_ids)):
                raise ValueError("finish operation_id values must be unique")

            # Validate the complete lease cohort before changing any row. This
            # prevents callers from committing only a subset of their batch.
            leased_ids = {
                operation_id for operation_id, row in self._rows.items()
                if row.state is OperationState.LEASED and row.lease_token == lease_token
            }
            if set(operation_ids) != leased_ids or len(operation_ids) != len(leased_ids):
                raise LeaseLost("finish set does not match lease cohort")

            rows = []
            for operation_id in operation_ids:
                row = self._rows.get(operation_id)
                if (row is None or row.state is not OperationState.LEASED
                        or row.lease_token != lease_token or row.leased_until <= now):
                    raise LeaseLost(operation_id)
                rows.append(row)

            # Simulate transaction commit failure before the first mutation.
            if self.fail_next_finish:
                self.fail_next_finish = False
                raise StorageError("injected commit failure")

            # Compute all target states first; the mutation section below is the
            # in-memory equivalent of one database transaction commit.
            transitions = []
            for row, finish in zip(rows, finishes):
                if finish.kind is FinishKind.RETRY:
                    transitions.append((OperationState.PENDING, None, finish.retry_at if finish.retry_at is not None else now, None))
                elif finish.kind is FinishKind.REJECT:
                    transitions.append((OperationState.REJECTED, None, row.due_at, finish.reason))
                elif row.change.version < self._latest_version[row.change.item_id]:
                    transitions.append((OperationState.OBSOLETE, None, row.due_at, None))
                else:
                    transitions.append((OperationState.ACKED, row.change.version, row.due_at, None))

            accepted = retried = rejected = obsolete = 0
            for row, (state, published, due_at, reason) in zip(rows, transitions):
                row.state = state
                row.lease_token = None
                row.leased_until = 0.0
                row.due_at = due_at
                row.reason = reason
                if state is OperationState.ACKED:
                    previous = self._published_version.get(row.change.item_id, -1)
                    self._published_version[row.change.item_id] = max(previous, published)
                    accepted += 1
                elif state is OperationState.PENDING:
                    retried += 1
                elif state is OperationState.REJECTED:
                    rejected += 1
                elif state is OperationState.OBSOLETE:
                    obsolete += 1
            return FinishSummary(accepted, retried, rejected, obsolete)

    def state(self, operation_id: str) -> OperationState:
        with self._lock:
            return self._rows[operation_id].state

    def due_at(self, operation_id: str) -> float:
        with self._lock:
            return self._rows[operation_id].due_at

    def published_version(self, item_id: str) -> Optional[int]:
        with self._lock:
            return self._published_version.get(item_id)

    def attempts(self, operation_id: str) -> int:
        with self._lock:
            return self._rows[operation_id].attempt

    def snapshot(self) -> Mapping[str, OperationState]:
        with self._lock:
            return {key: row.state for key, row in self._rows.items()}
