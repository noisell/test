"""Atomic catalog batch publisher with protocol validation and lease fencing."""

from .backoff import ExponentialBackoff
from .domain import Finish, FinishKind, GatewayResult, PublishReport, ResultKind
from .gateway import BulkGateway, TemporaryGatewayError
from .metrics import Metrics
from .repository import CatalogRepository, LeaseLost, StorageError


class CatalogPublisher:
    def __init__(
        self,
        repository: CatalogRepository,
        gateway: BulkGateway,
        metrics: Metrics,
        clock,
        backoff: ExponentialBackoff,
        lease_seconds: int = 30,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.metrics = metrics
        self.clock = clock
        self.backoff = backoff
        self.lease_seconds = lease_seconds

    def run_once(self, limit: int = 100) -> PublishReport:
        claimed_at = self.clock.now()
        batch = self.repository.claim_due(claimed_at, self.lease_seconds, limit)
        report = PublishReport(claimed=len(batch.operations))
        if not batch.operations:
            return report

        requested = {op.operation_id: op for op in batch.operations}
        protocol_error = False
        try:
            results = list(self.gateway.publish(batch.operations))
        except (TemporaryGatewayError, OSError):
            results = self._retry_results(batch.operations, "transport")
        except TypeError:
            # A gateway violating its return-shape contract is treated as a
            # malformed response, never allowed to crash or partially commit.
            results = []
            protocol_error = True

        # Validate the complete response before constructing or persisting any
        # transition. A malformed batch is retried as one idempotent unit.
        if len(results) != len(requested):
            protocol_error = True
        by_id = {}
        for result in results:
            if not isinstance(result, GatewayResult):
                protocol_error = True
                break
            operation_id = result.operation_id
            if (not isinstance(operation_id, str) or
                    not isinstance(result.kind, ResultKind) or
                    operation_id not in requested or operation_id in by_id):
                protocol_error = True
                break
            by_id[operation_id] = result
        if len(by_id) != len(requested):
            protocol_error = True

        if protocol_error:
            report.protocol_errors = 1
            results_by_id = {
                op.operation_id: GatewayResult(
                    op.operation_id, ResultKind.RETRYABLE, "invalid_bulk_response"
                )
                for op in batch.operations
            }
        else:
            results_by_id = by_id

        # Use the current time, not the claim time: the lease may have expired
        # while the provider request was in flight.
        finish_now = self.clock.now()
        finishes = [
            self._finish_for(
                op.operation_id, op.attempt, results_by_id[op.operation_id], finish_now
            )
            for op in batch.operations
        ]
        try:
            summary = self.repository.finish_batch(
                batch.lease_token, finishes, finish_now
            )
        except LeaseLost:
            report.lease_lost = len(batch.operations)
        except StorageError:
            self.metrics.increment("catalog.finish_failed")
        else:
            report.accepted = summary.accepted
            report.retried = summary.retried
            report.rejected = summary.rejected
            report.obsolete = summary.obsolete

        self._record(report)
        return report

    @staticmethod
    def _retry_results(operations, reason):
        return [
            GatewayResult(op.operation_id, ResultKind.RETRYABLE, reason)
            for op in operations
        ]

    def _finish_for(self, operation_id: str, attempt: int, result: GatewayResult, now: float) -> Finish:
        if result.kind is ResultKind.ACCEPTED:
            return Finish(operation_id, FinishKind.ACK)
        if result.kind is ResultKind.REJECTED:
            return Finish(operation_id, FinishKind.REJECT, reason=result.reason)
        retry_at = self.backoff.retry_at(now, attempt, result.retry_after_seconds)
        return Finish(operation_id, FinishKind.RETRY, retry_at=retry_at, reason=result.reason)

    def _record(self, report: PublishReport) -> None:
        for name in (
            "claimed",
            "accepted",
            "retried",
            "rejected",
            "obsolete",
            "protocol_errors",
            "lease_lost",
        ):
            value = getattr(report, name)
            if value:
                self.metrics.increment(f"catalog.{name}", value)
