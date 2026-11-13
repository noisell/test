"""Catalog mirror batch publisher.

This is the incident-era implementation.  It kept the ordered legacy provider
working for years, so changes here must retain the public API and report shape.
"""

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
        now = self.clock.now()
        batch = self.repository.claim_due(now, self.lease_seconds, limit)
        report = PublishReport(claimed=len(batch.operations))
        if not batch.operations:
            return report
        try:
            results = self.gateway.publish(batch.operations)
        except TemporaryGatewayError:
            results = [
                GatewayResult(op.operation_id, ResultKind.RETRYABLE, "transport")
                for op in batch.operations
            ]

        # The previous provider returned results in request order.  Keep the
        # simple path until the bulk-v2 incident has a verified replacement.
        for operation, result in zip(batch.operations, results):
            finish = self._finish_for(operation.operation_id, operation.attempt, result, now)
            try:
                summary = self.repository.finish_one(batch.lease_token, finish, now)
            except LeaseLost:
                report.lease_lost += 1
                continue
            except StorageError:
                self.metrics.increment("catalog.finish_failed")
                continue
            report.accepted += summary.accepted
            report.retried += summary.retried
            report.rejected += summary.rejected
            report.obsolete += summary.obsolete
        self._record(report)
        return report

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
            self.metrics.increment(f"catalog.{name}", getattr(report, name))

