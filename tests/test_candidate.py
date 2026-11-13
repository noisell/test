"""Behavioral interaction tests for the publisher/repository boundary."""

import unittest

from catalog_mirror import (
    CatalogChange, CatalogPublisher, ExponentialBackoff, Finish, FinishKind,
    GatewayResult, InMemoryCatalogRepository, InMemoryMetrics, LeaseLost,
    OperationState, ResultKind, TemporaryGatewayError,
)
from tests.fakes import FakeClock, ScriptedGateway


class CandidateInteractionTests(unittest.TestCase):
    def setup_case(self, changes, scripts, lease_seconds=30):
        repo = InMemoryCatalogRepository(changes)
        gateway = ScriptedGateway(scripts)
        metrics = InMemoryMetrics()
        clock = FakeClock()
        publisher = CatalogPublisher(repo, gateway, metrics, clock, ExponentialBackoff(5, 60), lease_seconds)
        return repo, gateway, metrics, clock, publisher

    def test_reordered_mixed_results_are_correlated_by_operation_id(self):
        repo, _, _, _, publisher = self.setup_case(
            [CatalogChange("a", "sku-a", 1, {}), CatalogChange("b", "sku-b", 1, {}), CatalogChange("c", "sku-c", 1, {})],
            [[GatewayResult("c", ResultKind.REJECTED, "bad"), GatewayResult("a", ResultKind.ACCEPTED), GatewayResult("b", ResultKind.RETRYABLE, retry_after_seconds=9)]],
        )
        report = publisher.run_once()
        self.assertEqual((report.accepted, report.retried, report.rejected), (1, 1, 1))
        self.assertEqual(repo.state("a"), OperationState.ACKED)
        self.assertEqual(repo.state("b"), OperationState.PENDING)
        self.assertEqual(repo.state("c"), OperationState.REJECTED)

    def test_duplicate_unknown_and_missing_ids_retry_entire_batch(self):
        malformed = (
            [GatewayResult("a", ResultKind.ACCEPTED), GatewayResult("a", ResultKind.ACCEPTED)],
            [GatewayResult("a", ResultKind.ACCEPTED), GatewayResult("ghost", ResultKind.ACCEPTED)],
            [GatewayResult("a", ResultKind.ACCEPTED)],
        )
        for results in malformed:
            repo, _, _, _, publisher = self.setup_case(
                [CatalogChange("a", "a", 1, {}), CatalogChange("b", "b", 1, {})], [results]
            )
            report = publisher.run_once()
            self.assertEqual(report.protocol_errors, 1)
            self.assertEqual(report.retried, 2)
            self.assertTrue(all(s is OperationState.PENDING for s in repo.snapshot().values()))

    def test_transport_error_retries_all_with_stable_operation_ids(self):
        repo, gateway, _, clock, publisher = self.setup_case(
            [CatalogChange("a", "a", 1, {}), CatalogChange("b", "b", 1, {})],
            [TemporaryGatewayError("offline"), [GatewayResult("a", ResultKind.ACCEPTED), GatewayResult("b", ResultKind.ACCEPTED)]],
        )
        first = publisher.run_once()
        self.assertEqual(first.retried, 2)
        self.assertEqual([op.operation_id for op in gateway.calls[0]], ["a", "b"])
        clock.advance(5)
        second = publisher.run_once()
        self.assertEqual(second.accepted, 2)
        self.assertEqual([op.operation_id for op in gateway.calls[1]], ["a", "b"])

    def test_expired_lease_discards_late_provider_response(self):
        class SlowGateway:
            def __init__(self, clock):
                self.clock = clock
            def publish(self, operations):
                self.clock.advance(31)
                return [GatewayResult(op.operation_id, ResultKind.ACCEPTED) for op in operations]

        repo = InMemoryCatalogRepository([CatalogChange("a", "sku", 1, {})])
        clock = FakeClock()
        publisher = CatalogPublisher(repo, SlowGateway(clock), InMemoryMetrics(), clock, ExponentialBackoff(), 30)
        report = publisher.run_once()
        self.assertEqual(report.accepted, 0)
        self.assertEqual(report.lease_lost, 1)
        self.assertEqual(repo.state("a"), OperationState.LEASED)

    def test_commit_failure_changes_no_operation_and_reports_no_success(self):
        repo, _, metrics, _, publisher = self.setup_case(
            [CatalogChange("a", "a", 1, {}), CatalogChange("b", "b", 1, {})],
            [[GatewayResult("a", ResultKind.ACCEPTED), GatewayResult("b", ResultKind.REJECTED, "no")]],
        )
        repo.fail_next_finish = True
        report = publisher.run_once()
        self.assertEqual((report.accepted, report.rejected, report.retried), (0, 0, 0))
        self.assertEqual(repo.state("a"), OperationState.LEASED)
        self.assertEqual(repo.state("b"), OperationState.LEASED)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot.get("catalog.accepted", 0), 0)
        self.assertEqual(snapshot.get("catalog.rejected", 0), 0)

    def test_old_ack_after_newer_publish_is_obsolete_and_cannot_downgrade(self):
        repo = InMemoryCatalogRepository([CatalogChange("old", "sku", 1, {})])
        repo.enqueue(CatalogChange("new", "sku", 2, {}))
        old_batch = repo.claim_due(1000, 30, 1)
        new_batch = repo.claim_due(1000, 30, 1)
        new_summary = repo.finish_batch(new_batch.lease_token, [Finish("new", FinishKind.ACK)], 1000)
        old_summary = repo.finish_batch(old_batch.lease_token, [Finish("old", FinishKind.ACK)], 1000)
        self.assertEqual(new_summary.accepted, 1)
        self.assertEqual(old_summary.obsolete, 1)
        self.assertEqual(repo.published_version("sku"), 2)
        self.assertEqual(repo.state("old"), OperationState.OBSOLETE)

    def test_repository_rejects_partial_finish_without_mutation(self):
        repo = InMemoryCatalogRepository([CatalogChange("a", "a", 1, {}), CatalogChange("b", "b", 1, {})])
        batch = repo.claim_due(1000, 30, 2)
        with self.assertRaises(LeaseLost):
            repo.finish_batch(batch.lease_token, [Finish("a", FinishKind.ACK)], 1000)
        self.assertEqual(repo.state("a"), OperationState.LEASED)
        self.assertEqual(repo.state("b"), OperationState.LEASED)


if __name__ == "__main__":
    unittest.main()
