import unittest

from catalog_mirror import (
    CatalogChange,
    CatalogPublisher,
    ExponentialBackoff,
    InMemoryCatalogRepository,
    InMemoryMetrics,
    OperationState,
)
from tests.fakes import FakeClock, OrderedAcceptingGateway


class LegacyPublisherTest(unittest.TestCase):
    def make_publisher(self, repository, gateway, clock=None):
        return CatalogPublisher(
            repository,
            gateway,
            InMemoryMetrics(),
            clock or FakeClock(),
            ExponentialBackoff(5, 60),
            lease_seconds=30,
        )

    def test_empty_poll_is_a_noop(self):
        repository = InMemoryCatalogRepository()
        gateway = OrderedAcceptingGateway()
        report = self.make_publisher(repository, gateway).run_once(10)
        self.assertEqual(report.claimed, 0)
        self.assertEqual(gateway.calls, [])

    def test_ordered_provider_marks_operations_acked(self):
        changes = [
            CatalogChange("op-a", "sku-a", 1, {"price": "100"}),
            CatalogChange("op-b", "sku-b", 4, {"price": "250"}),
        ]
        repository = InMemoryCatalogRepository(changes)
        report = self.make_publisher(repository, OrderedAcceptingGateway()).run_once(10)
        self.assertEqual((report.claimed, report.accepted), (2, 2))
        self.assertEqual(repository.state("op-a"), OperationState.ACKED)
        self.assertEqual(repository.published_version("sku-b"), 4)

    def test_limit_keeps_remaining_change_pending(self):
        changes = [
            CatalogChange("op-a", "sku-a", 1, {}),
            CatalogChange("op-b", "sku-b", 1, {}),
        ]
        repository = InMemoryCatalogRepository(changes)
        report = self.make_publisher(repository, OrderedAcceptingGateway()).run_once(1)
        self.assertEqual(report.claimed, 1)
        self.assertEqual(sum(s is OperationState.PENDING for s in repository.snapshot().values()), 1)


if __name__ == "__main__":
    unittest.main()

