import unittest

from catalog_mirror import (
    CatalogChange,
    CatalogPublisher,
    ExponentialBackoff,
    GatewayResult,
    InMemoryCatalogRepository,
    InMemoryMetrics,
    OperationState,
    ResultKind,
)
from tests.fakes import FakeClock, ScriptedGateway


class IncidentRegressionTest(unittest.TestCase):
    def build(self, repository, gateway, clock):
        return CatalogPublisher(
            repository,
            gateway,
            InMemoryMetrics(),
            clock,
            ExponentialBackoff(5, 60),
            lease_seconds=30,
        )

    def test_each_result_is_applied_to_the_named_operation(self):
        repository = InMemoryCatalogRepository(
            [
                CatalogChange("op-100", "sku-red", 1, {}),
                CatalogChange("op-200", "sku-blue", 1, {}),
            ]
        )
        gateway = ScriptedGateway(
            [[
                GatewayResult("op-200", ResultKind.REJECTED, "invalid currency"),
                GatewayResult("op-100", ResultKind.ACCEPTED),
            ]]
        )
        report = self.build(repository, gateway, FakeClock()).run_once(10)
        self.assertEqual((report.accepted, report.rejected), (1, 1))
        self.assertEqual(repository.state("op-100"), OperationState.ACKED)
        self.assertEqual(repository.state("op-200"), OperationState.REJECTED)

    def test_incomplete_response_is_retried_as_a_safe_unit(self):
        repository = InMemoryCatalogRepository(
            [CatalogChange("op-a", "a", 1, {}), CatalogChange("op-b", "b", 1, {})]
        )
        gateway = ScriptedGateway([[GatewayResult("op-a", ResultKind.ACCEPTED)]])
        report = self.build(repository, gateway, FakeClock()).run_once(10)
        self.assertEqual(report.retried, 2)
        self.assertEqual(repository.state("op-a"), OperationState.PENDING)
        self.assertEqual(repository.state("op-b"), OperationState.PENDING)
        self.assertEqual(report.protocol_errors, 1)


if __name__ == "__main__":
    unittest.main()

