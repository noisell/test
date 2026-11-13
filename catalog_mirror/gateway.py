"""Bulk upstream boundary and strict v2 response parsing."""

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, Sequence

from .domain import GatewayResult, LeasedOperation, ResultKind


class TemporaryGatewayError(RuntimeError):
    pass


class BulkGateway(Protocol):
    def publish(self, operations: Sequence[LeasedOperation]) -> Sequence[GatewayResult]: ...


@dataclass
class JsonBulkGateway:
    """Translate domain objects to/from an injected JSON HTTP callable."""

    post_json: Callable[[str, Mapping[str, Any]], Mapping[str, Any]]
    endpoint: str = "/v2/catalog/bulk"

    def publish(self, operations: Sequence[LeasedOperation]) -> Sequence[GatewayResult]:
        request = {
            "operations": [
                {
                    "operation_id": op.operation_id,
                    "item_id": op.item_id,
                    "version": op.version,
                    "fields": dict(op.fields),
                }
                for op in operations
            ]
        }
        try:
            response = self.post_json(self.endpoint, request)
        except (TimeoutError, ConnectionError, ValueError) as error:
            # ValueError includes JSON decoding failures in common adapters.
            raise TemporaryGatewayError(str(error)) from error
        if not isinstance(response, Mapping):
            raise TemporaryGatewayError("bulk response must be a JSON object")
        raw_results = response.get("results")
        if not isinstance(raw_results, list):
            raise TemporaryGatewayError("bulk response has no results list")
        try:
            return [self._parse_result(raw) for raw in raw_results]
        except (TypeError, ValueError) as error:
            raise TemporaryGatewayError("bulk response contains invalid result data") from error

    @staticmethod
    def _parse_result(raw: Any) -> GatewayResult:
        if not isinstance(raw, dict):
            raise TemporaryGatewayError("bulk result must be an object")
        operation_id = raw.get("operation_id")
        status = raw.get("status")
        if not isinstance(operation_id, str) or not operation_id:
            raise TemporaryGatewayError("bulk result has invalid operation_id")
        try:
            kind = ResultKind(status)
        except (ValueError, TypeError) as error:
            raise TemporaryGatewayError(f"unknown bulk status: {status!r}") from error
        retry_after = raw.get("retry_after_seconds")
        if retry_after is not None and (
            isinstance(retry_after, bool) or not isinstance(retry_after, int) or retry_after < 0
        ):
            raise TemporaryGatewayError("retry_after_seconds must be non-negative")
        reason = raw.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise TemporaryGatewayError("reason must be a string")
        return GatewayResult(operation_id, kind, reason, retry_after)
