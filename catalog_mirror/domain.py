"""Domain objects shared by the catalog mirror layers."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple


class OperationState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    ACKED = "acked"
    REJECTED = "rejected"
    OBSOLETE = "obsolete"


class ResultKind(str, Enum):
    ACCEPTED = "accepted"
    RETRYABLE = "retryable"
    REJECTED = "rejected"


class FinishKind(str, Enum):
    ACK = "ack"
    RETRY = "retry"
    REJECT = "reject"


@dataclass(frozen=True)
class CatalogChange:
    operation_id: str
    item_id: str
    version: int
    fields: Mapping[str, str]


@dataclass(frozen=True)
class LeasedOperation:
    operation_id: str
    item_id: str
    version: int
    fields: Mapping[str, str]
    attempt: int


@dataclass(frozen=True)
class LeaseBatch:
    lease_token: str
    operations: Tuple[LeasedOperation, ...]


@dataclass(frozen=True)
class GatewayResult:
    operation_id: str
    kind: ResultKind
    reason: Optional[str] = None
    retry_after_seconds: Optional[int] = None


@dataclass(frozen=True)
class Finish:
    operation_id: str
    kind: FinishKind
    retry_at: Optional[float] = None
    reason: Optional[str] = None


@dataclass
class PublishReport:
    claimed: int = 0
    accepted: int = 0
    retried: int = 0
    rejected: int = 0
    obsolete: int = 0
    protocol_errors: int = 0
    lease_lost: int = 0

    def terminal(self) -> int:
        return self.accepted + self.rejected + self.obsolete


@dataclass(frozen=True)
class FinishSummary:
    accepted: int = 0
    retried: int = 0
    rejected: int = 0
    obsolete: int = 0

