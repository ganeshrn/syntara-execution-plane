"""Protocol boundary between Kubernetes allocation and workload application data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable
    from typing import Any

    import grpc


GRPC_PORT = 50051
MAX_MESSAGE_BYTES = 2_097_152
CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
    ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
    ("grpc.enable_retries", 0),
)


class WorkloadProtocolError(Exception):
    """Safe application-protocol failure classified for the EP lifecycle."""

    def __init__(self, message: str, *, retryable: bool = False, cancelled: bool = False) -> None:
        """Record whether the safe failure is retryable or cancellation-confirmed."""
        super().__init__(message)
        self.retryable = retryable
        self.cancelled = cancelled


class WorkloadProtocol(Protocol):
    """One workload ABI that can run inside an EP-managed cold-start pod."""

    @property
    def timeout_seconds(self) -> int:
        """Return the approved execution timeout used for pod lifetime limits."""

    def validate_transport(self) -> None:
        """Reject an invalid request before Kubernetes resources are created."""

    def invoke(
        self,
        channel: grpc.Channel,
        *,
        attempt_id: str,
        progress: Callable[[dict[str, Any]], None],
        cancelled: threading.Event,
        startup_seconds: float,
        grace_seconds: int,
        on_execute_submitted: Callable[[], None],
    ) -> dict[str, Any]:
        """Call the image's workload ABI and return one terminal frame."""
