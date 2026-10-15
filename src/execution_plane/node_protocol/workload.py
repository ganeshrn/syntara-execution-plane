"""Legacy script-node workload adapter for the generic Kubernetes transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from execution_plane.node_protocol.client import NodeRpcError, invoke
from execution_plane.node_protocol.codec import MAX_MESSAGE_BYTES, encode_request
from execution_plane.worker_manager.vanilla_k8s.workload_protocol import WorkloadProtocolError

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    import grpc


_INVALID_TIMEOUT = "Node invocation timeout is invalid"
_TRANSPORT_TOO_LARGE = "Node invocation exceeds transport limit"


@dataclass(frozen=True)
class NodeWorkload:
    """Keep the existing script protocol behind the shared transport interface."""

    invocation: dict[str, Any]

    @property
    def timeout_seconds(self) -> int:
        """Return the legacy invocation duration used to bound the Pod lifetime."""
        timeout = self.invocation.get("timeout_seconds")
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1:
            raise WorkloadProtocolError(_INVALID_TIMEOUT)
        return timeout

    def validate_transport(self) -> None:
        """Reject a legacy request that exceeds the agreed gRPC message limit."""
        try:
            request = encode_request(self.invocation, "transport-preflight")
        except (TypeError, ValueError) as error:
            raise WorkloadProtocolError(_TRANSPORT_TOO_LARGE) from error
        if request.ByteSize() > MAX_MESSAGE_BYTES:
            raise WorkloadProtocolError(_TRANSPORT_TOO_LARGE)

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
        """Delegate the legacy script request to its existing gRPC client."""
        try:
            return invoke(
                channel,
                self.invocation,
                identity=attempt_id,
                progress=progress,
                cancelled=cancelled,
                startup=startup_seconds,
                grace=grace_seconds,
                on_execute_submitted=on_execute_submitted,
            )
        except NodeRpcError as error:
            raise WorkloadProtocolError(str(error), retryable=error.retryable, cancelled=error.cancelled) from error
