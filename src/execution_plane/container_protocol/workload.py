"""Adapt validated SDK container workload requests to the published runtime ABI."""

from __future__ import annotations

import json
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from syntara_plugin.contracts import (
    CanonicalJsonError,
    container_workload_request_digest,
    load_bundle,
    validate_document,
)
from syntara_plugin.runtime import ANY_STEP_REVISION, ContainerClient, ContainerInvocation, ContainerRemoteError

from execution_plane.worker_manager.vanilla_k8s.workload_protocol import WorkloadProtocolError

if TYPE_CHECKING:
    from collections.abc import Callable

    import grpc


_MAX_JSON_BYTES = 1_048_576
_INVALID_REQUEST = "Container workload request is invalid"
_LEASES_UNSUPPORTED = "Container credential leases are unavailable until EP secret mounts are implemented"
_CANCELLED_BEFORE_SUBMISSION = "Container workload was cancelled before submission"
_STEP_NOT_ADVERTISED = "Container image does not advertise the selected step revision"
_NEGOTIATION_FAILED = "Container workload ABI negotiation failed"
_STOPPED_AFTER_CANCEL = "Container workload stopped after cancellation"
_NO_TERMINAL_RESULT = "Container workload protocol did not return a terminal result"


def validate_container_request(request: object) -> dict[str, Any]:
    """Validate an SDK-owned request and its mirrored canonical hash fields.

    The request contains handles and mount paths only. It must never contain a
    credential value; non-empty leases remain rejected until EP implements its
    separately designed OpenShift secret-delivery integration.
    """
    if not isinstance(request, dict):
        raise TypeError(_INVALID_REQUEST)
    errors = validate_document(load_bundle(), "runtime", request)
    if errors:
        raise ValueError(_INVALID_REQUEST)
    work_item = request.get("workItem")
    policy_snapshot = request.get("policySnapshot")
    if not isinstance(work_item, dict) or not isinstance(policy_snapshot, dict):
        raise TypeError(_INVALID_REQUEST)
    try:
        expected_hash = container_workload_request_digest(request)
    except CanonicalJsonError as error:
        raise ValueError(_INVALID_REQUEST) from error
    if work_item.get("requestHash") != expected_hash or policy_snapshot.get("requestHash") != expected_hash:
        raise ValueError(_INVALID_REQUEST)
    if request.get("credentialLeases"):
        raise ValueError(_LEASES_UNSUPPORTED)
    return request


@dataclass(frozen=True)
class ContainerWorkload:
    """A credential-value-free invocation of one installed plugin step revision."""

    image: str
    invocation: ContainerInvocation

    @classmethod
    def from_request(cls, request: object) -> ContainerWorkload:
        """Materialize runtime data only after canonical contract validation."""
        validated = validate_container_request(request)
        work_item = validated["workItem"]
        if not isinstance(work_item, dict):  # defensive: the schema validation above guarantees this
            raise TypeError(_INVALID_REQUEST)
        return cls(
            image=str(work_item["imageDigest"]),
            invocation=ContainerInvocation(
                invocation_id=str(work_item["invocationId"]),
                workflow_run_id=str(work_item["workflowRunId"]),
                step_revision_id=str(work_item["stepRevisionId"]),
                inputs=dict(work_item["inputs"]),
                credential_file_paths={},
                timeout_seconds=int(work_item["timeoutSeconds"]),
                runtime_context=dict(work_item.get("runtimeContext", {})),
            ),
        )

    @property
    def timeout_seconds(self) -> int:
        """Return the approved duration used to bound the EP Pod lifetime."""
        return self.invocation.timeout_seconds

    def validate_transport(self) -> None:
        """Perform bounded JSON checks before allocating Kubernetes resources."""
        for value in (self.invocation.inputs, self.invocation.runtime_context):
            try:
                encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
            except (TypeError, ValueError) as error:
                raise WorkloadProtocolError(_INVALID_REQUEST) from error
            if len(encoded) > _MAX_JSON_BYTES:
                raise WorkloadProtocolError(_INVALID_REQUEST)

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
        """Negotiate the ABI, invoke exactly one selected step, and map its result."""
        del attempt_id, progress
        client = ContainerClient(channel)
        self._wait_for_selected_step(client, cancelled, startup_seconds)
        if cancelled.is_set():
            raise WorkloadProtocolError(_CANCELLED_BEFORE_SUBMISSION, cancelled=True)
        on_execute_submitted()
        return self._execute_with_cancellation(client, cancelled, grace_seconds)

    def _wait_for_selected_step(
        self, client: ContainerClient, cancelled: threading.Event, startup_seconds: float
    ) -> None:
        deadline = time.monotonic() + startup_seconds
        while True:
            if cancelled.is_set():
                raise WorkloadProtocolError(_CANCELLED_BEFORE_SUBMISSION, cancelled=True)
            try:
                advertised = client.health(timeout_seconds=min(1, max(0.1, deadline - time.monotonic())))
                if self.invocation.step_revision_id not in advertised and ANY_STEP_REVISION not in advertised:
                    raise WorkloadProtocolError(_STEP_NOT_ADVERTISED)
                return
            except ContainerRemoteError as error:
                if error.source != "transport" or time.monotonic() >= deadline:
                    raise WorkloadProtocolError(_NEGOTIATION_FAILED, retryable=error.source == "transport") from error
                cancelled.wait(0.1)

    def _execute_with_cancellation(
        self, client: ContainerClient, cancelled: threading.Event, grace_seconds: int
    ) -> dict[str, Any]:
        stopped = threading.Event()
        watcher = threading.Thread(
            target=_relay_cancellation,
            args=(client, self.invocation.invocation_id, cancelled, stopped),
            daemon=True,
            name="container-grpc-cancel",
        )
        watcher.start()
        try:
            result = client.execute(self.invocation, timeout_seconds=self.timeout_seconds + grace_seconds)
            return {
                "kind": "result",
                "outputSchemaDigest": result.output_schema_digest,
                "output": dict(result.output) if result.output is not None else None,
                "objectReference": result.object_reference,
            }
        except ContainerRemoteError as error:
            if error.source == "workload":
                return {
                    "kind": "failure",
                    "code": error.code.value,
                    "retryable": error.retryable,
                    "outcome": error.outcome,
                }
            if cancelled.is_set():
                raise WorkloadProtocolError(_STOPPED_AFTER_CANCEL, cancelled=True) from error
            raise WorkloadProtocolError(_NO_TERMINAL_RESULT, retryable=error.source == "transport") from error
        finally:
            stopped.set()
            watcher.join(timeout=6)


def _relay_cancellation(
    client: ContainerClient, invocation_id: str, cancelled: threading.Event, stopped: threading.Event
) -> None:
    """Ask the image to cancel once when the EP work item is cancelled."""
    while not stopped.wait(0.1):
        if cancelled.is_set():
            with suppress(ContainerRemoteError):
                client.cancel(invocation_id, timeout_seconds=5)
            return
