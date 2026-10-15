"""Published-plugin contract and container ABI integration checks."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any

import grpc
import pytest
from syntara_plugin.contracts import container_workload_request_digest
from syntara_plugin.runtime import (
    ActionRegistry,
    ContainerInvocation,
    ContainerResult,
    add_container_runtime_service,
)

from execution_plane.container_protocol.workload import ContainerWorkload, validate_container_request
from execution_plane_client.commands.work import WorkExecute

_OUTPUT_DIGEST = "sha256:" + "a" * 64
_IMAGE_DIGEST = "registry.example/acme/issue-tools@sha256:" + "b" * 64
_PLUGIN_DIGEST = "sha256:" + "c" * 64


def _request(*, credential_leases: list[object] | None = None, runtime_context: object | None = None) -> dict[str, Any]:
    request: dict[str, Any] = {
        "apiVersion": "syntara.io/v1alpha1",
        "kind": "ContainerWorkloadRequest",
        "workItem": {
            "apiVersion": "syntara.io/v1alpha1",
            "kind": "ContainerStepWorkItem",
            "invocationId": "invocation-1",
            "workflowRunId": "workflow-1",
            "stepRevisionId": "github.create-issue.v1",
            "pluginDigest": _PLUGIN_DIGEST,
            "runtimeAbi": "syntara.container/v1alpha1",
            "imageDigest": _IMAGE_DIGEST,
            "integrationHandle": "github-primary",
            "inputs": {"title": "Example"},
            "runtimeContext": runtime_context if runtime_context is not None else {},
            "credentialHandles": [],
            "capabilityGrantId": "grant-1",
            "executionPolicyId": "policy-1",
            "timeoutSeconds": 30,
            "requestHash": "",
        },
        "policySnapshot": {
            "apiVersion": "syntara.io/v1alpha1",
            "kind": "ExecutionPolicySnapshot",
            "policyId": "policy-1",
            "revision": "1",
            "requestHash": "",
            "effectiveCapabilities": [],
        },
        "credentialLeases": credential_leases or [],
    }
    digest = container_workload_request_digest(request)
    request["workItem"]["requestHash"] = digest
    request["policySnapshot"]["requestHash"] = digest
    return request


def test_container_command_accepts_only_the_container_request_shape() -> None:
    command = WorkExecute(workload_type="container", container_request=_request())

    assert command.image is None
    assert command.invocation is None
    assert command.container_request is not None


def test_container_request_rejects_a_modified_hash() -> None:
    request = _request()
    request["workItem"]["inputs"] = {"title": "Changed"}

    with pytest.raises(ValueError, match="invalid"):
        validate_container_request(request)


def test_container_workload_executes_through_the_published_runtime_abi() -> None:
    def create_issue(invocation: ContainerInvocation) -> ContainerResult:
        assert invocation.workflow_run_id == "workflow-1"
        assert invocation.credential_file_paths == {}
        assert invocation.inputs == {"title": "Example"}
        return ContainerResult(_OUTPUT_DIGEST, output={"issue": 42})

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    add_container_runtime_service(server, ActionRegistry({"github.create-issue.v1": create_issue}))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        result = ContainerWorkload.from_request(_request()).invoke(
            channel,
            attempt_id="attempt-1",
            progress=lambda _event: None,
            cancelled=Event(),
            startup_seconds=1,
            grace_seconds=1,
            on_execute_submitted=lambda: None,
        )
    finally:
        channel.close()
        server.stop(grace=0).wait()

    assert result == {
        "kind": "result",
        "outputSchemaDigest": _OUTPUT_DIGEST,
        "output": {"issue": 42},
        "objectReference": None,
    }


def test_container_workload_rejects_nonempty_credential_leases() -> None:
    with pytest.raises(ValueError, match="credential leases"):
        ContainerWorkload.from_request(
            _request(
                credential_leases=[
                    {
                        "apiVersion": "syntara.io/v1alpha1",
                        "kind": "CredentialLease",
                        "leaseId": "lease-1",
                        "credentialHandle": "credential-1",
                        "audience": "execution-plane",
                        "expiresAt": "2030-01-01T00:00:00Z",
                        "secretFiles": [{"name": "token", "path": "/var/run/syntara/secrets/token"}],
                    }
                ]
            )
        )
