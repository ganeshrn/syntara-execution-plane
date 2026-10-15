"""Versioned work invocation values without node-provider imports."""

from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field, StrictStr, field_validator, model_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json


class NodeInvocation(WireModel):
    """Version-one invocation consumed by the existing node protocol."""

    version: Literal[1] = 1
    operation: Literal["execute"] = "execute"
    inputs: dict[str, object]
    credentials: dict[str, object] = Field(repr=False)
    workflow_context: dict[str, object]
    settings: dict[str, object]
    timeout_seconds: PositiveInt
    max_output_bytes: PositiveInt

    @field_validator("inputs", "credentials", "workflow_context", "settings", mode="before")
    @classmethod
    def check_json(cls, value: object) -> object:
        """Keep all nested invocation data portable and bounded."""
        return validate_json(value)


class WorkExecute(CommandPayload):
    """Script or SDK-container command with stable caller correlation."""

    command_type: ClassVar[str] = "work.execute"

    image: Annotated[StrictStr, Field(min_length=1, max_length=2048)] | None = None
    selectors: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    invocation: NodeInvocation | None = Field(default=None, repr=False)
    output_config: dict[str, str] | None = None
    workload_type: Literal["script", "container"] = "script"
    container_request: dict[str, Any] | None = Field(default=None, repr=False)

    @field_validator("container_request", mode="before")
    @classmethod
    def check_container_request_json(cls, value: object) -> object:
        """Keep container data portable without importing EP implementation modules."""
        if value is None:
            return value
        return validate_json(value)

    @model_validator(mode="after")
    def check_workload_shape(self) -> "WorkExecute":
        """Accept exactly one supported workload payload shape."""
        if self.workload_type == "script":
            if self.image is None or self.invocation is None or self.container_request is not None:
                message = "script work requires image and invocation only"
                raise ValueError(message)
            return self
        if self.container_request is None or self.image is not None or self.invocation is not None:
            message = "container work requires container_request only"
            raise ValueError(message)
        return self
