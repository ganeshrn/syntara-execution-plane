"""Handle work commands through the existing work lifecycle."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.container_protocol.workload import validate_container_request
from execution_plane.work_item.work_item_service import WorkItemService
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference

_CONTAINER_REQUEST_INVALID = "Container workload request is invalid"


class WorkCommandHandler:
    """Persist work, without retaining the command envelope."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Use the caller's command UUID as the work item's stable UUID."""
        if not isinstance(payload, WorkExecute):
            raise UnsupportedCommand
        if payload.workload_type == "container":
            # Validate the SDK-owned runtime schema before persisting or scheduling.
            # Secret material is not part of this request and credential leases remain
            # unsupported until the platform mount path exists.
            try:
                validate_container_request(payload.container_request)
            except (TypeError, ValueError) as error:
                raise ValueError(_CONTAINER_REQUEST_INVALID) from error
        work_service = WorkItemService(WorkStore.from_engine(context.engine))
        item = await work_service.dispatch(
            client_id=context.client_id,
            item_id=message.command_id,
            payload=payload.model_dump(mode="json"),
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=item.id))
