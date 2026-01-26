"""Minimal lakehouse mount helper for Fabric."""

from __future__ import annotations

import time
from dataclasses import dataclass

try:
    import sempy.fabric as fabric
except ImportError:  # pragma: no cover - Fabric only
    fabric = None  # type: ignore[assignment]

try:
    from notebookutils import fs
except ImportError:  # pragma: no cover - Fabric only
    fs = None  # type: ignore[assignment]


@dataclass(frozen=True)
class LakehouseMount:
    workspace_id: str
    lakehouse_id: str
    lakehouse_base_path: str
    source_data_path: str


class StorageMountError(RuntimeError):
    """Raised when the lakehouse cannot be mounted."""


def _resolve_ids(workspace_name: str, display_name: str) -> tuple[str, str]:
    if fabric is None:
        raise ImportError("sempy.fabric is required to resolve lakehouse metadata.")

    workspaces_df = fabric.list_workspaces()
    workspace_id = (
        workspaces_df[workspaces_df["Name"] == workspace_name]["Id"].to_list()[0]
    )
    items_df = fabric.list_items(workspace=workspace_id)
    lakehouse_id = (
        items_df[
            (items_df["Type"] == "Lakehouse")
            & (items_df["Display Name"] == display_name)
        ]["Id"]
        .to_list()[0]
    )
    return workspace_id, lakehouse_id


def _format_base_path(workspace_id: str, lakehouse_id: str) -> str:
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"


def mount_lakehouse(
    workspace_name: str,
    lakehouse_display_name: str,
    mount_point: str | None = None,
    max_retries: int = 20,
    retry_delay: float = 1.0,
) -> LakehouseMount:
    """Mount the Fabric lakehouse and return the mount metadata."""
    if fs is None:
        raise ImportError("notebookutils.fs is required to mount the lakehouse.")

    workspace_id, lakehouse_id = _resolve_ids(workspace_name, lakehouse_display_name)
    lakehouse_base_path = _format_base_path(workspace_id, lakehouse_id)
    mount_point = mount_point or f"/home/trusted-service-user/mounts/{lakehouse_display_name.replace(' ', '_')}"

    last_error: Exception | None = None
    for _ in range(max_retries):
        try:
            fs.unmount(mount_point)
        except Exception:
            pass

        try:
            fs.mount(lakehouse_base_path, mount_point)
            source_data_path = fs.getMountPath(mount_point)
            return LakehouseMount(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                lakehouse_base_path=lakehouse_base_path,
                source_data_path=source_data_path,
            )
        except Exception as error:  # pragma: no cover
            last_error = error
            time.sleep(retry_delay)

    raise StorageMountError(
        f"Failed to mount after {max_retries} attempts: {last_error}"
    )
