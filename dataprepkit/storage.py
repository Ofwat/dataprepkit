"""Helper utilities for managing Fabric lakehouse mounts."""

from __future__ import annotations

import time
from typing import Iterable

try:
    from notebookutils import fs
except ImportError:  # pragma: no cover - Fabric only
    fs = None  # type: ignore[assignment]

try:
    import sempy.fabric as fabric
except ImportError:  # pragma: no cover - Fabric only
    fabric = None  # type: ignore[assignment]


class StorageMountError(RuntimeError):
    """Raised when a mount operation cannot be completed."""


def ensure_mount(
    base_path: str,
    mount_point: str,
    *,
    retries: int = 20,
    delay_seconds: float = 1.0,
    force_unmount: bool = True,
) -> str:
    """Ensure the given Fabric lakehouse path is mounted at the requested mount point."""
    if fs is None:
        raise ImportError("notebookutils.fs is required to manage Fabric mounts.")

    if force_unmount:
        try:
            fs.unmount(mount_point)
        except Exception:  # pragma: no cover
            pass

    for attempt in range(retries):
        try:
            fs.mount(base_path, mount_point)
            return fs.getMountPath(mount_point)
        except Exception:  # pragma: no cover
            if attempt == retries - 1:
                raise StorageMountError(
                    "Failed to mount lakehouse after "
                    f"{retries} attempts for {mount_point}"
                )
            time.sleep(delay_seconds)


def _find_resource_id(items: Iterable[dict], key: str, value: str) -> str:
    for item in items:
        if item.get(key) == value:
            return item["Id"]
    raise StorageMountError(f"Could not locate resource with {key} == {value}")


def mount_lakehouse(
    workspace_name: str,
    lakehouse_display_name: str,
    mount_point: str,
    *,
    retries: int = 20,
    delay_seconds: float = 1.0,
    force_unmount: bool = True,
) -> str:
    """
    Mount a Fabric lakehouse path by workspace/display name before returning the mount path.

    Parameters
    ----------
    workspace_name : str
        Fabric workspace display name.
    lakehouse_display_name : str
        Display name of the lakehouse to mount.
    mount_point : str
        Local mount point.

    Other parameters mimic :func:`ensure_mount`.
    """
    if fabric is None:
        raise ImportError("sempy.fabric is required to locate Fabric resources.")

    ws_df = fabric.list_workspaces()
    workspace_id = _find_resource_id(ws_df.to_dict("records"), "Display Name", workspace_name)
    items = fabric.list_items(workspace=workspace_id)
    lakehouse_id = _find_resource_id(
        items.to_dict("records"), "Display Name", lakehouse_display_name
    )
    base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"

    return ensure_mount(
        base_path,
        mount_point,
        retries=retries,
        delay_seconds=delay_seconds,
        force_unmount=force_unmount,
    )
