from typing import TYPE_CHECKING, Any

from textual_fastdatatable.backend import (
    ArrowBackend,
    DataTableBackend,
    create_backend,
)

if TYPE_CHECKING:
    from textual_fastdatatable.data_table import DataTable

__all__ = [
    "DataTable",
    "ArrowBackend",
    "DataTableBackend",
    "create_backend",
]


def __getattr__(name: str) -> Any:
    """Lazily import the DataTable widget (PEP 562).

    Importing the widget imports Textual; keeping it out of module scope means
    `textual_fastdatatable.backend` can be imported without paying for the
    framework.
    """
    if name == "DataTable":
        from textual_fastdatatable.data_table import DataTable

        return DataTable
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
