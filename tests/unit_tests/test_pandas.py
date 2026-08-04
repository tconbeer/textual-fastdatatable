from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from textual_fastdatatable import ArrowBackend, DataTable, create_backend

pd = pytest.importorskip("pandas", reason="pandas is not installed")


@pytest.fixture
def frame() -> Any:
    return pd.DataFrame(
        {
            "one": [1, 2, 3],
            "two": ["a", "b", "c"],
            "three": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        }
    )


def test_create_backend_from_pandas(frame: Any) -> None:
    backend = create_backend(frame)

    assert isinstance(backend, ArrowBackend)
    assert backend.row_count == 3
    assert backend.column_count == 3
    assert list(backend.columns) == ["one", "two", "three"]
    assert list(backend.get_row_at(0)) == [1, "a", date(2024, 1, 1)]
    assert list(backend.get_column_at(1)) == ["a", "b", "c"]
    assert backend.get_cell_at(2, 0) == 3


def test_create_backend_from_pandas_max_rows(frame: Any) -> None:
    backend = create_backend(frame, max_rows=2)

    assert backend.row_count == 2
    assert backend.source_row_count == 3


def test_data_table_from_pandas(frame: Any) -> None:
    table = DataTable(data=frame)

    assert table.backend is not None
    assert table.row_count == 3
    assert table.column_count == 3


def test_pandas_needs_no_extras(frame: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """pyarrow converts the frame, so the polars extra is not involved."""
    monkeypatch.setattr("textual_fastdatatable.backend._HAS_POLARS", False)

    backend = create_backend(frame)

    assert isinstance(backend, ArrowBackend)
    assert backend.row_count == 3


def test_pandas_index_is_not_a_column(frame: Any) -> None:
    """The index is dropped; reset_index() promotes it to a column."""
    indexed = frame.set_index("one")

    assert list(create_backend(indexed).columns) == ["two", "three"]
    assert list(create_backend(indexed.reset_index()).columns) == [
        "one",
        "two",
        "three",
    ]


def test_non_dataframe_still_raises() -> None:
    with pytest.raises(TypeError, match="Cannot automatically create backend"):
        create_backend(object())
