from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from textual_fastdatatable.backend import ArrowBackend, PolarsBackend, create_backend

MAX_32BIT_INT = 2**31 - 1
MAX_64BIT_INT = 2**63 - 1


def test_empty_sequence() -> None:
    backend = create_backend(data=[])
    assert backend
    assert backend.row_count == 0
    assert backend.column_count == 0
    assert backend.columns == []
    assert backend.column_content_widths == []


def test_none_without_column_names() -> None:
    with pytest.raises(TypeError):
        create_backend(data=None)


@pytest.mark.parametrize("data", [None, [], ()])
def test_no_data_with_column_names(data: None | list | tuple) -> None:
    backend = create_backend(data=data, column_names=["a", "b"])
    assert backend.row_count == 0
    assert backend.column_count == 2
    assert list(backend.columns) == ["a", "b"]
    assert backend.column_content_widths == [0, 0]


def test_no_data_with_empty_column_names() -> None:
    backend = create_backend(data=None, column_names=[])
    assert backend.row_count == 0
    assert backend.column_count == 0
    assert list(backend.columns) == []


def test_records_with_column_names() -> None:
    backend = create_backend(data=[(1, "x"), (2, "y")], column_names=["one", "two"])
    assert list(backend.columns) == ["one", "two"]
    assert backend.row_count == 2
    assert backend.get_row_at(0) == [1, "x"]
    assert backend.get_column_at(1) == ["x", "y"]


def test_records_without_column_names() -> None:
    backend = create_backend(data=[(1, "x"), (2, "y")])
    assert list(backend.columns) == ["f0", "f1"]


def test_records_with_column_names_wins_over_header() -> None:
    """The header row is still consumed; the caller's names win over it."""
    backend = create_backend(
        data=[("h1", "h2"), (1, "x"), (2, "y")],
        has_header=True,
        column_names=["one", "two"],
    )
    assert list(backend.columns) == ["one", "two"]
    assert backend.row_count == 2
    assert backend.get_row_at(0) == [1, "x"]


def test_records_with_mismatched_column_names() -> None:
    backend = create_backend(data=[(1, "x"), (2, "y")], column_names=["only one"])
    assert list(backend.columns) == ["f0", "f1"]


def test_arrow_table_with_column_names() -> None:
    backend = create_backend(
        data=pa.table({"a": [1, 2], "b": [3, 4]}), column_names=["one", "two"]
    )
    assert list(backend.columns) == ["one", "two"]
    assert list(backend.source_data.column_names) == ["one", "two"]
    assert backend.get_row_at(0) == [1, 3]


def test_arrow_table_with_mismatched_column_names() -> None:
    """A mismatch means a misbehaving caller; the data's own names win."""
    data = pa.table({"a": [1, 2], "b": [3, 4]})
    backend = create_backend(data=data, column_names=["one", "two", "three"])
    assert list(backend.columns) == ["a", "b"]
    assert backend.get_row_at(0) == [1, 3]


def test_arrow_table_without_columns_with_column_names() -> None:
    backend = create_backend(data=pa.table([]), column_names=["a", "b"])
    assert list(backend.columns) == ["a", "b"]
    assert backend.row_count == 0


def test_record_batch_with_column_names() -> None:
    batch = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [3, 4]})
    backend = create_backend(data=batch, column_names=["one", "two"])
    assert list(backend.columns) == ["one", "two"]
    assert backend.get_row_at(1) == [2, 4]


def test_pydict_with_column_names() -> None:
    backend = create_backend(data={"a": [1, 2], "b": [3, 4]}, column_names=["one", "b"])
    assert list(backend.columns) == ["one", "b"]
    assert backend.get_row_at(0) == [1, 3]


def test_duplicate_column_names_reach_source_data() -> None:
    """`select 1 as a, 2 as a` is legal SQL; source_data keeps both names."""
    backend = create_backend(data=[(1, 2)], column_names=["a", "a"])
    assert isinstance(backend, ArrowBackend)
    assert list(backend.source_data.column_names) == ["a", "a"]
    # display needs unique names, so backend.data de-duplicates them
    assert list(backend.columns) == ["a", "a0"]
    assert backend.get_row_at(0) == [1, 2]


def test_duplicate_column_names_from_arrow_table() -> None:
    data = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "y"])
    backend = create_backend(data=data, column_names=["a", "a"])
    assert isinstance(backend, ArrowBackend)
    assert list(backend.source_data.column_names) == ["a", "a"]
    assert list(backend.columns) == ["a", "a0"]


def test_column_names_with_max_rows() -> None:
    backend = create_backend(
        data=[(1, "x"), (2, "y"), (3, "z")], max_rows=2, column_names=["one", "two"]
    )
    assert list(backend.columns) == ["one", "two"]
    assert backend.row_count == 2
    assert backend.source_row_count == 3


def test_polars_dataframe_with_column_names() -> None:
    backend = create_backend(
        data=pl.DataFrame({"a": [1, 2], "b": [3, 4]}), column_names=["one", "two"]
    )
    assert isinstance(backend, PolarsBackend)
    assert list(backend.columns) == ["one", "two"]
    assert backend.get_row_at(0) == [1, 3]


def test_polars_dataframe_with_mismatched_column_names() -> None:
    backend = create_backend(
        data=pl.DataFrame({"a": [1, 2], "b": [3, 4]}), column_names=["one"]
    )
    assert list(backend.columns) == ["a", "b"]


def test_csv_with_column_names(tmp_path: Path) -> None:
    path = tmp_path / "headerless.csv"
    path.write_text("1,x\n2,y\n")
    backend = create_backend(data=path, has_header=False, column_names=["one", "two"])
    assert isinstance(backend, PolarsBackend)
    assert list(backend.columns) == ["one", "two"]
    assert backend.row_count == 2


def test_parquet_with_column_names(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"a": [1, 2], "b": [3, 4]}), str(path))
    backend = create_backend(data=path, column_names=["one", "two"])
    assert list(backend.columns) == ["one", "two"]
    assert backend.row_count == 2


def test_infinity_timestamps() -> None:
    from_py = create_backend(
        data={"dt": [date.max, date.min], "ts": [datetime.max, datetime.min]}
    )
    assert from_py
    assert from_py.row_count == 2

    from_arrow = create_backend(
        data=pa.table(
            {
                "dt32": [
                    pa.scalar(MAX_32BIT_INT, type=pa.date32()),
                    pa.scalar(-MAX_32BIT_INT, type=pa.date32()),
                ],
                "dt64": [
                    pa.scalar(MAX_64BIT_INT, type=pa.date64()),
                    pa.scalar(-MAX_64BIT_INT, type=pa.date64()),
                ],
                "ts": [
                    pa.scalar(MAX_64BIT_INT, type=pa.timestamp("s")),
                    pa.scalar(-MAX_64BIT_INT, type=pa.timestamp("s")),
                ],
                "tns": [
                    pa.scalar(MAX_64BIT_INT, type=pa.timestamp("ns")),
                    pa.scalar(-MAX_64BIT_INT, type=pa.timestamp("ns")),
                ],
            }
        )
    )
    assert from_arrow
    assert from_arrow.row_count == 2
    assert from_arrow.get_row_at(0) == [date.max, date.max, datetime.max, datetime.max]
    assert from_arrow.get_row_at(1) == [date.min, date.min, datetime.min, datetime.min]
    assert from_arrow.get_column_at(0) == [date.max, date.min]
    assert from_arrow.get_column_at(2) == [datetime.max, datetime.min]
    assert from_arrow.get_cell_at(0, 0) == date.max
