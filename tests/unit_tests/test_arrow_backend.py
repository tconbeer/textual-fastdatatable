from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pytest
from textual_fastdatatable import ArrowBackend


@pytest.fixture
def pydict() -> dict[str, Sequence[str | int]]:
    return {
        "first column": [1, 2, 3, 4, 5],
        "two": ["a", "b", "c", "d", "asdfasdf"],
        "three": ["foo", "bar", "baz", "qux", "foofoo"],
    }


@pytest.fixture
def records(pydict: dict[str, Sequence[str | int]]) -> list[tuple[str | int, ...]]:
    header = tuple(pydict.keys())
    cols = list(pydict.values())
    num_rows = len(cols[0])
    data = [tuple([col[i] for col in cols]) for i in range(num_rows)]
    return [header, *data]


@pytest.fixture
def backend(pydict: dict[str, Sequence[str | int]]) -> ArrowBackend:
    return ArrowBackend.from_pydict(pydict)


def test_from_records(records: list[tuple[str | int, ...]]) -> None:
    backend = ArrowBackend.from_records(records)
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert tuple(backend.columns) == records[0]


def test_from_pydict(pydict: dict[str, Sequence[str | int]]) -> None:
    backend = ArrowBackend.from_pydict(pydict)
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert tuple(backend.columns) == tuple(pydict.keys())


def test_from_parquet(pydict: dict[str, Sequence[str | int]], tmp_path: Path) -> None:
    tbl = pa.Table.from_pydict(pydict)
    p = tmp_path / "test.parquet"
    pa.parquet.write_table(tbl, str(p))

    backend = ArrowBackend.from_parquet(p)
    assert backend.data.equals(tbl)


def test_column_content_widths(backend: ArrowBackend) -> None:
    assert backend.column_content_widths == [1, 8, 6]


def test_get_row_at(backend: ArrowBackend) -> None:
    assert backend.get_row_at(0) == [1, "a", "foo"]
    assert backend.get_row_at(4) == [5, "asdfasdf", "foofoo"]
    with pytest.raises(IndexError):
        backend.get_row_at(10)
    with pytest.raises(IndexError):
        backend.get_row_at(-1)


def test_get_column_at(backend: ArrowBackend) -> None:
    assert backend.get_column_at(0) == [1, 2, 3, 4, 5]
    assert backend.get_column_at(2) == ["foo", "bar", "baz", "qux", "foofoo"]

    with pytest.raises(IndexError):
        backend.get_column_at(10)


def test_get_cell_at(backend: ArrowBackend) -> None:
    assert backend.get_cell_at(0, 0) == 1
    assert backend.get_cell_at(4, 1) == "asdfasdf"
    with pytest.raises(IndexError):
        backend.get_cell_at(10, 0)
    with pytest.raises(IndexError):
        backend.get_cell_at(0, 10)


def test_append_column(backend: ArrowBackend) -> None:
    original_table = backend.data
    backend.append_column("new")
    assert backend.column_count == 4
    assert backend.row_count == 5
    assert backend.get_column_at(3) == [None] * backend.row_count

    backend.append_column("def", default="zzz")
    assert backend.column_count == 5
    assert backend.row_count == 5
    assert backend.get_column_at(4) == ["zzz"] * backend.row_count

    assert backend.data.select(["first column", "two", "three"]).equals(original_table)


def test_append_rows(backend: ArrowBackend) -> None:
    original_table = backend.data
    backend.append_rows([(6, "w", "x"), (7, "y", "z")])
    assert backend.column_count == 3
    assert backend.row_count == 7
    assert backend.column_content_widths == [1, 8, 6]

    backend.append_rows([(999, "w" * 12, "x" * 15)])
    assert backend.column_count == 3
    assert backend.row_count == 8
    assert backend.column_content_widths == [3, 12, 15]

    assert backend.data.slice(0, 5).equals(original_table)


def test_drop_row(backend: ArrowBackend) -> None:
    backend.drop_row(0)
    assert backend.row_count == 4
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 8, 6]

    backend.drop_row(3)
    assert backend.row_count == 3
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 1, 3]

    with pytest.raises(IndexError):
        backend.drop_row(3)


def test_update_cell(backend: ArrowBackend) -> None:
    backend.update_cell(0, 0, 0)
    assert backend.get_column_at(0) == [0, 2, 3, 4, 5]
    assert backend.row_count == 5
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 8, 6]

    backend.update_cell(3, 1, "z" * 50)
    assert backend.get_row_at(3) == [4, "z" * 50, "qux"]
    assert backend.row_count == 5
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 50, 6]


def test_sort(backend: ArrowBackend) -> None:
    original_table = backend.data
    original_col_one = list(backend.get_column_at(0)).copy()
    original_col_two = list(backend.get_column_at(1)).copy()
    backend.sort(by="two")
    assert backend.get_column_at(0) != original_col_one
    assert backend.get_column_at(1) == sorted(original_col_two)

    backend.sort(by=[("two", "descending")])
    assert backend.get_column_at(0) != original_col_one
    assert backend.get_column_at(1) == sorted(original_col_two, reverse=True)

    backend.sort(by=[("first column", "ascending")])
    assert backend.data.equals(original_table)


def test_empty_query() -> None:
    data: dict[str, list] = {"a": []}
    backend = ArrowBackend.from_pydict(data)
    assert backend.column_content_widths == [0]
