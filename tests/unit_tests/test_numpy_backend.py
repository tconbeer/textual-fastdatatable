from __future__ import annotations

import numpy as np
import pytest
from textual_fastdatatable import NumpyBackend


@pytest.fixture
def records() -> list[tuple[int, str, str]]:
    return [
        (1, "a", "foo"),
        (2, "b", "bar"),
        (3, "c", "baz"),
        (4, "d", "qux"),
        (5, "asdfasdf", "foofoo"),
    ]


@pytest.fixture
def backend(records: list[tuple[int, str, str]]) -> NumpyBackend:
    return NumpyBackend(records)


def test_from_records(backend: NumpyBackend) -> None:
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert backend.source_row_count == 5
    assert backend.columns == ("f0", "f1", "f2")


def test_from_records_with_limit(records: list[tuple[int, str, str]]) -> None:
    backend = NumpyBackend(records, max_rows=2)
    assert backend.column_count == 3
    assert backend.row_count == 2
    assert backend.source_row_count == 5
    assert isinstance(backend.data, np.recarray)
    assert backend.columns == ("f0", "f1", "f2")


def test_column_content_widths(backend: NumpyBackend) -> None:
    assert backend.column_content_widths == [1, 8, 6]


def test_get_row_at(backend: NumpyBackend) -> None:
    assert backend.get_row_at(0) == (1, "a", "foo")
    assert backend.get_row_at(4) == (5, "asdfasdf", "foofoo")
    with pytest.raises(IndexError):
        backend.get_row_at(10)
    with pytest.raises(IndexError):
        backend.get_row_at(-1)


def test_get_column_at(backend: NumpyBackend) -> None:
    assert backend.get_column_at(0) == [1, 2, 3, 4, 5]
    assert backend.get_column_at(2) == ["foo", "bar", "baz", "qux", "foofoo"]

    with pytest.raises(IndexError):
        backend.get_column_at(10)


def test_get_cell_at(backend: NumpyBackend) -> None:
    assert backend.get_cell_at(0, 0) == 1
    assert backend.get_cell_at(4, 1) == "asdfasdf"
    with pytest.raises(IndexError):
        backend.get_cell_at(10, 0)
    with pytest.raises(IndexError):
        backend.get_cell_at(0, 10)


def test_append_column(backend: NumpyBackend) -> None:
    original_table = backend.data
    backend.append_column("new")
    assert backend.column_count == 4
    assert backend.row_count == 5
    assert all(np.isnan(backend.get_column_at(3)))

    backend.append_column("def", default="zzz")
    assert backend.column_count == 5
    assert backend.row_count == 5
    assert backend.get_column_at(4) == ["zzz"] * backend.row_count

    assert np.array_equal(backend.data[["f0", "f1", "f2"]], original_table)


def test_append_rows(backend: NumpyBackend) -> None:
    original_table = backend.data
    backend.append_rows([(6, "w", "x"), (7, "y", "z")])
    assert backend.column_count == 3
    assert backend.row_count == 7
    assert backend.column_content_widths == [1, 8, 6]

    backend.append_rows([(999, "w" * 12, "x" * 15)])
    assert backend.column_count == 3
    assert backend.row_count == 8
    assert backend.column_content_widths == [3, 12, 15]

    assert np.array_equal(backend.data[0:5], original_table)


def test_drop_row(backend: NumpyBackend) -> None:
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


def test_update_cell(backend: NumpyBackend) -> None:
    backend.update_cell(0, 0, 0)
    assert backend.get_column_at(0) == [0, 2, 3, 4, 5]
    assert backend.row_count == 5
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 8, 6]

    backend.update_cell(3, 1, "z" * 50)
    assert backend.get_row_at(3) == (4, "z" * 50, "qux")
    assert backend.row_count == 5
    assert backend.column_count == 3
    assert backend.column_content_widths == [1, 50, 6]


def test_sort(backend: NumpyBackend) -> None:
    original_table = np.copy(backend.data)
    original_col_one = backend.get_column_at(0).copy()
    original_col_two = backend.get_column_at(1).copy()
    backend.sort(by="f1")
    assert backend.get_column_at(0) != original_col_one
    assert backend.get_column_at(1) == sorted(original_col_two)

    backend.sort(by=[("f1", "descending")])
    assert backend.get_column_at(0) != original_col_one
    assert backend.get_column_at(1) == sorted(original_col_two, reverse=True)

    backend.sort(by=[("f0", "ascending")])
    assert np.array_equal(backend.data, original_table)


def test_empty_query() -> None:
    backend = NumpyBackend(data=[])
    assert backend.column_content_widths == [0]
