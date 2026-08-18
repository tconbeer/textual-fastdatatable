from __future__ import annotations

import pytest

from textual_fastdatatable.backend import (
    ArrowBackend,
    DataTableBackend,
    PolarsBackend,
)


def test_column_content_widths(backend: DataTableBackend) -> None:
    assert backend.column_content_widths == [1, 8, 6]


@pytest.mark.parametrize(
    "value,expected_width",
    [
        ("hello", 5),  # ascii: one cell per character
        ("日本語", 6),  # three double-width characters
        ("🙂x", 3),  # a double-width emoji
        ("señor", 5),  # six characters, one of them a zero-width combining tilde
        ("a\tb", 3),  # wcwidth measures a control character as -1; count instead
    ],
)
def test_column_content_widths_are_measured_in_cells(
    backend_class: type[ArrowBackend] | type[PolarsBackend],
    value: str,
    expected_width: int,
) -> None:
    """A column is as wide as its strings render, not as many chars as they have."""
    backend = backend_class.from_pydict({"one": [value, "a"]})

    assert backend.column_content_widths == [expected_width]


def test_column_content_widths_are_repeatable(
    backend_class: type[ArrowBackend] | type[PolarsBackend],
) -> None:
    """Regression test: measuring re-registered the UDF, which segfaulted pyarrow.

    `pc.register_scalar_function` raises for a name that is taken, and drops a
    reference to the function already registered under it, so the third call to that
    function crashed the interpreter.
    """
    backend = backend_class.from_pydict({f"col {i}": ["日本語", "a"] for i in range(6)})

    assert backend.column_content_widths == [6] * 6
    assert backend.column_content_widths == [6] * 6


def test_get_row_at(backend: DataTableBackend) -> None:
    assert backend.get_row_at(0) == [1, "a", "foo"]
    assert backend.get_row_at(4) == [5, "asdfasdf", "foofoo"]
    with pytest.raises(IndexError):
        backend.get_row_at(10)
    with pytest.raises(IndexError):
        backend.get_row_at(-1)


def test_get_column_at(backend: DataTableBackend) -> None:
    assert backend.get_column_at(0) == [1, 2, 3, 4, 5]
    assert backend.get_column_at(2) == ["foo", "bar", "baz", "qux", "foofoo"]

    with pytest.raises(IndexError):
        backend.get_column_at(10)


def test_get_cell_at(backend: DataTableBackend) -> None:
    assert backend.get_cell_at(0, 0) == 1
    assert backend.get_cell_at(4, 1) == "asdfasdf"
    with pytest.raises(IndexError):
        backend.get_cell_at(10, 0)
    with pytest.raises(IndexError):
        backend.get_cell_at(0, 10)


def test_append_column(backend: DataTableBackend) -> None:
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


def test_append_rows(backend: DataTableBackend) -> None:
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


def test_drop_row(backend: DataTableBackend) -> None:
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


def test_update_cell(backend: DataTableBackend) -> None:
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


def test_sort(backend: DataTableBackend) -> None:
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
