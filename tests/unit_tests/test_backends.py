from __future__ import annotations

import pytest

from textual_fastdatatable.backend import (
    LINE_BREAKS,
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
        ("a\tb", 3),  # ASCII, tab included: Arrow's character count is its width
        # a row is one line tall, so only the first line of a multi-line value is
        # rendered, followed by the two-cell truncation marker
        ("日\nbbbb", 4),
        ("aaaa\rb", 6),  # a carriage return ends the first line too
        ("\nbbbb", 2),  # a value that starts with a break renders as the marker alone
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


@pytest.mark.parametrize(
    "value,markup_width,literal_width",
    [
        # rich renders the tags away; without markup they are just characters
        ("[dim]日本[/]", 4, 12),
        ("[red]abc[/]", 3, 11),
        # rich is the authority on what markup means, not a regex approximating it:
        # it renders "[[x]]" as "[]", and refuses "[/]x", which is escaped instead
        ("日本[[x]]", 6, 9),
        ("[[red]]", 2, 7),
        ("[/]x", 4, 4),
        # a value with a bracket but no tag in it renders as itself either way
        ('{"a": [1, 2]}', 13, 13),
        ("a[b", 3, 3),
    ],
)
def test_column_content_widths_follow_render_markup(
    backend_class: type[ArrowBackend] | type[PolarsBackend],
    value: str,
    markup_width: int,
    literal_width: int,
) -> None:
    """A value is measured as it will be rendered: as markup, or literally."""
    for render_markup, expected_width in ((True, markup_width), (False, literal_width)):
        backend = backend_class.from_pydict({"one": [value, "a"]})
        backend.render_markup = render_markup

        assert backend.column_content_widths == [expected_width]


def test_line_breaks_match_the_formatters() -> None:
    """`backend` restates `format`'s rules because it cannot import it.

    Either constant out of step measures every multi-line value wrong.
    """
    from textual_fastdatatable.backend import _MARKER_WIDTH
    from textual_fastdatatable.format import (
        LINE_BREAK_PROG,
        MULTILINE_MARKER,
        MULTILINE_MARKER_WIDTH,
        measure_width,
    )

    assert measure_width(MULTILINE_MARKER) == MULTILINE_MARKER_WIDTH == _MARKER_WIDTH
    for char in map(chr, range(0x110000)):
        assert (LINE_BREAK_PROG.search(char) is not None) == (char in LINE_BREAKS), (
            f"format and backend disagree about {char!r}"
        )


@pytest.mark.parametrize(
    "value,expected_width",
    [
        ("aaaa\nbb", 6),
        ("aaaa\r\nbb", 6),  # the CR ends the line, and the LF after it changes nothing
        ("aaaa\rbb", 6),
        ("bb\naaaa", 4),  # the first line is measured, not the widest one
        ("\naaaa", 2),
        ("aaaa\n", 6),
    ],
)
def test_multiline_widths_are_measured_over_a_whole_column(
    backend_class: type[ArrowBackend] | type[PolarsBackend],
    value: str,
    expected_width: int,
) -> None:
    """Arrow measures a column of ASCII values without ever calling into python.

    Values with no break at all, and enough of them, so that the vectorized path
    is what answers rather than the per-value one.
    """
    rows = ["a"] * 5_000 + [value] + ["a"] * 5_000
    backend = backend_class.from_pydict({"one": rows})

    assert backend.column_content_widths == [expected_width]


def test_line_breaks_are_found_past_the_first_scanned_block() -> None:
    """The byte scan reads a block at a time; a break in a later block still counts."""
    import pyarrow as pa

    from textual_fastdatatable.backend import _SCAN_BLOCK_SIZE, _line_breaks_present

    def string_array(values: list[str]) -> pa.Array:
        array = pa.array(values, type=pa.string())
        assert isinstance(array, pa.Array)
        return array

    filler = "a" * 1_000
    rows = [filler] * (_SCAN_BLOCK_SIZE // len(filler) + 10)

    assert _line_breaks_present(string_array(rows)) == frozenset()
    assert _line_breaks_present(string_array([*rows, "b\nc"])) == frozenset("\n")
    assert _line_breaks_present(string_array([*rows, "b\r\nc"])) == LINE_BREAKS


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
