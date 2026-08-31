from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import date
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from textual_fastdatatable.backend import (
    LINE_BREAKS,
    ArrowBackend,
    DataTableBackend,
    PolarsBackend,
)
from textual_fastdatatable.format import measure_width

UUIDS = [uuid.UUID(int=i) for i in range(3)]


class _MultiLine:
    """A driver's own type, of the kind that prints more lines than a row shows."""

    def __str__(self) -> str:
        return "first line\nsecond line is longer"


def _array(values: Sequence[Any], type: pa.DataType | None = None) -> pa.Array:  # noqa: A002
    """`pa.array`, narrowed: these arrays are never chunked."""
    array = pa.array(values, type=type)
    assert isinstance(array, pa.Array)
    return array


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


def _uuid_array() -> pa.Array:
    """A column of the canonical `arrow.uuid` extension type, stored as its bytes."""
    storage = _array([value.bytes for value in UUIDS], type=pa.binary(16))
    return pa.ExtensionArray.from_storage(pa.uuid(), storage)


@pytest.mark.parametrize(
    "array,expected_width",
    [
        # the storage of an arrow.uuid is 16 bytes; a cell shows its 36 characters
        (_uuid_array(), 36),
        # an arrow.json is its storage's string, and is measured as one
        pytest.param(
            pa.ExtensionArray.from_storage(pa.json_(), _array(['{"a": 日}']))
            if hasattr(pa, "json_")
            else None,
            9,  # the wide character is two cells, as it is in a string column
            marks=pytest.mark.skipif(
                not hasattr(pa, "json_"), reason="pyarrow<19 has no arrow.json"
            ),
        ),
        # an arrow.bool8 is stored as an int8, and rendered as a bool: "✓ True "
        (
            pa.ExtensionArray.from_storage(pa.bool8(), _array([1, 0], type=pa.int8())),
            7,
        ),
        # an extension type nobody has a Python class for -- a geometry from a
        # database driver, say -- shows the preview its storage bytes render as
        (
            pa.ExtensionArray.from_storage(
                pa.opaque(pa.binary(), "GEOMETRY", "duckdb"),
                _array([b"\x01\x02"], type=pa.binary()),
            ),
            len(r"b'\x01\x02'"),
        ),
        # binary is not text either, however castable to it Arrow considers it
        (_array([b"\x00\x01\xff", b"hello", None], type=pa.binary()), 15),
        (_array([b"\xfe\xed"], type=pa.large_binary()), 11),
        # a preview renders literally, markup in the bytes and all, so it is
        # measured that way too: as markup, `b'[red]x'` would be four cells
        (_array([b"[red]x"], type=pa.binary()), len("b'[red]x'")),
        (_array([b"\x00\xff"] * 2, type=pa.binary(2)), 11),
        # a long value shows a bounded preview, which is wider than the bytes it
        # previews: 32 bytes of \xNN escapes, plus the count of the rest
        (
            _array([bytes(64)], type=pa.binary()),
            len(r"b'" + r"\x00" * 32 + r"'") + 12,
        ),
        # a dictionary is only as good as its values: Arrow renders a boolean as
        # `true`, where the widget shows `✓ True`
        (_array([True, False]).dictionary_encode(), 7),
        # a nested value is measured as Python prints it, which is how it renders
        (_array([[1, 2, 3], [4]], type=pa.list_(pa.int64())), 9),
        # a tag inside one renders as itself, so it is measured as itself
        (_array([["[red]x"]], type=pa.list_(pa.string())), len("['[red]x']")),
        (
            _array(
                [{"a": 1, "b": "x"}],
                type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
            ),
            18,
        ),
        # a string is text, and stays on Arrow's own fast path
        (_array(["日本語", "a"]), 6),
        (_array(["日本語", "a"]).dictionary_encode(), 6),
    ],
)
def test_arrow_columns_are_measured_as_the_widget_renders_them(
    array: pa.Array, expected_width: int
) -> None:
    """A column is as wide as its values render, whatever Arrow makes of its type.

    Arrow's cast to string reinterprets a binary type's bytes rather than failing."""
    backend = ArrowBackend(pa.table({"one": array}))

    assert backend.column_content_widths == [expected_width]
    # ... which is the width of the widest cell, as the widget renders it
    assert expected_width == max(
        measure_width(backend.get_cell_at(row, 0)) for row in range(backend.row_count)
    )


def test_uuid_columns_do_not_decode_their_storage_as_utf8() -> None:
    """Regression test for #176: measuring an arrow.uuid column raised.

    The cast reinterpreted the uuid's 16 bytes, and measuring decoded them as UTF-8."""
    backend = ArrowBackend(pa.table({"u": _uuid_array()}))

    assert backend.column_content_widths == [36]
    assert backend.get_cell_at(0, 0) == UUIDS[0]
    assert backend.get_row_at(1) == [UUIDS[1]]
    assert backend.get_column_at(0) == UUIDS


@pytest.mark.parametrize(
    "series,expected_width",
    [
        # polars raises rather than cast these to text, or renders them its own way
        (pl.Series([b"\x00\x01\xff", b"hello", None]), 15),
        (pl.Series([b"[red]x"]), len("b'[red]x'")),
        (pl.Series([[1, 2, 3], [4]]), 9),
        (pl.Series([[1, 2]], dtype=pl.Array(pl.Int64, 2)), 6),
        (pl.Series([{"a": 1, "b": "x"}]), 18),
        (pl.Series([date(2024, 1, 1)], dtype=pl.Object), 10),
        # a value that prints more lines than a row shows is measured as one line
        (pl.Series([_MultiLine()], dtype=pl.Object), len("first line") + 2),
        (pl.Series([None, None]), 0),
        # text, on the other hand, polars measures itself
        (pl.Series(["日本語", "a"]), 6),
        (pl.Series(["日本語", "a"], dtype=pl.Enum(["日本語", "a"])), 6),
        (pl.Series(["日本語", "a"], dtype=pl.Categorical), 6),
    ],
)
def test_polars_columns_are_measured_as_the_widget_renders_them(
    series: pl.Series, expected_width: int
) -> None:
    """The same, for the types polars cannot cast to the text a cell shows."""
    backend = PolarsBackend.from_dataframe(pl.DataFrame({"one": series}))

    assert backend.column_content_widths == [expected_width]
    assert expected_width == max(
        measure_width(backend.get_cell_at(row, 0)) for row in range(backend.row_count)
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ({"a": 1}, {"a": 1}),
        (b"\x00", b"\x00"),
    ],
)
def test_polars_cells_are_python_values(value: Any, expected: Any) -> None:
    """A nested cell is a python value, the way the Arrow backend gives it.

    `series[i]` hands back a Series, which renders as its own multi-line repr."""
    backend = PolarsBackend.from_dataframe(pl.DataFrame({"one": [value]}))

    assert backend.get_cell_at(0, 0) == expected
    assert backend.get_column_at(0) == [expected]
    assert backend.get_row_at(0) == [expected]


def test_all_null_extension_columns_measure_nothing() -> None:
    """Every value renders as the widget's null_rep, which the widget measures."""
    storage = _array([None, None], type=pa.binary(16))
    table = pa.table({"u": pa.ExtensionArray.from_storage(pa.uuid(), storage)})

    assert ArrowBackend(table).column_content_widths == [0]


@pytest.mark.parametrize(
    "array,converted",
    [
        # arrow.uuid renders every value 36 characters wide, so one is measured
        (_uuid_array(), 1),
        # an arrow.json is its storage's string: Arrow measures the column itself
        (
            pa.ExtensionArray.from_storage(pa.json_(), _array(['{"a": 1}'] * 3))
            if hasattr(pa, "json_")
            else _uuid_array(),
            0,
        ),
        # nothing is known about a struct's values, so every one is converted
        (
            _array([{"a": 1}] * 3, type=pa.struct([("a", pa.int64())])),
            3,
        ),
    ],
)
def test_extension_columns_are_not_converted_value_by_value(
    monkeypatch: pytest.MonkeyPatch, array: pa.Array, converted: int
) -> None:
    """What an extension type says about its values is what saves the conversion.

    A column of a million uuids measured every one of them before this counted.
    """
    from textual_fastdatatable import format as formatter

    calls = 0
    display_text = formatter.display_text

    def counted(*args: Any, **kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        return display_text(*args, **kwargs)

    monkeypatch.setattr(formatter, "display_text", counted)

    assert ArrowBackend(pa.table({"one": array})).column_content_widths
    assert calls == converted


@pytest.mark.parametrize("render_markup,expected_width", [(True, 3), (False, 11)])
def test_a_value_measured_in_python_follows_render_markup(
    render_markup: bool, expected_width: int
) -> None:
    """A string that reaches the value-by-value path is measured as it renders.

    It is the only value type the setting reaches, and it arrives as a bare object."""
    backend = PolarsBackend.from_dataframe(
        pl.DataFrame({"one": pl.Series(["[red]abc[/]"], dtype=pl.Object)})
    )
    backend.render_markup = render_markup

    assert backend.column_content_widths == [expected_width]
    assert expected_width == measure_width(
        backend.get_cell_at(0, 0), render_markup=render_markup
    )
