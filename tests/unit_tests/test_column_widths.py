from __future__ import annotations

import pytest
from rich.text import Text
from textual.app import App, ComposeResult

from textual_fastdatatable import DataTable
from textual_fastdatatable.format import MULTILINE_MARKER

WIDE_LABEL = "日本語"
"""Three double-width characters: three characters, six cells."""

COMBINING_LABEL = "señor"
"""Six characters, five cells: the combining tilde occupies none of its own."""


class TableApp(App):
    def __init__(self, table: DataTable) -> None:
        super().__init__()
        self.table = table

    def compose(self) -> ComposeResult:
        yield self.table


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "label,expected_render_width",
    [
        ("id", 4),  # wider than the one-cell data below it
        ("a_very_long_label", 19),
        (WIDE_LABEL, 8),
        (f"[red]{WIDE_LABEL}[/]", 8),  # markup is not rendered, so it is not measured
        (COMBINING_LABEL, 7),
    ],
)
async def test_column_is_wide_enough_for_its_label(
    label: str, expected_render_width: int
) -> None:
    """A column fits its label, measured in cells rather than counted in characters."""
    table = DataTable(data={label: ["a"]})
    app = TableApp(table)
    async with app.run_test():
        (column,) = table.ordered_columns
        assert column.render_width == expected_render_width


@pytest.mark.asyncio
async def test_wide_label_is_clamped_by_max_column_content_width() -> None:
    table = DataTable(data={WIDE_LABEL: ["a"]}, max_column_content_width=4)
    app = TableApp(table)
    async with app.run_test():
        (column,) = table.ordered_columns
        assert column.render_width == 6


@pytest.mark.asyncio
async def test_added_column_is_wide_enough_for_its_label() -> None:
    table = DataTable(data={"a": ["1"]})
    app = TableApp(table)
    async with app.run_test():
        table.add_column(Text(WIDE_LABEL))
        assert table.ordered_columns[1].render_width == 8


def test_columns_can_be_measured_without_an_app() -> None:
    """Labels are measured on first access, which may be before the widget mounts."""
    table = DataTable(data={WIDE_LABEL: ["a"]})

    (column,) = table.ordered_columns
    assert column.render_width == 8


@pytest.mark.asyncio
async def test_label_is_not_measured_wider_than_the_console() -> None:
    """A column is never wider than the console it is measured against."""
    table = DataTable(data={"a" * 200: ["a"]})
    app = TableApp(table)
    async with app.run_test(size=(80, 24)):
        (column,) = table.ordered_columns
        assert column.render_width == 82


@pytest.mark.asyncio
async def test_truncation_marker_survives_a_capped_column() -> None:
    """The marker is reserved out of the width, so the value gives way to it.

    Left to compete for the width, the marker is the tail rich clips off first,
    which would drop it in exactly the wide text columns whose values are most
    likely to carry lines below.
    """
    table = DataTable(
        data={"note": ["a first line too wide for the cap\nand a second line"]},
        max_column_content_width=20,
    )
    app = TableApp(table)
    async with app.run_test(size=(40, 6)):
        rendered = table.render_line(1).text

    assert rendered.rstrip().endswith(MULTILINE_MARKER)
    # ... and the ellipsis the marker opens with is the only one: rich adding a
    # second for the width would read as a typo
    assert rendered.count("…") == 1


@pytest.mark.asyncio
async def test_a_single_line_value_is_still_clipped_the_ordinary_way() -> None:
    """Nothing is reserved for a value with no lines below, so nothing changes."""
    table = DataTable(
        data={"note": ["a single line too wide for the cap"]},
        max_column_content_width=20,
    )
    app = TableApp(table)
    async with app.run_test(size=(40, 6)):
        rendered = table.render_line(1).text

    assert rendered.rstrip().endswith("…")
    assert MULTILINE_MARKER not in rendered
