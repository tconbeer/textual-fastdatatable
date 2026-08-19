from __future__ import annotations

import asyncio
import json
from typing import NamedTuple

import pytest
from rich.segment import Segments
from rich.text import Text
from textual.app import App, ComposeResult
from textual.geometry import Offset, Region
from textual.widgets import Tooltip

from textual_fastdatatable import DataTable

LONG_VALUE = json.dumps({f"key_{i}": f"value_{i}" * 5 for i in range(200)})
TERMINAL_SIZE = (80, 24)


class TooltipApp(App):
    TOOLTIP_DELAY = 0.05

    def __init__(self, value: str) -> None:
        super().__init__()
        self.value = value

    def compose(self) -> ComposeResult:
        # the tooltip only appears for values wider than the rendered column
        yield DataTable(data={"json": [self.value] * 40}, max_column_content_width=20)


class WideColumnTooltipApp(TooltipApp):
    """An app whose column is wide enough for every line of its value."""

    def compose(self) -> ComposeResult:
        yield DataTable(data={"txt": [self.value] * 40})


class RestyledTooltipApp(TooltipApp):
    """An app that restyles the tooltip, the way harlequin does."""

    CSS = """
    Tooltip {
        max-width: 72;
        padding: 0 1;
        margin: 2 0;
    }
    """


class HoverResult(NamedTuple):
    """The state of a tooltip while its app was still running."""

    displayed: bool
    displayed_after_refresh: bool
    region: Region
    mouse_position: Offset
    content: object


async def _hover_middle_cell(app: TooltipApp) -> HoverResult:
    """Hover a cell halfway down the screen and observe the tooltip."""
    async with app.run_test(tooltips=True, size=TERMINAL_SIZE) as pilot:
        await pilot.hover(DataTable, offset=(2, TERMINAL_SIZE[1] // 2))
        await asyncio.sleep(app.TOOLTIP_DELAY * 4)
        await pilot.pause()
        tooltip = app.screen.get_child_by_type(Tooltip)
        displayed = tooltip.display
        result = HoverResult(
            displayed=displayed,
            displayed_after_refresh=displayed,
            region=tooltip.region,
            mouse_position=app.mouse_position,
            content=app.query_one(DataTable).tooltip,
        )
        # Textual re-checks the widget under the mouse after every layout
        # refresh, and clears a tooltip that has covered its own widget.
        app.screen.screen_layout_refresh_signal.publish(app.screen)
        await pilot.pause()
        return result._replace(displayed_after_refresh=tooltip.display)


@pytest.mark.asyncio
async def test_long_value_tooltip_does_not_cover_the_mouse() -> None:
    """A tooltip that covers the mouse flickers forever.

    Textual places the tooltip at the mouse and clamps it to the screen if it
    fits neither below nor above; it then clears any tooltip that is no longer
    under its own widget, so an oversized tooltip is shown and hidden over and
    over. Regression test for https://github.com/tconbeer/harlequin/issues/894
    """
    app = TooltipApp(LONG_VALUE)
    result = await _hover_middle_cell(app)
    assert result.displayed, "no tooltip was shown for a long cell value"
    assert result.region.height < TERMINAL_SIZE[1]
    assert not result.region.contains_point(result.mouse_position)
    assert result.displayed_after_refresh, "the tooltip was cleared, and will flicker"


@pytest.mark.asyncio
async def test_long_value_tooltip_is_marked_truncated() -> None:
    app = TooltipApp(LONG_VALUE)
    result = await _hover_middle_cell(app)
    assert isinstance(result.content, Segments)
    text = "".join(segment.text for segment in result.content.segments)
    assert text.startswith('{"key_0"')
    assert text.rstrip("\n").endswith("… (truncated)")
    # every line ends in a newline; Textual sizes a renderable by counting them,
    # so without the last one the truncation marker would be clipped
    assert len(text.splitlines()) == text.count("\n")
    assert result.region.height >= len(text.splitlines())


@pytest.mark.asyncio
async def test_tooltip_is_measured_from_the_tooltip_widgets_own_styles() -> None:
    """An app may restyle the tooltip; its size must not be assumed."""
    app = RestyledTooltipApp(LONG_VALUE)
    result = await _hover_middle_cell(app)
    assert result.displayed
    assert result.displayed_after_refresh
    assert not result.region.contains_point(result.mouse_position)

    assert isinstance(result.content, Segments)
    text = "".join(segment.text for segment in result.content.segments)
    lines = text.splitlines()
    # the app's own max-width (72) and padding (0 1) are used, not Textual's
    # default 40 with padding 1 2, which would wrap the content at 36 cells
    assert result.region.width == 72
    assert 36 < max(len(line) for line in lines) <= 70


@pytest.mark.asyncio
async def test_short_value_tooltip_is_not_truncated() -> None:
    """Values that fit are still passed to the tooltip unchanged."""
    app = TooltipApp("a" * 30)
    result = await _hover_middle_cell(app)
    assert result.displayed
    assert result.content is not None
    assert not isinstance(result.content, Segments)


@pytest.mark.asyncio
async def test_multiline_value_gets_a_tooltip_however_narrow_it_is() -> None:
    """A cell shows one line of a multi-line value; the tooltip shows the rest.

    The column is wide enough for every line here, so width alone would never
    trigger a tooltip -- but the cell still only renders the first line.
    See tconbeer/harlequin#635 and #771.
    """
    app = WideColumnTooltipApp("one\ntwo\nthree")
    result = await _hover_middle_cell(app)
    assert result.displayed, "no tooltip was shown for a multi-line cell value"
    assert isinstance(result.content, Text)
    assert result.content.plain == "one\ntwo\nthree"


@pytest.mark.asyncio
async def test_single_line_value_that_fits_gets_no_tooltip() -> None:
    """Nothing is hidden, so there is nothing to show."""
    app = WideColumnTooltipApp("one line")
    result = await _hover_middle_cell(app)
    assert result.content is None
