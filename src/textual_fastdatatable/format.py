from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from itertools import chain
from typing import cast

from rich.align import Align
from rich.console import Console, ConsoleOptions, RenderableType
from rich.errors import MarkupError
from rich.markup import escape
from rich.protocol import is_renderable
from rich.segment import Segment, Segments
from rich.text import Text

from textual_fastdatatable.column import Column

MAX_MEASURE_WIDTH = 2**16
"""The width of the fallback console: with no app to measure against, nothing is known
about the screen a value will be rendered on, so nothing caps the measurement."""

_console: Console | None = None
"""The console to measure against when the caller has no app to borrow one from (the
backend never does). Built on the first measurement, so consumers that never measure
anything never build a Console."""

_console_options: ConsoleOptions | None = None
"""The render options of `_console`, built with it. `Console.options` builds a fresh
ConsoleOptions on every access, which is about half the cost of measuring a short
value; this console has a fixed width and is never resized, so its options are too."""


def measure_width(
    obj: object, console: Console | None = None, render_markup: bool = True
) -> int:
    """The width, in cells, needed to render one value: a cell, or a column label.

    This is the one place widths are measured. They cannot be counted with `len()`:
    a character can occupy two cells (CJK, many emoji) or none (a combining mark),
    and a value is measured as `cell_formatter` will render it, not as it is stored.
    The measurement is capped by the width of `console`, so that a column measured
    against an app is never wider than its screen.

    render_markup must match the widget's, so that a string is measured as it will
    be rendered: `[dim]a[/]` is one cell as markup and eleven cells literally.
    """
    global _console, _console_options
    options = None
    if console is None:
        if _console is None:
            # the flags Textual builds its own console with, so that a value measures
            # the width it will be rendered at (`:smile:` stays seven cells, not two)
            _console = Console(
                width=MAX_MEASURE_WIDTH, markup=True, emoji=False, highlight=False
            )
            _console_options = _console.options
        console, options = _console, _console_options
    return console.measure(
        cell_formatter(obj, null_rep=Text(""), render_markup=render_markup),
        options=options,
    ).maximum


def cell_formatter(
    obj: object, null_rep: Text, col: Column | None = None, render_markup: bool = True
) -> RenderableType:
    """Convert a cell into a Rich renderable for display.

    For correct formatting, clients should call `locale.setlocale()` first.

    Args:
        obj: Data for a cell.
        col: Column that the cell came from (used to compute width).

    Returns:
        A renderable to be displayed which represents the data.
    """
    if obj is None:
        return Align(null_rep, align="center")

    elif isinstance(obj, str) and render_markup:
        try:
            rich_text: Text | str = Text.from_markup(obj)
        except MarkupError:
            rich_text = escape(obj)
        return rich_text

    elif isinstance(obj, str):
        return escape(obj)

    elif isinstance(obj, bool):
        return Align(
            f"[dim]{'✓' if obj else 'X'}[/] {obj}{' ' if obj else ''}",
            style="bold" if obj else "",
            align="right",
        )

    elif isinstance(obj, (float, Decimal)):
        return Align(f"{obj:n}", align="right")

    elif isinstance(obj, int):
        if col is not None and col.is_id:
            # no separators in ID fields
            return Align(str(obj), align="right")
        else:
            return Align(f"{obj:n}", align="right")

    elif isinstance(obj, (datetime, time)):

        def _fmt_datetime(obj: datetime | time) -> str:
            return obj.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        if obj in (datetime.max, datetime.min):
            return Align(
                (
                    f"[bold]{'∞ ' if obj == datetime.max else '-∞ '}[/]"
                    f"[dim]{_fmt_datetime(obj)}[/]"
                ),
                align="right",
            )

        return Align(_fmt_datetime(obj), align="right")

    elif isinstance(obj, date):
        if obj in (date.max, date.min):
            return Align(
                (
                    f"[bold]{'∞ ' if obj == date.max else '-∞ '}[/]"
                    f"[dim]{obj.isoformat()}[/]"
                ),
                align="right",
            )

        return Align(obj.isoformat(), align="right")

    elif isinstance(obj, timedelta):
        return Align(str(obj), align="right")

    elif isinstance(obj, (bytes, bytearray, memoryview)):
        # binary values (e.g. varbinary columns) can contain sequences like
        # [/...] that Rich would try to parse as markup; show an escaped,
        # truncated preview instead. See tconbeer/harlequin#974.
        data = bytes(obj)
        preview = repr(data[:32])
        if len(data) > 32:
            preview = f"{preview} (+{len(data) - 32} bytes)"
        return escape(preview)

    elif not is_renderable(obj):
        return str(obj)

    else:
        return cast(RenderableType, obj)


def truncate_renderable(
    renderable: RenderableType, console: Console, max_width: int, max_lines: int
) -> RenderableType:
    """Clip a renderable so it fits in a box of max_width x max_lines.

    Args:
        renderable: A Rich renderable.
        console: The console used to render the renderable.
        max_width: The width (in cells) the renderable will be rendered at.
        max_lines: The maximum number of lines the returned renderable may occupy.

    Returns:
        The original renderable, if it already fits; otherwise a renderable of
        exactly max_lines lines, the last of which marks the content as truncated.
    """
    if max_lines < 2 or max_width < 1:
        return renderable
    options = console.options.update(width=max_width, height=None, overflow="fold")
    # new_lines=True terminates every line, including the last: Textual sizes a
    # renderable by counting its newlines, and would otherwise clip the marker.
    lines = console.render_lines(renderable, options, pad=False, new_lines=True)
    if len(lines) <= max_lines:
        return renderable

    ellipsis = Text("… (truncated)", style="italic dim", no_wrap=True)
    return Segments(
        [
            *chain.from_iterable(lines[: max_lines - 1]),
            *ellipsis.render(console),
            Segment.line(),
        ]
    )
