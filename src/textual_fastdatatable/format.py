from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import cast

from rich.align import Align
from rich.console import Console, RenderableType
from rich.errors import MarkupError
from rich.markup import escape
from rich.protocol import is_renderable
from rich.segment import Segment, Segments
from rich.text import Text

from textual_fastdatatable.column import Column


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
    lines = console.render_lines(renderable, options, pad=False, new_lines=False)
    if len(lines) <= max_lines:
        return renderable

    ellipsis = Text("… (truncated)", style="italic dim", no_wrap=True)
    segments: list[Segment] = []
    for line in [*lines[: max_lines - 1], list(ellipsis.render(console))]:
        segments.extend(line)
        # every line needs its newline: Textual sizes a renderable by counting
        # the newlines in it, and would otherwise clip the last line.
        segments.append(Segment.line())
    return Segments(segments)


def measure_width(obj: object, console: Console) -> int:
    renderable = cell_formatter(obj, null_rep=Text(""))
    return console.measure(renderable).maximum
