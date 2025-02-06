from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import cast

from rich.align import Align
from rich.console import Console, RenderableType
from rich.errors import MarkupError
from rich.markup import escape
from rich.protocol import is_renderable
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
                f"[bold]{'∞ ' if obj == datetime.max else '-∞ '}[/][dim]{_fmt_datetime(obj)}[/]",
                align="right",
            )

        return Align(_fmt_datetime(obj), align="right")

    elif isinstance(obj, date):
        if obj in (date.max, date.min):
            return Align(
                f"[bold]{'∞ ' if obj == date.max else '-∞ '}[/][dim]{obj.isoformat()}[/]",
                align="right",
            )

        return Align(obj.isoformat(), align="right")

    elif isinstance(obj, timedelta):
        return Align(str(obj), align="right")

    elif not is_renderable(obj):
        return str(obj)

    else:
        return cast(RenderableType, obj)


def measure_width(obj: object, console: Console) -> int:
    renderable = cell_formatter(obj, null_rep=Text(""))
    return console.measure(renderable).maximum
