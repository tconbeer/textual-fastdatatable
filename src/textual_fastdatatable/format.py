from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from itertools import chain
from typing import cast

from rich.align import Align
from rich.cells import cell_len
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

MULTILINE_MARKER = "…⏎"
"""Marks a cell whose value carries on past the one line a row has room for.

Rows are always one line tall, so everything below the first line of a multi-line
value is unrenderable. Without a marker that value is indistinguishable from the
one line it shows -- and a value that *starts* with a line break is
indistinguishable from an empty one (tconbeer/harlequin#635). The marker says
there is more, and the cell's tooltip shows it.

Two glyphs, not the bare ellipsis it reads as half of: rich ends a value clipped
to the *column's width* with `…` as well, so an ellipsis alone cannot say whether
the rest of a value is off to the right or below. The return symbol says below.
It is `East_Asian_Width=Neutral`, so unlike the ellipsis beside it no terminal
should give it a second cell."""

MULTILINE_MARKER_STYLE = "dim italic"
"""Styled so the marker reads as a marker, not as data the value ends with."""

MULTILINE_MARKER_WIDTH = cell_len(MULTILINE_MARKER)
"""The cells the marker occupies, reserved out of a cell's width when one is given.

Measured rather than written down, so the marker above stays the only place its
width is decided."""

LINE_BREAK_PROG = re.compile(r"[\r\n]")
"""What counts as the end of the first line of a value.

Deliberately only the two breaks that appear in data and that a terminal cannot
render inline: rich splits lines on ``\n``, and a lone ``\r`` would drive the
cursor back over the row."""

_console: Console | None = None
"""The console to measure against when the caller has no app to borrow one from (the
backend never does). Built on the first measurement, so consumers that never measure
anything never build a Console."""

_console_options: ConsoleOptions | None = None
"""The render options of `_console`, built with it. `Console.options` builds a fresh
ConsoleOptions on every access, which is about half the cost of measuring a short
value; this console has a fixed width and is never resized, so its options are too."""


def has_line_break(obj: object) -> bool:
    """Whether a value carries on past the first line, so a cell can only show part.

    Only strings (and the `Text` a string is parsed into) can: every other type
    `cell_formatter` knows renders on one line by construction.
    """
    if isinstance(obj, Text):
        obj = obj.plain
    return isinstance(obj, str) and LINE_BREAK_PROG.search(obj) is not None


def _split_first_line(value: str, truncate: bool) -> tuple[str, bool]:
    """`value` up to its first line break, and whether one was found."""
    match = LINE_BREAK_PROG.search(value) if truncate else None
    if match is None:
        return value, False
    return value[: match.start()], True


def _mark_truncated(text: Text, max_width: int | None) -> Text:
    """Append the marker to `text`, which is a value's first line, in place.

    The marker is reserved out of `max_width` rather than left to compete with the
    value for it: rich would otherwise clip the tail of an over-wide cell, and the
    marker, being the tail, is the first thing to go -- exactly in the columns
    whose values are most likely to have lines below. `max_width` is None wherever
    nothing bounds the value, which is every measurement (see `measure_width`) and
    every tooltip.

    The value is cropped rather than ellipsized, because the marker opens with an
    ellipsis of its own; letting rich add a second one reads as a typo, not as a
    second kind of truncation.
    """
    if max_width is not None:
        text.truncate(max(max_width - MULTILINE_MARKER_WIDTH, 0), overflow="crop")
    text.append(MULTILINE_MARKER, style=MULTILINE_MARKER_STYLE)
    return text


def truncate_to_first_line(text: Text, max_width: int | None = None) -> Text:
    """Clip `text` to its first line, marking it if there was more.

    Returns `text` itself when it is already one line, so the common case copies
    nothing; a multi-line value is sliced (which keeps its styles) rather than
    mutated, since the caller may not own it.
    """
    match = LINE_BREAK_PROG.search(text.plain)
    if match is None:
        return text
    return _mark_truncated(text[: match.start()], max_width)


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

    A multi-line value measures the width of its first line plus the truncation
    marker, because that is all of it a one-line row ever renders.
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
    obj: object,
    null_rep: Text,
    col: Column | None = None,
    render_markup: bool = True,
    truncate_multiline: bool = True,
    max_width: int | None = None,
) -> RenderableType:
    """Convert a cell into a Rich renderable for display.

    For correct formatting, clients should call `locale.setlocale()` first.

    Args:
        obj: Data for a cell.
        col: Column that the cell came from (used to compute width).
        render_markup: Parse strings as console markup, instead of literally.
        truncate_multiline: Clip a value that spans more than one line to its
            first line, followed by `MULTILINE_MARKER`. On by default, since a
            row is one line tall and the rest would be dropped silently. Callers
            that have room for every line -- the tooltip -- pass False.
        max_width: The cells the value will be rendered into, when the caller
            knows. Only a clipped value reads it, to reserve room for the marker
            (see `_mark_truncated`); pass None where nothing bounds the value.

    Returns:
        A renderable to be displayed which represents the data.
    """
    if obj is None:
        return Align(null_rep, align="center")

    elif isinstance(obj, str) and render_markup:
        head, truncated = _split_first_line(obj, truncate_multiline)
        try:
            rich_text = Text.from_markup(head)
        except MarkupError:
            # rich will not parse it, so hand back something it renders as it is
            return _mark_truncated(Text(head), max_width) if truncated else escape(head)
        return _mark_truncated(rich_text, max_width) if truncated else rich_text

    elif isinstance(obj, str):
        head, truncated = _split_first_line(obj, truncate_multiline)
        # `Text` renders literally, which is what escaping the markup achieves;
        # only a marked value needs to be one, so that the mark can carry a style
        return _mark_truncated(Text(head), max_width) if truncated else escape(head)

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

    elif isinstance(obj, Text):
        return truncate_to_first_line(obj, max_width) if truncate_multiline else obj

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
