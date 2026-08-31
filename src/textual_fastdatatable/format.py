from __future__ import annotations

import re
import uuid
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

MULTILINE_MARKER = "…⏎"
"""Ends a cell whose value has lines below the one the row can show.

Not a bare `…`: rich ends a value clipped to the column's *width* with one too, so
the return symbol is what says the rest is below rather than to the right."""

MULTILINE_MARKER_STYLE = "dim italic"
"""Styled so the marker reads as a marker, not as the end of the value."""

MULTILINE_MARKER_WIDTH = 2
"""Cells `MULTILINE_MARKER` occupies. Checked by test_format."""

BINARY_PREVIEW_BYTES = 32
"""Bytes of a binary value a cell shows before summarizing the rest of them."""

FIXED_WIDTH_TYPES = (bool, uuid.UUID)
"""Types whose every value renders the same width: a uuid is 36 characters and a
bool is 7, so a column of them is measured from one value, like a temporal type."""

LINE_BREAK_PROG = re.compile(r"[\r\n]")
"""Where a value's first line ends.

Only these two: rich splits lines on `\n`, and a lone `\r` would drive the cursor
back over the row."""

_console: Console | None = None
"""The console to measure against when the caller has no app to borrow one from (the
backend never does). Built on the first measurement, so consumers that never measure
anything never build a Console."""

_console_options: ConsoleOptions | None = None
"""The render options of `_console`, built with it. `Console.options` builds a fresh
ConsoleOptions on every access, which is about half the cost of measuring a short
value; this console has a fixed width and is never resized, so its options are too."""


def _escape(text: str) -> str:
    """`rich.markup.escape`, skipped for the values it would leave alone.

    Testing for the two things it acts on costs a fraction of the substitution."""
    return escape(text) if "[" in text or text.endswith("\\") else text


def has_line_break(obj: object) -> bool:
    """Whether a cell can only show part of this value.

    Asked of the text the value renders as, since a type of a driver's own prints
    whatever it likes -- and a cell showing one line of it owes the reader a tooltip.
    """
    if isinstance(obj, Text):
        obj = obj.plain
    elif not isinstance(obj, str):
        obj = display_text(obj)
    return LINE_BREAK_PROG.search(obj) is not None


def _split_first_line(value: str, truncate: bool) -> tuple[str, bool]:
    """`value` up to its first line break, and whether one was found."""
    match = LINE_BREAK_PROG.search(value) if truncate else None
    if match is None:
        return value, False
    return value[: match.start()], True


def _mark_truncated(text: Text, max_width: int | None) -> Text:
    """Append the marker to a value's first line, in place, within `max_width`.

    The marker is the tail of the line, so left to compete for `max_width` it is the
    first thing rich clips off. Cropping the value to make room keeps it. Cropping,
    not ellipsizing: the marker opens with an ellipsis already.
    """
    if max_width is not None:
        text.truncate(max(max_width - MULTILINE_MARKER_WIDTH, 0), overflow="crop")
    text.append(MULTILINE_MARKER, style=MULTILINE_MARKER_STYLE)
    return text


def truncate_to_first_line(text: Text, max_width: int | None = None) -> Text:
    """`text` clipped to its first line and marked, or `text` itself if it is one line.

    Slices rather than mutates, since the caller owns `text`.
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

    A multi-line value measures its first line plus the marker, all a row renders.
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


def display_text(
    obj: object, col: Column | None = None, render_markup: bool = True
) -> str:
    """The markup a cell shows for `obj`, without the alignment `cell_formatter` adds.

    Always markup: what renders literally is escaped, as rich parses every string."""
    if obj is None:
        # a null renders as the widget's null_rep, which is the widget's to measure
        return ""

    elif isinstance(obj, str):
        return obj if render_markup else _escape(obj)

    elif isinstance(obj, (bytes, bytearray, memoryview)):
        # binary values (e.g. varbinary columns) can contain sequences like
        # [/...] that Rich would try to parse as markup; show an escaped,
        # bounded preview instead. See tconbeer/harlequin#974.
        data = bytes(obj)
        preview = repr(data[:BINARY_PREVIEW_BYTES])
        if len(data) > BINARY_PREVIEW_BYTES:
            preview = f"{preview} (+{len(data) - BINARY_PREVIEW_BYTES} bytes)"
        return _escape(preview)

    elif isinstance(obj, Text):
        # a Text renders literally, carrying its own styles, which take no cells
        return _escape(obj.plain)

    elif isinstance(obj, bool):
        return f"[dim]{'✓' if obj else 'X'}[/] {obj}{' ' if obj else ''}"

    elif isinstance(obj, (float, Decimal)):
        return f"{obj:n}"

    elif isinstance(obj, int):
        # no separators in ID fields
        return str(obj) if col is not None and col.is_id else f"{obj:n}"

    elif isinstance(obj, (datetime, time)):
        formatted = obj.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        if obj in (datetime.max, datetime.min):
            sign = "∞ " if obj == datetime.max else "-∞ "
            return f"[bold]{sign}[/][dim]{formatted}[/]"
        return formatted

    elif isinstance(obj, date):
        if obj in (date.max, date.min):
            sign = "∞ " if obj == date.max else "-∞ "
            return f"[bold]{sign}[/][dim]{obj.isoformat()}[/]"
        return obj.isoformat()

    elif isinstance(obj, timedelta):
        return str(obj)

    else:
        # a uuid, a list, a struct's dict, a driver's own type: whatever it prints as,
        # escaped. The brackets in a repr are the repr's, so a tag rich finds in one
        # was never markup: rendering it eats the structure around it, and an
        # unbalanced one raises. A string is the only value markup is parsed in.
        return _escape(str(obj))


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
        truncate_multiline: Clip a multi-line value to its first line plus
            `MULTILINE_MARKER`. False only for the tooltip, which has room for
            every line.
        max_width: Cells the value will be rendered into. Only a clipped value
            reads it, to reserve room for the marker. None when measuring.

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
            # not markup after all, so fall through to rendering it literally
            return (
                _mark_truncated(Text(head), max_width) if truncated else _escape(head)
            )
        return _mark_truncated(rich_text, max_width) if truncated else rich_text

    elif isinstance(obj, str):
        head, truncated = _split_first_line(obj, truncate_multiline)
        # `Text` renders literally, so it needs no escaping; a marked value has to
        # be one anyway, to carry the marker's style
        return _mark_truncated(Text(head), max_width) if truncated else _escape(head)

    elif isinstance(obj, bool):
        return Align(display_text(obj), style="bold" if obj else "", align="right")

    elif isinstance(obj, (float, Decimal, int)):
        return Align(display_text(obj, col), align="right")

    elif isinstance(obj, (datetime, time, date, timedelta)):
        return Align(display_text(obj), align="right")

    elif isinstance(obj, Text):
        return truncate_to_first_line(obj, max_width) if truncate_multiline else obj

    elif not is_renderable(obj):
        # binary and everything else with no renderable of its own -- a uuid, a
        # list, a struct's dict -- as the text `display_text` gives it, clipped to
        # the one line a row has room for like any other value. A repr escapes its
        # breaks, but nothing stops a driver's own type from printing several lines.
        head, truncated = _split_first_line(display_text(obj), truncate_multiline)
        return _mark_truncated(Text.from_markup(head), max_width) if truncated else head

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
