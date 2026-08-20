from __future__ import annotations

import pytest
from rich.console import Console
from rich.text import Text

from textual_fastdatatable.format import (
    MAX_MEASURE_WIDTH,
    MULTILINE_MARKER,
    MULTILINE_MARKER_WIDTH,
    cell_formatter,
    measure_width,
    truncate_to_first_line,
)

NULL = Text("")


def _can_render(renderable: object) -> bool:
    """Return True if Rich can render *renderable* without raising."""
    console = Console()
    try:
        with console.capture():
            console.print(renderable)
        return True
    except Exception:
        return False


def test_cell_formatter_json_null_render_markup_false() -> None:
    """A JSON string containing [null] must not produce markup spans when
    render_markup=False.

    Regression test for https://github.com/tconbeer/harlequin/issues/933:
    hovering a cell whose value is a JSON string with a ``[null]`` array element
    crashed Harlequin with ``MissingStyle: Failed to get style 'null'`` because
    the tooltip code path called cell_formatter without forwarding
    render_markup=self.render_markup.  With render_markup=True (the wrong default),
    Rich parses ``[null]`` as a markup tag and creates a Span with style ``"null"``,
    which Textual's style resolver later rejects.

    With render_markup=False the string must be returned as plain escaped text with
    no spans, so it is always safe to render.
    """
    json_str = (
        '{"color":[null],"b":"testtesttesttesttesttesttesttesttesttesttesttesttest"}'
    )
    result = cell_formatter(json_str, null_rep=Text(""), render_markup=False)

    # Must be renderable without errors.
    assert _can_render(result), (
        "cell_formatter with render_markup=False produced an unrenderable object "
        "for a JSON string containing [null]"
    )

    # Must not contain any span whose style is the bare word "null" — that would
    # indicate the string was parsed as Rich markup.
    if isinstance(result, Text):
        null_spans = [span for span in result._spans if span.style == "null"]
        assert not null_spans, (
            f"cell_formatter with render_markup=False must not produce spans with "
            f"style 'null'; got {null_spans}"
        )


def test_bytes_with_markup_hostile_content_do_not_raise() -> None:
    # regression test for tconbeer/harlequin#974: varbinary values arrive as
    # bytes and can contain [/...] sequences that Rich parses as closing tags
    hostile = b"[/l\x1d\x1a\t#\xd9\xfa9Z\xa9\xe4\x8e\xab\xfaH\x18\xba\x91\xd2"
    rendered = cell_formatter(hostile, null_rep=NULL)
    assert isinstance(rendered, str)
    # must not raise MarkupError
    Text.from_markup(rendered)


def test_bytes_preview_is_truncated() -> None:
    data = bytes(range(256))
    rendered = cell_formatter(data, null_rep=NULL)
    assert isinstance(rendered, str)
    assert "(+224 bytes)" in rendered


def test_short_bytes_not_truncated() -> None:
    rendered = cell_formatter(b"abc", null_rep=NULL)
    assert isinstance(rendered, str)
    assert "bytes)" not in rendered
    Text.from_markup(rendered)


def test_bytearray_and_memoryview() -> None:
    for value in (bytearray(b"[/x]"), memoryview(b"[/x]")):
        rendered = cell_formatter(value, null_rep=NULL)
        assert isinstance(rendered, str)
        Text.from_markup(rendered)


def test_bytes_are_measurable() -> None:
    width = measure_width(b"[/l\xd9\xfa9Z")
    assert width > 0


def _plain(renderable: object) -> str:
    """What a cell renderable prints, markup rendered away."""
    console = Console(
        width=MAX_MEASURE_WIDTH, markup=True, emoji=False, highlight=False
    )
    with console.capture() as capture:
        console.print(renderable, end="")
    return capture.get()


@pytest.mark.parametrize("render_markup", [True, False])
@pytest.mark.parametrize(
    "value,expected",
    [
        ("one\ntwo", f"one{MULTILINE_MARKER}"),
        ("one\r\ntwo", f"one{MULTILINE_MARKER}"),
        ("one\rtwo", f"one{MULTILINE_MARKER}"),
        ("one\ntwo\nthree", f"one{MULTILINE_MARKER}"),
        # a value that starts with a break is otherwise indistinguishable from an
        # empty one: tconbeer/harlequin#635
        ("\nhidden", MULTILINE_MARKER),
        ("\n\N{PILE OF POO}", MULTILINE_MARKER),
        ("one line", "one line"),  # nothing to mark
        ("", ""),
    ],
)
def test_multiline_cells_are_marked_as_truncated(
    value: str, expected: str, render_markup: bool
) -> None:
    """A row is one line tall, so what is below the first line must be marked."""
    result = cell_formatter(value, null_rep=NULL, render_markup=render_markup)

    assert _can_render(result)
    assert _plain(result) == expected
    assert measure_width(value, render_markup=render_markup) == len(expected)


def test_multiline_markup_is_still_rendered_as_markup() -> None:
    """Clipping to the first line must not turn the value into a literal string."""
    result = cell_formatter("[red]bold[/] text\nmore", null_rep=NULL)

    assert _plain(result) == f"bold text{MULTILINE_MARKER}"


def test_multiline_value_that_is_not_markup_is_escaped() -> None:
    """The MarkupError fallback marks the value too, rather than dropping the mark."""
    result = cell_formatter("[/] not markup\nmore", null_rep=NULL)

    assert _plain(result) == f"[/] not markup{MULTILINE_MARKER}"


def test_multiline_text_values_are_marked_without_being_mutated() -> None:
    """A `Text` handed to the formatter belongs to the caller."""
    value = Text("one\ntwo", style="bold")

    result = cell_formatter(value, null_rep=NULL)

    assert _plain(result) == f"one{MULTILINE_MARKER}"
    assert value.plain == "one\ntwo"


def test_truncate_multiline_false_keeps_every_line() -> None:
    """The tooltip has room for the lines a cell does not."""
    result = cell_formatter("one\ntwo", null_rep=NULL, truncate_multiline=False)

    assert _plain(result) == "one\ntwo"


def test_single_line_text_is_returned_unchanged() -> None:
    """The common case must not pay for a copy."""
    value = Text("one line")

    assert cell_formatter(value, null_rep=NULL) is value
    assert truncate_to_first_line(value) is value


@pytest.mark.parametrize(
    "max_width,expected",
    [
        (None, "first line" + MULTILINE_MARKER),  # nothing bounds it
        (20, "first line" + MULTILINE_MARKER),  # room to spare
        (12, "first line" + MULTILINE_MARKER),  # exactly enough
        (8, "first " + MULTILINE_MARKER),  # the value gives way, not the marker
        (3, "f" + MULTILINE_MARKER),
        (2, MULTILINE_MARKER),  # room for nothing but the marker
    ],
)
def test_the_marker_is_reserved_out_of_the_width(
    max_width: int | None, expected: str
) -> None:
    """The marker must not be the tail rich clips off an over-wide cell.

    It is the last thing on the line, so leaving it to compete with the value for
    the width loses it exactly where lines below are most likely -- a wide column
    of text. The value gives way instead.
    """
    result = cell_formatter(
        "first line\nsecond line", null_rep=NULL, max_width=max_width
    )

    assert _plain(result) == expected
    if max_width is not None:
        assert measure_width(result) <= max(max_width, MULTILINE_MARKER_WIDTH)


def test_a_cropped_value_does_not_get_a_second_ellipsis() -> None:
    """The marker opens with one; rich adding another reads as a typo."""
    result = cell_formatter("a long first line\nmore", null_rep=NULL, max_width=10)

    assert _plain(result).count("…") == 1


def test_a_single_line_value_is_never_clipped_by_the_marker_reservation() -> None:
    """Only a marked value reserves anything; everything else renders as it was."""
    result = cell_formatter("a long single line", null_rep=NULL, max_width=4)

    assert _plain(result) == "a long single line"
