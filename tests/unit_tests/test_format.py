from __future__ import annotations

from rich.console import Console
from rich.text import Text

from textual_fastdatatable.format import cell_formatter, measure_width

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
