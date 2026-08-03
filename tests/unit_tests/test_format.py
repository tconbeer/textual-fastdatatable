from __future__ import annotations

from rich.console import Console
from rich.text import Text

from textual_fastdatatable.format import cell_formatter


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
