from __future__ import annotations

import re
from dataclasses import dataclass

from rich.text import Text

CELL_X_PADDING = 2

SNAKE_ID_PROG = re.compile(r"(\b|_)id\b", flags=re.IGNORECASE)
CAMEL_ID_PROG = re.compile(r"[a-z]I[dD]\b")


@dataclass
class Column:
    """Metadata for a column in the DataTable."""

    label: Text
    width: int = 0
    content_width: int = 0
    auto_width: bool = False
    max_content_width: int | None = None

    def __post_init__(self) -> None:
        self._is_id: bool | None = None

    @property
    def render_width(self) -> int:
        """Width in cells, required to render a column."""
        # +2 is to account for space padding either side of the cell
        if self.auto_width and self.max_content_width is not None:
            return (
                min(max(len(self.label), self.content_width), self.max_content_width)
                + CELL_X_PADDING
            )
        elif self.auto_width:
            return max(len(self.label), self.content_width) + CELL_X_PADDING
        else:
            return self.width + CELL_X_PADDING

    @property
    def is_id(self) -> bool:
        if self._is_id is None:
            snake_id = SNAKE_ID_PROG.search(str(self.label)) is not None
            camel_id = CAMEL_ID_PROG.search(str(self.label)) is not None
            self._is_id = snake_id or camel_id
        return self._is_id
