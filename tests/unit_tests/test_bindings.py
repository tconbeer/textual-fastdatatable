from __future__ import annotations

from typing import Any

import pytest
from textual.app import App, ComposeResult

from textual_fastdatatable import DataTable


class TableApp(App):
    def __init__(self) -> None:
        super().__init__()
        self.copied: list[list[tuple[Any, ...]]] = []

    def compose(self) -> ComposeResult:
        yield DataTable(data={"a": [1, 2], "b": ["x", "y"]}, cursor_type="range")

    def on_data_table_selection_copied(
        self, message: DataTable.SelectionCopied
    ) -> None:
        self.copied.append(message.values)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["ctrl+c", "super+c"])
async def test_copy_bindings(key: str) -> None:
    """Both copy chords post a SelectionCopied message.

    Textual binds `ctrl+c,super+c` for copy so that terminals which report the
    command key (like Ghostty) copy on cmd+c; this table follows suit.
    """
    app = TableApp()
    async with app.run_test() as pilot:
        await pilot.press(key)
        await pilot.pause()

    assert app.copied == [[(1,)]]
