from __future__ import annotations

import asyncio
import timeit
from pathlib import Path

import pandas as pd
from textual.app import App, ComposeResult
from textual.widgets import DataTable as BuiltinDataTable
from textual_fastdatatable import ArrowBackend
from textual_fastdatatable import DataTable as FastDataTable

BENCHMARK_DATA_FILE = (
    Path(__file__).parent.parent.parent / "tests" / "data" / "lap_times.parquet"
)


class BuiltinApp(App):
    def compose(self) -> ComposeResult:
        df = pd.read_parquet(BENCHMARK_DATA_FILE)
        table: BuiltinDataTable = BuiltinDataTable()
        table.add_columns(*[str(col) for col in df.columns])
        for row in df.iterrows():
            table.add_row(row, height=1, label=None)
        yield table


class FastApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet(BENCHMARK_DATA_FILE)
        yield FastDataTable(backend)


if __name__ == "__main__":

    async def run_headless(app: App) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()

    def run(app: App) -> None:
        asyncio.run(run_headless(app))

    def run_builtin() -> None:
        builtin_app = BuiltinApp()
        run(builtin_app)

    def run_fast() -> None:
        fast_app = FastApp()
        run(fast_app)

    print("Timing Fast App:")
    print(timeit.timeit(run_fast, number=1))

    print("Timing Built-in App:")
    print(timeit.timeit(run_builtin, number=1))
