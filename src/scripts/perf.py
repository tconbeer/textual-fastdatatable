from __future__ import annotations

import asyncio
import timeit
from functools import partial
from pathlib import Path

import pandas as pd
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.types import CSSPathType
from textual.widgets import DataTable as BuiltinDataTable
from textual_fastdatatable import ArrowBackend
from textual_fastdatatable import DataTable as FastDataTable

BENCHMARK_DATA = Path(__file__).parent.parent.parent / "tests" / "data"


class BuiltinApp(App):
    def __init__(
        self,
        data_path: Path,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css)
        self.data_path = data_path

    def compose(self) -> ComposeResult:
        df = pd.read_parquet(self.data_path)
        table: BuiltinDataTable = BuiltinDataTable()
        table.add_columns(*[str(col) for col in df.columns])
        for row in df.iterrows():
            table.add_row(row, height=1, label=None)
        yield table


class FastApp(App):
    def __init__(
        self,
        data_path: Path,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css)
        self.data_path = data_path

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet(self.data_path)
        yield FastDataTable(backend)


if __name__ == "__main__":

    async def run_headless(app: App) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()

    def run(app: App) -> None:
        asyncio.run(run_headless(app))

    def run_builtin(data_path: Path) -> None:
        builtin_app = BuiltinApp(data_path)
        run(builtin_app)

    def run_fast(data_path: Path) -> None:
        fast_app = FastApp(data_path)
        run(fast_app)

    print("Records | Built-in DataTable | FastDataTable")
    print("--------|--------------------|--------------")
    for n in [100, 1000, 10000, 100000, 538121]:
        tries = 3 if n <= 10000 else 1
        path = BENCHMARK_DATA / f"lap_times_{n}.parquet"
        fast = partial(run_fast, path)
        builtin = partial(run_builtin, path)

        fast_per = timeit.timeit(fast, number=tries) / tries
        builtin_per = timeit.timeit(builtin, number=tries) / tries
        print(f"{n}|{builtin_per:,.2f}s|{fast_per:,.2f}s")
