from __future__ import annotations

from pathlib import Path
from time import perf_counter

import pandas as pd
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.types import CSSPathType
from textual.widgets import DataTable as BuiltinDataTable
from textual_fastdatatable import DataTable as FastDataTable
from textual_fastdatatable import NativeBackend

BENCHMARK_DATA = Path(__file__).parent.parent.parent / "tests" / "data"


class BuiltinApp(App):
    def __init__(
        self,
        data_path: Path,
        exit: bool = True,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css)
        self.data_path = data_path
        self._do_exit = exit

    def compose(self) -> ComposeResult:
        df = pd.read_parquet(self.data_path)
        rows = [tuple(row) for row in df.itertuples(index=False)]
        start = perf_counter()
        table: BuiltinDataTable = BuiltinDataTable()
        table.add_columns(*[str(col) for col in df.columns])
        for row in rows:
            table.add_row(*row, height=1, label=None)
        yield table
        self.elapsed = perf_counter() - start

    def on_mount(self) -> None:
        if self._do_exit:
            self.exit(result=self.elapsed)


class NativeBackendApp(App):
    def __init__(
        self,
        data_path: Path,
        exit: bool = True,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css)
        self.data_path = data_path
        self._do_exit = exit

    def compose(self) -> ComposeResult:
        df = pd.read_parquet(self.data_path)
        # should be fastest if the data is a list of tuples
        rows = [tuple(row) for row in df.itertuples(index=False)]
        data = [[str(col) for col in df.columns], *rows]
        start = perf_counter()
        backend = NativeBackend(data=data)
        table: FastDataTable = FastDataTable(backend=backend)
        yield table
        self.elapsed = perf_counter() - start

    def on_mount(self) -> None:
        if self._do_exit:
            self.exit(result=self.elapsed)


class ArrowBackendApp(App):
    def __init__(
        self,
        data_path: Path,
        exit: bool = True,
        driver_class: type[Driver] | None = None,
        css_path: CSSPathType | None = None,
        watch_css: bool = False,
    ):
        super().__init__(driver_class, css_path, watch_css)
        self.data_path = data_path
        self._do_exit = exit

    def compose(self) -> ComposeResult:
        start = perf_counter()
        yield FastDataTable(data=self.data_path)
        self.elapsed = perf_counter() - start

    def on_mount(self) -> None:
        if self._do_exit:
            self.exit(result=self.elapsed)


if __name__ == "__main__":
    print("Records | Built-in DataTable | ArrowDataTable | NativeDataTable")
    print("--------|--------------------|----------------|----------------")
    for n in [100, 1000, 10000, 100000, 538121]:
        tries = 3 if n <= 10000 else 1
        path = BENCHMARK_DATA / f"lap_times_{n}.parquet"
        apps = [BuiltinApp(path), ArrowBackendApp(path), NativeBackendApp(path)]
        elapsed: list[list[float]] = [[], [], []]
        for i, app in enumerate(apps):
            for _ in range(tries):
                elapsed[i].append(app.run(headless=True))  # type: ignore
        avg_elapsed = [sum(app_times) / tries for app_times in elapsed]
        formatted = [f"{t:,.3f}s" for t in avg_elapsed]
        print(f"{n}|{'|'.join(formatted)}")

    # BuiltinApp(BENCHMARK_DATA / f"lap_times_{10000}.parquet", exit=False).run()
