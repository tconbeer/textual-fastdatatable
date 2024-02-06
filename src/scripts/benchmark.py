from __future__ import annotations

import gc
from pathlib import Path
from time import perf_counter

import pandas as pd
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.pilot import Pilot
from textual.types import CSSPathType
from textual.widgets import DataTable as BuiltinDataTable
from textual_fastdatatable import ArrowBackend
from textual_fastdatatable import DataTable as FastDataTable

BENCHMARK_DATA = Path(__file__).parent.parent.parent / "tests" / "data"


async def scroller(pilot: Pilot) -> None:
    first_paint = perf_counter() - pilot.app.start  # type: ignore
    for _ in range(5):
        await pilot.press("pagedown")
    for _ in range(15):
        await pilot.press("right")
    for _ in range(5):
        await pilot.press("pagedown")
    elapsed = perf_counter() - pilot.app.start  # type: ignore
    pilot.app.exit(result=(first_paint, elapsed))


class BuiltinApp(App):
    TITLE = "Built-In DataTable"

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
        rows = [tuple(row) for row in df.itertuples(index=False)]
        self.start = perf_counter()
        table: BuiltinDataTable = BuiltinDataTable()
        table.add_columns(*[str(col) for col in df.columns])
        for row in rows:
            table.add_row(*row, height=1, label=None)
        yield table


class ArrowBackendApp(App):
    TITLE = "FastDataTable (Arrow from Parquet)"

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
        self.start = perf_counter()
        yield FastDataTable(data=self.data_path)


class ArrowBackendAppFromRecords(App):
    TITLE = "FastDataTable (Arrow from Records)"

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
        rows = [tuple(row) for row in df.itertuples(index=False)]
        self.start = perf_counter()
        backend = ArrowBackend.from_records(rows, has_header=False)
        table = FastDataTable(
            backend=backend, column_labels=[str(col) for col in df.columns]
        )
        yield table


if __name__ == "__main__":
    app_defs = [BuiltinApp, ArrowBackendApp, ArrowBackendAppFromRecords]
    bench = [
        (f"lap_times_{n}.parquet", 3 if n <= 10000 else 1)
        for n in [100, 1000, 10000, 100000, 538121]
    ]
    bench.extend([(f"wide_{n}.parquet", 1) for n in [10000, 100000]])
    with open("results.md", "w") as f:
        print(
            "Records |",
            " | ".join([a.TITLE for a in app_defs]),  # type: ignore
            sep="",
            file=f,
        )
        print("--------|", "|".join(["--------" for _ in app_defs]), sep="", file=f)
        for p, tries in bench:
            first_paint: list[list[float]] = [list() for _ in app_defs]
            elapsed: list[list[float]] = [list() for _ in app_defs]
            for i, app_cls in enumerate(app_defs):
                for _ in range(tries):
                    app = app_cls(BENCHMARK_DATA / p)
                    gc.disable()
                    fp, el = app.run(headless=True, auto_pilot=scroller)  # type: ignore
                    gc.collect()
                    first_paint[i].append(fp)
                    elapsed[i].append(el)
            gc.enable()
            avg_first_paint = [sum(app_times) / tries for app_times in first_paint]
            avg_elapsed = [sum(app_times) / tries for app_times in elapsed]
            formatted = [
                f"{fp:7,.3f}s / {el:7,.3f}s"
                for fp, el in zip(avg_first_paint, avg_elapsed)
            ]
            print(f"{p} | {' | '.join(formatted)}", file=f)
