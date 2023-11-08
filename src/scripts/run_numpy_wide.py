from __future__ import annotations

from pathlib import Path

import pandas as pd
from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.types import CSSPathType
from textual_fastdatatable import DataTable, NumpyBackend

BENCHMARK_DATA = Path(__file__).parent.parent.parent / "tests" / "data"


class NumpyApp(App):
    TITLE = "FastDataTable (Numpy)"

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
        backend = NumpyBackend(rows)
        table = DataTable(
            backend=backend, column_labels=[str(col) for col in df.columns]
        )
        del df
        del rows
        yield table


if __name__ == "__main__":
    app = NumpyApp(data_path=BENCHMARK_DATA / "wide_10000.parquet")
    app.run()
