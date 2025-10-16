from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.driver import Driver
from textual.types import CSSPathType

from textual_fastdatatable import DataTable

BENCHMARK_DATA = Path(__file__).parent.parent / "tests" / "data"


class ArrowBackendApp(App):
    TITLE = "FastDataTable (Arrow)"

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
        yield DataTable(data=self.data_path)


if __name__ == "__main__":
    app = ArrowBackendApp(data_path=BENCHMARK_DATA / "wide_100000.parquet")
    app.run()
