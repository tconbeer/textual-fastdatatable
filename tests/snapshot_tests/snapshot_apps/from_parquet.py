from pathlib import Path

from textual.app import App, ComposeResult
from textual_fastdatatable import DataTable


class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable(
            data=Path(__file__).parent.parent.parent / "data" / "lap_times_100.parquet"
        )


app = TableApp()
if __name__ == "__main__":
    app.run()
