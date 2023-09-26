from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable


class TableApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet("./tests/data/lap_times.parquet")
        yield DataTable(backend)


if __name__ == "__main__":
    app = TableApp()
    app.run()
