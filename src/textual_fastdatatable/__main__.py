from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable


class TableApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet("./tests/data/lap_times_538121.parquet")
        yield DataTable(backend=backend)


if __name__ == "__main__":
    app = TableApp()
    app.run()
