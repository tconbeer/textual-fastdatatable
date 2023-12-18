from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable


class TableApp(App, inherit_bindings=False):
    BINDINGS = [("ctrl+q", "quit", "Quit")]

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet("./tests/data/lap_times_538121.parquet")
        yield DataTable(
            backend=backend, cursor_type="range", max_column_content_width=5
        )


if __name__ == "__main__":
    app = TableApp()
    app.run()
