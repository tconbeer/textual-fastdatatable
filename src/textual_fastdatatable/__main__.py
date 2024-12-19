from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable


class TableApp(App, inherit_bindings=False):
    BINDINGS = [("ctrl+q", "quit", "Quit"), ("ctrl+d", "quit", "Quit")]

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet("./tests/data/wide_100000.parquet")
        yield DataTable(backend=backend, cursor_type="range", fixed_columns=2)


if __name__ == "__main__":
    import locale

    locale.setlocale(locale.LC_ALL, "")
    app = TableApp()
    app.run()
