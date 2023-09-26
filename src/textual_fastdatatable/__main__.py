from textual.app import App, ComposeResult
from textual_fastdatatable import DataTable, ArrowBackend


class TableApp(App):

    def compose(self) -> ComposeResult:
        DATA = {
            "column_one": [1, 2, 3, 100, 1000, 10000],
            "column_two": ["foo", "bar", "baz", "qux", "foofoofoo", "barbar"],
        }
        backend = ArrowBackend.from_parquet("./tests/data/lap_times.parquet")
        yield DataTable(backend)


if __name__ == "__main__":
    app = TableApp()
    app.run()
