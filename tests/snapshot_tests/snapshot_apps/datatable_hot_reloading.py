from pathlib import Path

from textual.app import App, ComposeResult
from textual_fastdatatable import ArrowBackend, DataTable

CSS_PATH = (Path(__file__) / "../datatable_hot_reloading.tcss").resolve()

# Write some CSS to the file before the app loads.
# Then, the test will clear all the CSS to see if the
# hot reloading applies the changes correctly.
CSS_PATH.write_text(
    """\
DataTable > .datatable--cursor {
    background: purple;
}

DataTable > .datatable--fixed {
    background: red;
}

DataTable > .datatable--fixed-cursor {
    background: blue;
}

DataTable > .datatable--header {
    background: yellow;
}

DataTable > .datatable--odd-row {
    background: pink;
}

DataTable > .datatable--even-row {
    background: brown;
}
"""
)


class DataTableHotReloadingApp(App[None]):
    CSS_PATH = CSS_PATH

    def compose(self) -> ComposeResult:
        data = {
            # orig test set A width=10, we fake it with spaces
            "A         ": ["one", "three", "five"],
            "B": ["two", "four", "six"],
        }
        backend = ArrowBackend.from_pydict(data)
        yield DataTable(backend, zebra_stripes=True, cursor_type="row", fixed_columns=1)

    def on_mount(self) -> None:
        self.query_one(DataTable)


if __name__ == "__main__":
    app = DataTableHotReloadingApp()
    app.run()
