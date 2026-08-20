from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable

# rows are one line tall, so everything past the first line of a value is
# unrenderable; a truncation marker says so. See tconbeer/harlequin#635 and #771.
ROWS = [
    ("case", "value"),
    ("single line", "no marker here"),
    ("trailing lines", "first line\nsecond line\nthird line"),
    ("leading break", "\nhidden below the first line"),
    ("carriage return", "before\rafter"),
    ("markup", "[red]styled[/] first line\nand more"),
    ("blank", ""),
    # the marker is reserved out of the width, so it survives a first line that
    # is itself too wide for the capped column
    ("capped", "a first line long enough to fill the whole column\nand more"),
    ("capped, one line", "a single line long enough to fill the whole column"),
]


class TableApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_records(ROWS, has_header=True)
        yield DataTable(backend=backend, max_column_content_width=32)


app = TableApp()
if __name__ == "__main__":
    app.run()
