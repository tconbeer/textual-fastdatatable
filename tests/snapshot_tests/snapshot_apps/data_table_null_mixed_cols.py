from textual.app import App, ComposeResult
from textual_fastdatatable import ArrowBackend, DataTable

ROWS = [
    ("lane", "swimmer", "country", "time"),
    (3, "Li Zhuhao", "China", 51.26),
    ("eight", None, "France", 51.58),
    ("seven", "Tom Shields", "United States", None),
    (1, "Aleksandr Sadovnikov", "Russia", 51.84),
    (None, "Darren Burns", "Scotland", 51.84),
]


class TableApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_records(ROWS, has_header=True)
        yield DataTable(backend=backend, null_rep="[dim]∅ null[/]")


app = TableApp()
if __name__ == "__main__":
    app.run()
