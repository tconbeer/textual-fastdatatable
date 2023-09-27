from textual.app import App, ComposeResult
from textual.binding import Binding
from textual_fastdatatable import ArrowBackend, DataTable

ROWS = [
    ("lane", "swimmer", "country", "time"),
    (5, "Chad le Clos", "South Africa", 51.14),
    (4, "Joseph Schooling", "Singapore", 50.39),
    (2, "Michael Phelps", "United States", 51.14),
    (6, "László Cseh", "Hungary", 51.14),
    (3, "Li Zhuhao", "China", 51.26),
    (8, "Mehdy Metella", "France", 51.58),
    (7, "Tom Shields", "United States", 51.73),
    (10, "Darren Burns", "Scotland", 51.84),
    (1, "Aleksandr Sadovnikov", "Russia", 51.84),
]


class TableApp(App):
    """Snapshot app for testing removal of rows.
    Removes several rows, so we can check that the display of the
    DataTable updates as expected."""

    BINDINGS = [
        Binding("r", "remove_row", "Remove Row"),
    ]

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_records(ROWS)
        yield DataTable(backend=backend)

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.focus()

    def action_remove_row(self):
        table = self.query_one(DataTable)
        table.remove_row(2)
        table.remove_row(4)
        table.remove_row(6)


app = TableApp()
if __name__ == "__main__":
    app.run()
