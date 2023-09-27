from textual.app import App, ComposeResult
from textual.binding import Binding
from textual_fastdatatable import ArrowBackend, DataTable

MOVIES = [
    "Severance",
    "Foundation",
    "Dark",
    "The Boys",
    "The Last of Us",
    "Lost in Space",
    "Altered Carbon",
]


class AddColumn(App):
    BINDINGS = [
        Binding(key="c", action="add_column", description="Add Column"),
    ]

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_pydict({"Movies": MOVIES})
        table = DataTable(backend=backend)

        column_idx = table.add_column("No Default")
        table.add_column("With Default", default="ABC")
        table.add_column("Long Default", default="01234567890123456789")

        # Ensure we can update a cell
        table.update_cell(2, column_idx, "Hello!")
        yield table


app = AddColumn()
if __name__ == "__main__":
    app.run()
