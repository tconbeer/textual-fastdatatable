from textual.app import App, ComposeResult
from textual.binding import Binding
from textual_fastdatatable import DataTable, ArrowBackend

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
        backend = ArrowBackend.from_pydict(
            {
                "Movies": MOVIES,
                "No Default": ["", "", "Hello!", "", "", "", ""],
                "With Default": ["ABC"] * len(MOVIES),
                "Long Default": ["01234567890123456789"] * len(MOVIES),
            }
        )
        table = DataTable(backend)

        # TODO: support mutable data
        # column_key = table.add_column("No Default")
        # table.add_column("With Default", default="ABC")
        # table.add_column("Long Default", default="01234567890123456789")

        # Ensure we can update a cell
        # table.update_cell(row_keys[2], column_key, "Hello!")

        yield table


app = AddColumn()
if __name__ == "__main__":
    app.run()
