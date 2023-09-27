from textual.app import App, ComposeResult
from textual_fastdatatable import DataTable

DATA = {
    "Foo": list(range(50)),
    "Bar": ["0123456789"] * 50,
    "Baz": ["IJKLMNOPQRSTUVWXYZ"] * 50,
}


class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable(
            data=DATA, column_labels=["[red]Not Foo[/red]", "Zig", "[reverse]Zag[/]"]
        )


app = TableApp()
if __name__ == "__main__":
    app.run()
