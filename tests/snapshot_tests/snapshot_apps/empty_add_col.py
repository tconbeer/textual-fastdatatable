from textual.app import App, ComposeResult
from textual_fastdatatable import DataTable


class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Foo")
        table.add_rows([("1",), ("2",)])


app = TableApp()
if __name__ == "__main__":
    app.run()
