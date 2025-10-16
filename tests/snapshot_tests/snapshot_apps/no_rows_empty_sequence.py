from textual.app import App, ComposeResult

from textual_fastdatatable import DataTable


class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable(column_labels=["foo [red]foo[/red]", "bar"])


app = TableApp()
if __name__ == "__main__":
    app.run()
