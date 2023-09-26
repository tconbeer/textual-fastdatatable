import csv
import io

from textual.app import App, ComposeResult
from textual_fastdatatable import ArrowBackend, DataTable

CSV = """lane,swimmer,country,time
4,Joseph Schooling,Singapore,50.39
2,Michael Phelps,United States,51.14
5,Chad le Clos,South Africa,51.14
6,László Cseh,Hungary,51.14
3,Li Zhuhao,China,51.26
8,Mehdy Metella,France,51.58
7,Tom Shields,United States,51.73
1,Aleksandr Sadovnikov,Russia,51.84"""


class TableApp(App):
    def compose(self) -> ComposeResult:
        rows = csv.reader(io.StringIO(CSV))
        labels = next(rows)
        data = [row for row in rows]
        backend = ArrowBackend.from_pydict(
            {label: [row[i] for row in data] for i, label in enumerate(labels)}
        )
        table = DataTable(backend)
        table.focus()
        table.cursor_type = "column"
        table.fixed_columns = 1
        table.fixed_rows = 1
        yield table


if __name__ == "__main__":
    app = TableApp()
    app.run()
