from textual.app import App, ComposeResult
from textual_fastdatatable import ArrowBackend, DataTable

ROWS = [
    ("lane", "swimmer", "country", "time"),
    (4, "[Joseph Schooling]", "Singapore", 50.39),
    (2, "[red]Michael Phelps[/]", "United States", 51.14),
    (5, "[bold]Chad le Clos[/]", "South Africa", 51.14),
    (6, "László Cseh", "Hungary", 51.14),
    (3, "Li Zhuhao", "China", 51.26),
    (8, "Mehdy Metella", "France", 51.58),
    (7, "Tom Shields", "United States", 51.73),
    (1, "Aleksandr Sadovnikov", "Russia", 51.84),
    (10, "Darren Burns", "Scotland", 51.84),
]


class TableApp(App):
    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_records(ROWS, has_header=True)
        yield DataTable(backend=backend, render_markup=False)


app = TableApp()
if __name__ == "__main__":
    app.run()
