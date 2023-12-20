# textual-fastdatatable
A performance-focused reimplementation of Textual's DataTable widget, with a pluggable data storage backend.

Textual's built-in DataTable widget is beautiful and powerful, but it can be slow to load large datasets.

Here are some benchmarks on my relatively weak laptop. For each benchmark, we initialize a Textual App that
loads a dataset from a parquet file and mounts a data table; it then scrolls around the table
(10 pagedowns and 15 right arrows). 

For the built-in table and the others marked "from Records", the data is loaded into memory before the timer
is started; for the "Arrow from Parquet" back-end, the timer is started immediately.

The times in each column represent the time to the first paint of the table, and the time after scrolling
is completed (we wait until the table is fully rendered after each scroll):

Records | Built-In DataTable | FastDataTable (Arrow from Parquet) | FastDataTable (Arrow from Records) | FastDataTable (Numpy from Records) 
--------|--------|--------|--------|--------
lap_times_100.parquet |   0.019s /   1.716s |   0.012s /   1.724s |    0.011s /   1.700s |   0.011s /   1.688s
lap_times_1000.parquet |   0.103s /   1.931s |   0.011s /   1.859s |    0.011s /   1.799s |   0.015s /   1.848s
lap_times_10000.parquet |   0.977s /   2.824s |   0.013s /   1.834s |    0.016s /   1.812s |   0.078s /   1.869s
lap_times_100000.parquet |  11.773s /  13.770s |   0.025s /   1.790s |    0.156s /   1.824s |   0.567s /   2.347s
lap_times_538121.parquet |  62.960s /  65.760s |   0.077s /   1.803s |    0.379s /   2.234s |   3.324s /   5.031s
wide_10000.parquet |   5.110s /  10.539s |   0.024s /   3.373s |    0.042s /   3.278s |   0.369s /   3.461s
wide_100000.parquet |  51.144s /  56.604s |   0.054s /   3.294s |    0.429s /   3.642s |   3.628s /   6.732s


**NB:** FastDataTable currently does not support rows with a height of more than one line. See below for
more limitations, relative to the built-in DataTable.

## Installation

```bash
pip install textual-fastdatatable
```

## Usage

If you already have data in Apache Arrow or another common table format:

```py
from textual_fastdatatable import DataTable
data_table = DataTable(data = my_data)
```

The currently supported types are:

```py
AutoBackendType = Union[
    pa.Table,
    pa.RecordBatch,
    Path, # to parquet only
    str, # path to parquet only
    Sequence[Iterable[Any]],
    Mapping[str, Sequence[Any]],
]
```

To override the column labels and widths supplied by the backend:
```py
from textual_fastdatatable import DataTable
data_table = DataTable(data = my_data, column_labels=["Supports", "[red]Console[/]", "Markup!"], column_widths=[10, 5, None])
```

You can also pass in a `backend` manually (if you want more control or want to plug in your own).

```py
from textual_fastdatatable import ArrowBackend, DataTable, create_backend
backend = create_backend(my_data)
backend = ArrowBackend(my_arrow_table)
# from python dictionary in the form key: col_values
backend = ArrowBackend.from_pydict(
    {
        "col one": [1, 2, 3 ,4],
        "col two": ["a", "b", "c", "d"],
    }
)
# from a list of tuples or another sequence of iterables
backend = ArrowBackend.from_records(
    [
        ("col one", "col two"),
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
    ]
)
# from a path to a Parquet file:
backend = ArrowBackend.from_parquet("path/to/file.parquet")
```

## Limitations and Caveats

The `DataTable` does not currently support rows with a height of more than one line. Only the first line of each row will be displayed.

The `DataTable` does not currently support row labels.

The `ArrowBackend` is optimized to be fast for large, immutable datasets. Mutating the data,
especially adding or removing rows, may be slow.

The `ArrowBackend` cannot be initialized without data, however, the DataTable can (either with or without `column_labels`).

The `ArrowBackend` cannot store arbitrary Python objects or Rich Renderables as values. It may widen types to strings unnecessarily.

## Additional Features

`ctrl+c` will post a SelectionCopied message with a list of tuples of the values selected by the cursor. To use, initialize with `cursor_type=range` from an app that does NOT inherit bindings.

```py
from textual.app import App, ComposeResult

from textual_fastdatatable import ArrowBackend, DataTable


class TableApp(App, inherit_bindings=False):
    BINDINGS = [("ctrl+q", "quit", "Quit")]

    def compose(self) -> ComposeResult:
        backend = ArrowBackend.from_parquet("./tests/data/lap_times_538121.parquet")
        yield DataTable(backend=backend, cursor_type="range")


if __name__ == "__main__":
    app = TableApp()
    app.run()

```