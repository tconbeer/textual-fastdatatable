# textual-fastdatatable
A performance-focused reimplementation of Textual's DataTable widget, with a pluggable data storage backend.

Textual's built-in DataTable widget is beautiful and powerful, but it can be slow to load large datasets.

Here are some benchmarks on my relatively weak laptop. For each benchmark, we initialize a Textual App that
loads a dataset from a parquet file and mounts a data table; it then scrolls around the table
(10 pagedowns and 15 right arrows). For the built-in table, the data is loaded into memory before the timer
is started; the Arrow back-end reads directly from parquet, so the timer is started immediately.

The times in each column represent the time to the first paint of the table, and the time after scrolling
is completed (we wait until the table is fully rendered after each scroll):

Records |Built-In DataTable | FastDataTable (Arrow)
--------|--------|--------
lap_times_100.parquet |   0.024s /   1.741s |   0.020s /   1.751s
lap_times_1000.parquet |   0.107s /   1.997s |   0.022s /   1.913s
lap_times_10000.parquet |   1.071s /   3.016s |   0.022s /   1.956s
lap_times_100000.parquet |  10.803s /  13.086s |   0.038s /   2.162s
lap_times_538121.parquet |  60.795s /  64.837s |   0.085s /   1.928s
wide_10000.parquet |   4.655s /   9.987s |   0.025s /   3.205s
wide_100000.parquet |  49.764s /  55.258s |   0.062s /   3.209s


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