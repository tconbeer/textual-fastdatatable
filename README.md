# textual-fastdatatable
A performance-focused reimplementation of Textual's DataTable widget, with a pluggable data storage backend.

Textual's built-in DataTable widget is beautiful and powerful, but it can be slow to load large datasets.

Here are some benchmarks on my relatively weak laptop. For each benchmark, we initialize a Textual App that
loads a 6-column dataset from a parquet file and mounts a data table:

Records | Built-in DataTable | FastDataTable
--------|--------------------|--------------
100|0.21s|0.12s
1000|0.50s|0.12s
10000|3.78s|0.12s
100000|37.84s|0.14s
538121|231.17s|0.18s

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

The `ArrowBackend` is optimized to be fast for large, immutable datasets. Mutating the data,
especially adding or removing rows, may be slow.

The `ArrowBackend` cannot be initialized without data, however, the DataTable can (either with or without `column_labels`).

The `ArrowBackend` cannot store arbitrary Python objects or Rich Renderables as values. It may widen types to strings unnecessarily.