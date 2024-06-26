from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import suppress
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    Literal,
    Mapping,
    Sequence,
    TypeVar,
    Union,
)

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.lib as pal
import pyarrow.parquet as pq
import pyarrow.types as pt
from rich.console import Console

import polars as pl
import polars.datatypes as pld

from textual_fastdatatable.formatter import measure_width

AutoBackendType = Union[
    pa.Table,
    pa.RecordBatch,
    pl.DataFrame,
    Path,
    str,
    Sequence[Iterable[Any]],
    Mapping[str, Sequence[Any]],
]


def create_backend(
    data: AutoBackendType,
    max_rows: int | None = None,
    has_header: bool = False,
) -> DataTableBackend:
    if isinstance(data, pa.Table):
        return ArrowBackend(data, max_rows=max_rows)
    if isinstance(data, pa.RecordBatch):
        return ArrowBackend.from_batches(data, max_rows=max_rows)
    if isinstance(data, pl.DataFrame):
        return PolarsBackend.from_dataframe(data, max_rows=max_rows)

    if isinstance(data, Path) or isinstance(data, str):
        data = Path(data)
        if data.suffix in [".pqt", ".parquet"]:
            return ArrowBackend.from_parquet(data, max_rows=max_rows)

        return PolarsBackend.from_file_path(
            data, max_rows=max_rows, has_header=has_header
        )
    if isinstance(data, Sequence) and not data:
        return ArrowBackend(pa.table([]), max_rows=max_rows)
    if isinstance(data, Sequence) and _is_iterable(data[0]):
        return ArrowBackend.from_records(data, max_rows=max_rows, has_header=has_header)

    if (
        isinstance(data, Mapping)
        and isinstance(next(iter(data.keys())), str)
        and isinstance(next(iter(data.values())), Sequence)
    ):
        return ArrowBackend.from_pydict(data, max_rows=max_rows)

    raise TypeError(
        f"Cannot automatically create backend for data of type: {type(data)}. "
        f"Data must be of type: {AutoBackendType}."
    )


def _is_iterable(item: Any) -> bool:
    try:
        iter(item)
    except TypeError:
        return False
    else:
        return True


_TableTypeT = TypeVar("_TableTypeT")


class DataTableBackend(ABC, Generic[_TableTypeT]):
    @abstractmethod
    def __init__(self, data: _TableTypeT, max_rows: int | None = None) -> None:
        pass

    @property
    @abstractmethod
    def source_data(self) -> _TableTypeT:
        """
        Return the source data as an Arrow table
        """
        pass

    @property
    @abstractmethod
    def source_row_count(self) -> int:
        """
        The number of rows in the source data, before filtering down to max_rows
        """
        pass

    @property
    @abstractmethod
    def row_count(self) -> int:
        """
        The number of rows in backend's retained data, after filtering down to max_rows
        """
        pass

    @property
    def column_count(self) -> int:
        return len(self.columns)

    @property
    @abstractmethod
    def columns(self) -> Sequence[str]:
        """
        A list of column labels
        """
        pass

    @property
    @abstractmethod
    def column_content_widths(self) -> Sequence[int]:
        """
        A list of integers corresponding to the widest utf8 string length
        of any data in each column.
        """
        pass

    @abstractmethod
    def get_row_at(self, index: int) -> Sequence[Any]:
        pass

    @abstractmethod
    def get_column_at(self, index: int) -> Sequence[Any]:
        pass

    @abstractmethod
    def get_cell_at(self, row_index: int, column_index: int) -> Any:
        pass

    @abstractmethod
    def append_column(self, label: str, default: Any | None = None) -> int:
        """
        Returns column index
        """

    @abstractmethod
    def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
        """
        Returns new row indicies
        """
        pass

    @abstractmethod
    def drop_row(self, row_index: int) -> None:
        pass

    @abstractmethod
    def update_cell(self, row_index: int, column_index: int, value: Any) -> None:
        """
        Raises IndexError if bad indicies
        """

    @abstractmethod
    def sort(
        self, by: list[tuple[str, Literal["ascending", "descending"]]] | str
    ) -> None:
        """
        by: str sorts table by the data in the column with that name (asc).
        by: list[tuple] sorts the table by the named column(s) with the directions
            indicated.
        """


class ArrowBackend(DataTableBackend[pa.Table]):
    def __init__(self, data: pa.Table, max_rows: int | None = None) -> None:
        self._source_data = data

        # Arrow allows duplicate field names, but a table's to_pylist() and
        # to_pydict() methods will drop duplicate-named fields!
        field_names: list[str] = []
        renamed = False
        for field in data.column_names:
            n = 0
            while field in field_names:
                field = f"{field}{n}"
                renamed = True
                n += 1
            field_names.append(field)
        if renamed:
            data = data.rename_columns(field_names)

        self._source_row_count = data.num_rows
        if max_rows is not None and max_rows < self._source_row_count:
            self.data = data.slice(offset=0, length=max_rows)
        else:
            self.data = data
        self._console = Console()
        self._column_content_widths: list[int] = []

    @staticmethod
    def _pydict_from_records(
        records: Sequence[Iterable[Any]], has_header: bool = False
    ) -> dict[str, list[Any]]:
        headers = (
            records[0]
            if has_header
            else [f"f{i}" for i in range(len(list(records[0])))]
        )
        data = list(map(list, records[1:] if has_header else records))
        pydict = {header: [row[i] for row in data] for i, header in enumerate(headers)}
        return pydict

    @classmethod
    def from_batches(
        cls, data: pa.RecordBatch, max_rows: int | None = None
    ) -> "ArrowBackend":
        tbl = pa.Table.from_batches([data])
        return cls(tbl, max_rows=max_rows)

    @classmethod
    def from_parquet(
        cls, path: Path | str, max_rows: int | None = None
    ) -> "ArrowBackend":
        tbl = pq.read_table(str(path))
        return cls(tbl, max_rows=max_rows)

    @classmethod
    def from_pydict(
        cls, data: Mapping[str, Sequence[Any]], max_rows: int | None = None
    ) -> "ArrowBackend":
        try:
            tbl = pa.Table.from_pydict(dict(data))
        except (pal.ArrowInvalid, pal.ArrowTypeError):
            # one or more fields has mixed types, like int and
            # string. Cast all to string for safety
            new_data = {k: [str(val) for val in v] for k, v in data.items()}
            tbl = pa.Table.from_pydict(new_data)
        return cls(tbl, max_rows=max_rows)

    @classmethod
    def from_records(
        cls,
        records: Sequence[Iterable[Any]],
        has_header: bool = False,
        max_rows: int | None = None,
    ) -> "ArrowBackend":
        pydict = cls._pydict_from_records(records, has_header)
        return cls.from_pydict(pydict, max_rows=max_rows)

    @property
    def source_data(self) -> pa.Table:
        return self._source_data

    @property
    def source_row_count(self) -> int:
        return self._source_row_count

    @property
    def row_count(self) -> int:
        return self.data.num_rows

    @property
    def column_count(self) -> int:
        return self.data.num_columns

    @property
    def columns(self) -> Sequence[str]:
        return self.data.column_names

    @property
    def column_content_widths(self) -> list[int]:
        if not self._column_content_widths:
            measurements = [self._measure(arr) for arr in self.data.columns]
            # pc.max returns None for each column without rows; we need to return 0
            # instead.
            self._column_content_widths = [cw or 0 for cw in measurements]

        return self._column_content_widths

    def get_row_at(self, index: int) -> Sequence[Any]:
        row: Dict[str, Any] = self.data.slice(index, length=1).to_pylist()[0]
        return list(row.values())

    def get_column_at(self, column_index: int) -> Sequence[Any]:
        return self.data[column_index].to_pylist()

    def get_cell_at(self, row_index: int, column_index: int) -> Any:
        return self.data[column_index][row_index].as_py()

    def append_column(self, label: str, default: Any | None = None) -> int:
        """
        Returns column index
        """
        if default is None:
            arr: pa.Array = pa.nulls(self.row_count)
        else:
            arr = pa.nulls(self.row_count, type=pa.string())
            arr = arr.fill_null(str(default))

        self.data = self.data.append_column(label, arr)
        if self._column_content_widths:
            self._column_content_widths.append(measure_width(default, self._console))
        return self.data.num_columns - 1

    def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
        rows = list(records)
        indicies = list(range(self.row_count, self.row_count + len(rows)))
        records_with_headers = [self.data.column_names, *rows]
        pydict = self._pydict_from_records(records_with_headers, has_header=True)
        old_rows = self.data.to_batches()
        new_rows = pa.RecordBatch.from_pydict(
            pydict,
            schema=self.data.schema,
        )
        self.data = pa.Table.from_batches([*old_rows, new_rows])
        self._reset_content_widths()
        return indicies

    def drop_row(self, row_index: int) -> None:
        if row_index < 0 or row_index >= self.row_count:
            raise IndexError(f"Can't drop row {row_index} of {self.row_count}")
        above = self.data.slice(0, row_index).to_batches()
        below = self.data.slice(row_index + 1).to_batches()
        self.data = pa.Table.from_batches([*above, *below])
        self._reset_content_widths()
        pass

    def update_cell(self, row_index: int, column_index: int, value: Any) -> None:
        column = self.data.column(column_index)
        pycolumn = column.to_pylist()
        pycolumn[row_index] = value
        new_type = pa.string() if pt.is_null(column.type) else column.type
        self.data = self.data.set_column(
            column_index,
            self.data.column_names[column_index],
            pa.array(pycolumn, type=new_type),
        )
        if self._column_content_widths:
            self._column_content_widths[column_index] = max(
                measure_width(value, self._console),
                self._column_content_widths[column_index],
            )

    def sort(
        self, by: list[tuple[str, Literal["ascending", "descending"]]] | str
    ) -> None:
        """
        by: str sorts table by the data in the column with that name (asc).
        by: list[tuple] sorts the table by the named column(s) with the directions
            indicated.
        """
        self.data = self.data.sort_by(by)

    def _reset_content_widths(self) -> None:
        self._column_content_widths = []

    def _measure(self, arr: pa._PandasConvertible) -> int:
        # with some types we can measure the width more efficiently
        if pt.is_boolean(arr.type):
            return 7
        elif pt.is_null(arr.type):
            return 0
        elif (
            pt.is_integer(arr.type)
            or pt.is_floating(arr.type)
            or pt.is_decimal(arr.type)
        ):
            col_max = pc.max(arr.fill_null(0)).as_py()
            col_min = pc.min(arr.fill_null(0)).as_py()
            return max([measure_width(el, self._console) for el in [col_max, col_min]])
        elif pt.is_temporal(arr.type):
            try:
                value = arr.drop_null()[0].as_py()
            except IndexError:
                return 0
            else:
                return measure_width(value, self._console)

        # for everything else, we need to compute it

        try:
            arr = arr.cast(
                pa.string(),
                safe=False,
            )
        except (pal.ArrowNotImplementedError, pal.ArrowInvalid):

            def py_str(_ctx: Any, arr: pa.Array) -> str | pa.Array | pa.ChunkedArray:
                return pa.array([str(el) for el in arr], type=pa.string())

            udf_name = f"tfdt_pystr_{arr.type}"
            with suppress(pal.ArrowKeyError):  # already registered
                pc.register_scalar_function(
                    py_str,
                    function_name=udf_name,
                    function_doc={"summary": "str", "description": "built-in str"},
                    in_types={"arr": arr.type},
                    out_type=pa.string(),
                )

            arr = pc.call_function(udf_name, [arr])
        width: int = pc.max(pc.utf8_length(arr.fill_null("")).fill_null(0)).as_py()
        return width


try:
    import polars as pl
    import polars.datatypes as pld

    class PolarsBackend(DataTableBackend[pl.DataFrame]):

        @classmethod
        def from_file_path(
            cls, path: Path, max_rows: int | None = None, has_header: bool = True
        ) -> "PolarsBackend":
            if path.suffix in [".arrow", ".feather"]:
                tbl = pl.read_ipc(path)
            elif path.suffix == ".arrows":
                tbl = pl.read_ipc_stream(path)
            elif path.suffix == ".json":
                tbl = pl.read_json(path)
            elif path.suffix == ".csv":
                tbl = pl.read_csv(path, has_header=has_header)
            else:
                raise TypeError(
                    f"Dont know how to load file type {path.suffix} for {path}"
                )
            return cls(tbl, max_rows=max_rows)

        @classmethod
        def from_pydict(
            cls, pydict: dict[str, Sequence[str | int]], max_rows: int | None = None
        ) -> "PolarsBackend":
            return cls(pl.from_dict(pydict), max_rows=max_rows)

        @classmethod
        def from_dataframe(
            cls, frame: pl.DataFrame, max_rows: int | None = None
        ) -> "PolarsBackend":
            return cls(frame, max_rows=max_rows)

        def __init__(self, data: pl.DataFrame, max_rows: int | None = None) -> None:
            self._source_data = data

            # Arrow allows duplicate field names, but a table's to_pylist() and
            # to_pydict() methods will drop duplicate-named fields!
            field_names: list[str] = []
            for field in data.columns:
                n = 0
                while field in field_names:
                    field = f"{field}{n}"
                    n += 1
                field_names.append(field)
            data.columns = field_names

            self._source_row_count = len(data)
            if max_rows is not None and max_rows < self._source_row_count:
                self.data = data.slice(offset=0, length=max_rows)
            else:
                self.data = data
            self._console = Console()
            self._column_content_widths: list[int] = []

        @property
        def source_data(self) -> pl.DataFrame:
            return self._source_data

        @property
        def source_row_count(self) -> int:
            return self._source_row_count

        @property
        def row_count(self) -> int:
            return len(self.data)

        @property
        def column_count(self) -> int:
            return len(self.data.columns)

        @property
        def columns(self) -> Sequence[str]:
            return self.data.columns

        def get_row_at(self, index: int) -> Sequence[Any]:
            if index < 0 or index >= len(self.data):
                raise IndexError(
                    f"Cannot get row={index} in table with {len(self.data)} rows and {len(self.data.columns)} cols"
                )
            return list(self.data.slice(index, length=1).to_dicts()[0].values())

        def get_column_at(self, column_index: int) -> Sequence[Any]:
            if column_index < 0 or column_index >= len(self.data.columns):
                raise IndexError(
                    f"Cannot get column={column_index} in table with {len(self.data)} rows and {len(self.data.columns)} cols"
                )
            return list(self.data.to_series(column_index))

        def get_cell_at(self, row_index: int, column_index: int) -> Any:
            if (
                row_index >= len(self.data)
                or row_index < 0
                or column_index < 0
                or column_index >= len(self.data.columns)
            ):
                raise IndexError(
                    f"Cannot get cell at row={row_index} col={column_index} in table with {len(self.data)} rows and {len(self.data.columns)} cols"
                )
            return self.data.to_series(column_index)[row_index]

        def drop_row(self, row_index: int) -> None:
            if row_index < 0 or row_index >= self.row_count:
                raise IndexError(f"Can't drop row {row_index} of {self.row_count}")
            above = self.data.slice(0, row_index)
            below = self.data.slice(row_index + 1)
            self.data = pl.concat([above, below])
            self._reset_content_widths()

        def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
            rows_to_add = pl.from_dicts(
                [dict(zip(self.data.columns, row)) for row in records]
            )
            indicies = list(range(self.row_count, self.row_count + len(rows_to_add)))
            self.data = pl.concat([self.data, rows_to_add])
            self._reset_content_widths()
            return indicies

        def append_column(self, label: str, default: Any | None = None) -> int:
            """
            Returns column index
            """
            self.data = self.data.with_columns(
                pl.Series([default])
                .extend_constant(default, self.row_count - 1)
                .alias(label)
            )
            if self._column_content_widths:
                self._column_content_widths.append(
                    measure_width(default, self._console)
                )
            return len(self.data.columns) - 1

        def _reset_content_widths(self) -> None:
            self._column_content_widths = []

        def update_cell(self, row_index: int, column_index: int, value: Any) -> None:
            if row_index >= len(self.data) or column_index >= len(self.data.columns):
                raise IndexError(
                    f"Cannot update cell at row={row_index} col={column_index} in table with {len(self.data)} rows and {len(self.data.columns)} cols"
                )
            col_name = self.data.columns[column_index]
            self.data = self.data.with_columns(
                self.data.to_series(column_index)
                .scatter(row_index, value)
                .alias(col_name)
            )
            if self._column_content_widths:
                self._column_content_widths[column_index] = max(
                    measure_width(value, self._console),
                    self._column_content_widths[column_index],
                )

        @property
        def column_content_widths(self) -> list[int]:
            if not self._column_content_widths:
                measurements = [
                    self._measure(self.data[arr]) for arr in self.data.columns
                ]
                # pc.max returns None for each column without rows; we need to return 0
                # instead.
                self._column_content_widths = [cw or 0 for cw in measurements]

            return self._column_content_widths

        def _measure(self, arr: pl.Series) -> int:
            # with some types we can measure the width more efficiently
            dtype = arr.dtype
            if dtype == pld.Categorical():
                return self._measure(arr.cat.get_categories())

            if dtype.is_decimal() or dtype.is_float() or dtype.is_integer():
                col_max = arr.max()
                col_min = arr.min()
                return max(
                    [measure_width(el, self._console) for el in [col_max, col_min]]
                )
            if dtype.is_temporal():
                try:
                    value = arr.drop_null()[0].as_py()
                except IndexError:
                    return 0
                else:
                    return measure_width(value, self._console)
            if dtype.is_(pld.Boolean()):
                return 7

            # for everything else, we need to compute it

            arr = arr.cast(
                pl.Utf8(),
                strict=False,
            )
            width = arr.fill_null("<null>").str.len_chars().max()
            assert width is not None
            return width

        def sort(
            self, by: list[tuple[str, Literal["ascending", "descending"]]] | str
        ) -> None:
            """
            by: str sorts table by the data in the column with that name (asc).
            by: list[tuple] sorts the table by the named column(s) with the directions
                indicated.
            """
            if isinstance(by, str):
                cols = [by]
                typs = [False]
            else:
                cols = [x for x, _ in by]
                typs = [x == "descending" for _, x in by]
            self.data = self.data.sort(cols, descending=typs)

except ImportError:
    pass
