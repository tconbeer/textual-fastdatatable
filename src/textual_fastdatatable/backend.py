from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Mapping, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq
import pyarrow.types as pt
from rich.console import Console

from textual_fastdatatable.formatter import measure_width

AutoBackendType = Union[
    pa.Table,
    pa.RecordBatch,
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
    elif isinstance(data, pa.RecordBatch):
        return ArrowBackend.from_batches(data, max_rows=max_rows)
    elif isinstance(data, Path) or isinstance(data, str):
        return ArrowBackend.from_parquet(data, max_rows=max_rows)
    elif isinstance(data, Sequence) and not data:
        return ArrowBackend(pa.table([]), max_rows=max_rows)
    elif isinstance(data, Sequence) and _is_iterable(data[0]):
        return ArrowBackend.from_records(data, max_rows=max_rows, has_header=has_header)
    elif (
        isinstance(data, Mapping)
        and isinstance(next(iter(data.keys())), str)
        and isinstance(next(iter(data.values())), Sequence)
    ):
        return ArrowBackend.from_pydict(data, max_rows=max_rows)
    else:
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


class DataTableBackend(ABC):
    @abstractmethod
    def __init__(self, data: Any, max_rows: int | None = None) -> None:
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


class ArrowBackend(DataTableBackend):
    def __init__(self, data: pa.Table, max_rows: int | None = None) -> None:
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
        tbl = pa.Table.from_pydict(dict(data))
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
        except (pl.ArrowNotImplementedError, pl.ArrowInvalid):

            def py_str(_ctx: Any, arr: pa.Array) -> str | pa.Array | pa.ChunkedArray:
                return pa.array([str(el) for el in arr], type=pa.string())

            udf_name = f"tfdt_pystr_{arr.type}"
            with suppress(pl.ArrowKeyError):  # already registered
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
