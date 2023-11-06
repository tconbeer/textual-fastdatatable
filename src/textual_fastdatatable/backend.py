from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Mapping, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.lib as pl
import pyarrow.parquet as pq

AutoBackendType = Union[
    pa.Table,
    pa.RecordBatch,
    Path,
    str,
    Sequence[Iterable[Any]],
    Mapping[str, Sequence[Any]],
]


def create_backend(data: AutoBackendType) -> DataTableBackend:
    if isinstance(data, pa.Table):
        return ArrowBackend(data)
    elif isinstance(data, pa.RecordBatch):
        return ArrowBackend.from_batches(data)
    elif isinstance(data, Path) or isinstance(data, str):
        return ArrowBackend.from_parquet(data)
    elif isinstance(data, Sequence) and isinstance(data[0], Iterable):
        return NativeBackend(data)
    elif (
        isinstance(data, Mapping)
        and isinstance(next(iter(data.keys())), str)
        and isinstance(next(iter(data.values())), Sequence)
    ):
        return ArrowBackend.from_pydict(data)
    else:
        raise TypeError(
            f"Cannot automatically create backend for data of type: {type(data)}. "
            f"Data must be of type: {AutoBackendType}."
        )


class DataTableBackend(ABC):
    @abstractmethod
    def __init__(self, data: Any) -> None:
        pass

    @property
    @abstractmethod
    def row_count(self) -> int:
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
    def __init__(self, data: pa.Table) -> None:
        self.data: pa.Table = data
        self._string_data: pa.Table | None = None
        self._column_content_widths: list[int] = []

    @staticmethod
    def _pydict_from_records(
        records: Sequence[Iterable[Any]], has_header: bool = True
    ) -> dict[str, list[Any]]:
        headers = records[0] if has_header else range(len(list(records[0])))
        data = list(map(list, records[1:] if has_header else records))
        pydict = {header: [row[i] for row in data] for i, header in enumerate(headers)}
        return pydict

    @classmethod
    def from_batches(cls, data: pa.RecordBatch) -> "ArrowBackend":
        tbl = pa.Table.from_batches([data])
        return cls(tbl)

    @classmethod
    def from_parquet(cls, path: Path | str) -> "ArrowBackend":
        tbl = pq.read_table(str(path))
        return cls(tbl)

    @classmethod
    def from_pydict(cls, data: Mapping[str, Sequence[Any]]) -> "ArrowBackend":
        tbl = pa.Table.from_pydict(dict(data))
        return cls(tbl)

    @classmethod
    def from_records(
        cls, records: Sequence[Iterable[Any]], has_header: bool = True
    ) -> "ArrowBackend":
        pydict = cls._pydict_from_records(records, has_header)
        return cls.from_pydict(pydict)

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
            if self._string_data is None:
                self._string_data = pa.Table.from_arrays(
                    arrays=[
                        self._safe_cast_arr_to_str(arr) for arr in self.data.columns
                    ],
                    names=self.data.column_names,
                )
            content_widths = [
                pc.max(pc.utf8_length(arr).fill_null(0)).as_py()
                for arr in self._string_data.itercolumns()
            ]
            # pc.max returns None for each column without rows; we need to return 0
            # instead.
            self._column_content_widths = [cw or 0 for cw in content_widths]

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
        if self._string_data is not None:
            self._string_data = self._string_data.append_column(
                label,
                arr,
            )
        if self._column_content_widths:
            self._column_content_widths.append(len(str(default)))
        return self.data.num_columns - 1

    def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
        rows = list(records)
        indicies = list(range(self.row_count, self.row_count + len(rows)))
        records_with_headers = [self.data.column_names, *rows]
        pydict = self._pydict_from_records(records_with_headers)
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
        new_type = pa.string() if pa.types.is_null(column.type) else column.type
        self.data = self.data.set_column(
            column_index,
            self.data.column_names[column_index],
            pa.array(pycolumn, type=new_type),
        )
        if self._string_data is not None:
            self._string_data = self._string_data.set_column(
                column_index,
                self.data.column_names[column_index],
                pa.array(pycolumn, type=pa.string()),
            )
        if self._column_content_widths:
            self._column_content_widths[column_index] = max(
                len(str(value)), self._column_content_widths[column_index]
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
        self._string_data = None
        self._column_content_widths = []

    @staticmethod
    def _safe_cast_arr_to_str(arr: pa._PandasConvertible) -> pa._PandasConvertible:
        """
        Safe here means avoiding type errors casting to str; ironically that means
        setting PyArrow safe=false. If PyArrow can't do the cast (as with structs
        and other nested types), we fall back to Python.
        """
        try:
            arr = arr.cast(
                pa.string(),
                safe=False,
            )
        except pl.ArrowNotImplementedError:
            # todo: vectorize this with a pyarrow udf
            native_list = arr.to_pylist()
            arr = pa.array([str(i) for i in native_list], type=pa.string())
        return arr.fill_null("")


class NativeBackend(DataTableBackend):
    def __init__(self, data: Sequence[Iterable[Any]]) -> None:
        if not data:
            self._columns: Sequence[str] = []
            self.data: Sequence[Sequence[Any]] = []
        elif hasattr(data[0], "__getitem__") and hasattr(data[1], "__getitem__"):
            self._columns = [str(col) for col in data[0]]
            self.data = data[1:]  # type: ignore
        else:
            self._columns = [str(col) for col in data[0]]
            self.data = [tuple(row) for row in data[1:]]

    @property
    def row_count(self) -> int:
        return len(self.data)

    @property
    def column_count(self) -> int:
        return len(self.columns)

    @property
    def columns(self) -> Sequence[str]:
        """
        A list of column labels
        """
        return self._columns

    @property
    def column_content_widths(self) -> Sequence[int]:
        """
        A list of integers corresponding to the widest utf8 string length
        of any data in each column.
        """
        return [
            max((len(str(row[i])) for row in self.data))
            for i in range(len(self.columns))
        ]

    def get_row_at(self, index: int) -> Sequence[Any]:
        return self.data[index]

    def get_column_at(self, index: int) -> Sequence[Any]:
        return [row[index] for row in self.data]

    def get_cell_at(self, row_index: int, column_index: int) -> Any:
        return self.data[row_index][column_index]

    def append_column(self, label: str, default: Any | None = None) -> int:
        """
        Returns column index
        """
        new_idx = len(self.columns)
        if hasattr(self._columns, "append"):
            self._columns.append(label)
        else:
            self._columns = list(self._columns)
            self._columns.append(label)
        self.data = [(*row, default) for row in self.data]
        return new_idx

    def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
        """
        Returns new row indicies
        """
        old_len = len(self.data)
        if not hasattr(self.data, "append"):
            self.data = list(self.data)
        for row in records:
            self.data.append(tuple(row))
        new_len = len(self.data)
        return list(range(old_len, new_len))

    def drop_row(self, row_index: int) -> None:
        if hasattr(self.data, "__delitem__"):
            del self.data[row_index]
        else:
            self.data = [*self.data[:row_index], *self.data[row_index:]]

    def update_cell(self, row_index: int, column_index: int, value: Any) -> None:
        """
        Raises IndexError if bad indicies
        """
        try:
            self.data[row_index][column_index] = value  # type: ignore
        except TypeError:
            row = self.data[row_index]
            new_row = (*row[:column_index], value, *row[column_index + 1 :])
            try:
                self.data[row_index] = new_row  # type: ignore
            except TypeError:
                self.data = [
                    *self.data[:row_index],
                    new_row,
                    *self.data[row_index + 1 :],
                ]

    def sort(
        self, by: list[tuple[str, Literal["ascending", "descending"]]] | str
    ) -> None:
        """
        by: str sorts table by the data in the column with that name (asc).
        by: list[tuple] sorts the table by the named column(s) with the directions
            indicated.
        """
        if isinstance(by, str):
            by = [(by, "ascending")]
        for col_name, dir in reversed(by):
            self.data = sorted(
                self.data,
                key=lambda x: x[self.columns.index(col_name)],
                reverse=dir == "descending",
            )
