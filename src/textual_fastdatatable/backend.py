from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Mapping, Sequence, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich.console import RenderableType
from rich.text import Text


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
    def columns(self) -> Sequence[Text]:
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
    def get_row_at(self, index: int) -> Sequence[RenderableType]:
        pass

    @abstractmethod
    def get_column_at(self, index: int) -> Sequence[RenderableType]:
        pass

    @abstractmethod
    def get_cell_at(self, row_index: int, column_index: int) -> RenderableType:
        pass

    @abstractmethod
    def append_column(self, label: str, default: Any) -> int:
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
    def from_records(
        cls, records: Sequence[Iterable[Any]], has_header: bool = True
    ) -> "ArrowBackend":
        pydict = cls._pydict_from_records(records, has_header)
        return cls.from_pydict(pydict)

    @classmethod
    def from_pydict(cls, data: Mapping[str, Sequence[Any]]) -> "ArrowBackend":
        tbl = pa.Table.from_pydict(dict(data))
        return cls(tbl)

    @classmethod
    def from_parquet(cls, path: Union[Path, str]) -> "ArrowBackend":
        tbl = pq.read_table(str(path))
        return cls(tbl)

    @property
    def row_count(self) -> int:
        return self.data.num_rows

    @property
    def column_count(self) -> int:
        return self.data.num_columns

    @property
    def columns(self) -> Sequence[Text]:
        return [Text(label) for label in self.data.column_names]

    @property
    def column_content_widths(self) -> list[int]:
        if not self._column_content_widths:
            if self._string_data is None:
                self._string_data = pa.Table.from_arrays(  # type: ignore
                    arrays=[arr.cast("string") for arr in self.data.columns],
                    names=self.data.column_names,
                )
            self._column_content_widths = [
                pc.max(pc.utf8_length(arr)).as_py()  # type: ignore
                for arr in self._string_data.itercolumns()
            ]
        return self._column_content_widths

    def _reset_content_widths(self) -> None:
        self._column_content_widths = []

    def _maybe_update_content_width(self, column_index: int, w: int) -> None:
        self._column_content_widths[column_index] = max(
            self._column_content_widths[column_index], w
        )

    def get_row_at(self, index: int) -> Sequence[RenderableType]:
        row: Dict[str, RenderableType] = self.data.slice(index, length=1).to_pylist()[0]
        return list(row.values())

    def get_column_at(self, column_index: int) -> Sequence[RenderableType]:
        return self.data[column_index].to_pylist()

    def get_cell_at(self, row_index: int, column_index: int) -> RenderableType:
        return self.data[column_index][row_index].as_py()  # type: ignore

    def append_column(self, label: str, default: Any | None = None) -> int:
        """
        Returns column index
        """
        if default is None:
            default = ""

        column_data = pa.array([default] * self.row_count)
        column_string = pa.array([str(default)] * self.row_count, type=pa.string())
        self.data = self.data.append_column(label, column_data)  # type: ignore
        if self._string_data is not None:
            self._string_data = self._string_data.append_column(
                label,
                column_string,  # type: ignore
            )
        self._reset_content_widths()
        return self.data.num_columns - 1

    def append_rows(self, records: Iterable[Iterable[Any]]) -> list[int]:
        rows = list(records)
        indicies = list(range(self.row_count, self.row_count + len(rows)))
        records_with_headers = [self.data.column_names, *rows]
        pydict = self._pydict_from_records(records_with_headers)
        old_rows = self.data.to_batches()
        new_rows = pa.RecordBatch.from_pydict(
            pydict,  # type: ignore
            schema=self.data.schema,
        )
        self.data = pa.Table.from_batches([*old_rows, new_rows])  # type: ignore
        self.string_data = pa.Table.from_arrays(  # type: ignore
            arrays=[arr.cast("string") for arr in self.data.columns],
            names=self.data.column_names,
        )
        self._reset_content_widths()
        return indicies

    def drop_row(self, row_index: int) -> None:
        if row_index < 0 or row_index > self.row_count:
            raise IndexError(f"Can't drop row {row_index} of {self.row_count}")
        above = self.data.slice(0, row_index).to_batches()
        below = self.data.slice(row_index + 1).to_batches()
        self.data = pa.Table.from_batches([*above, *below])  # type: ignore
        self.string_data = pa.Table.from_arrays(  # type: ignore
            arrays=[arr.cast("string") for arr in self.data.columns],
            names=self.data.column_names,
        )
        self._reset_content_widths()
        pass

    def update_cell(self, row_index: int, column_index: int, value: Any) -> None:
        column = self.data.column(column_index)
        pycolumn = column.to_pylist()
        pycolumn[row_index] = value
        self.data = self.data.set_column(
            column_index,
            self.data.column_names[column_index],
            pa.array(pycolumn, type=column.type),  # type: ignore
        )
        if self._string_data is not None:
            self._string_data = self._string_data.set_column(
                column_index,
                self.data.column_names[column_index],
                pa.array(pycolumn, type=pa.string()),  # type: ignore
            )
        self._reset_content_widths()

    def sort(
        self, by: list[tuple[str, Literal["ascending", "descending"]]] | str
    ) -> None:
        """
        by: str sorts table by the data in the column with that name (asc).
        by: list[tuple] sorts the table by the named column(s) with the directions
            indicated.
        """
        self.data = self.data.sort_by(by)  # type: ignore
