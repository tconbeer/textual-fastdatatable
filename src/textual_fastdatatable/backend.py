from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Mapping, Sequence, Any, Union
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich.console import RenderableType
from rich.text import Text


class DataTableBackend(ABC):
    @property
    @abstractmethod
    def row_count(self) -> int:
        pass

    @property
    @abstractmethod
    def column_count(self) -> int:
        pass

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


class ArrowBackend(DataTableBackend):
    def __init__(self, data: pa.Table) -> None:
        super().__init__()
        self.data: pa.Table = data
        self.string_data: pa.Table = pa.Table.from_arrays(  # type: ignore
            arrays=[arr.cast("string") for arr in data.columns], names=data.column_names
        )

    @classmethod
    def from_records(cls, records: Sequence[tuple], has_header:bool=True) -> "ArrowBackend":
        headers = records[0] if has_header else range(len(records[0]))
        data = records[1:] if has_header else records
        pydict = {header: [row[i] for row in data] for i, header in enumerate(headers)}
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
    def column_content_widths(self) -> Sequence[int]:
        return [pc.max(pc.utf8_length(arr)).as_py() for arr in self.string_data.columns]  # type: ignore

    def get_row_at(self, index: int) -> Sequence[RenderableType]:
        row: Dict[str, RenderableType] = self.data.slice(index, length=1).to_pylist()[0]
        return list(row.values())

    def get_column_at(self, column_index: int) -> Sequence[RenderableType]:
        return self.data[column_index].to_pylist()

    def get_cell_at(self, row_index: int, column_index: int) -> RenderableType:
        return self.data[column_index][row_index].as_py()  # type: ignore
