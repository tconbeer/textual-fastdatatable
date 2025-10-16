from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import pyarrow as pa

from textual_fastdatatable import ArrowBackend


def test_from_records(records: list[tuple[str | int, ...]]) -> None:
    backend = ArrowBackend.from_records(records, has_header=True)
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert tuple(backend.columns) == records[0]


def test_from_records_no_header(records: list[tuple[str | int, ...]]) -> None:
    backend = ArrowBackend.from_records(records[1:], has_header=False)
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert tuple(backend.columns) == ("f0", "f1", "f2")


def test_from_pydict(pydict: dict[str, Sequence[str | int]]) -> None:
    backend = ArrowBackend.from_pydict(pydict)
    assert backend.column_count == 3
    assert backend.row_count == 5
    assert backend.source_row_count == 5
    assert tuple(backend.columns) == tuple(pydict.keys())


def test_from_pydict_with_limit(pydict: dict[str, Sequence[str | int]]) -> None:
    backend = ArrowBackend.from_pydict(pydict, max_rows=2)
    assert backend.column_count == 3
    assert backend.row_count == 2
    assert backend.source_row_count == 5
    assert tuple(backend.columns) == tuple(pydict.keys())


def test_from_parquet(pydict: dict[str, Sequence[str | int]], tmp_path: Path) -> None:
    tbl = pa.Table.from_pydict(pydict)
    p = tmp_path / "test.parquet"
    pa.parquet.write_table(tbl, str(p))

    backend = ArrowBackend.from_parquet(p)
    assert backend.data.equals(tbl)


def test_empty_query() -> None:
    data: dict[str, list] = {"a": []}
    backend = ArrowBackend.from_pydict(data)
    assert backend.column_content_widths == [0]


def test_dupe_column_labels() -> None:
    arr = pa.array([0, 1, 2, 3])
    tab = pa.table([arr] * 3, names=["a", "a", "a"])
    backend = ArrowBackend(data=tab)
    assert backend.column_count == 3
    assert backend.row_count == 4
    assert backend.get_row_at(2) == [2, 2, 2]


def test_timestamp_with_tz() -> None:
    """
    Ensure datetimes with offsets but no names do not crash the data table
    when casting to string.
    """
    dt = datetime(2024, 1, 1, hour=15, tzinfo=timezone(offset=timedelta(hours=-5)))
    arr = pa.array([dt, dt, dt])
    tab = pa.table([arr], names=["created_at"])
    backend = ArrowBackend(data=tab)
    assert backend.column_content_widths == [29]


def test_mixed_types() -> None:
    data = [(1000,), ("hi",)]
    backend = ArrowBackend.from_records(records=data)
    assert backend
    assert backend.row_count == 2
    assert backend.get_row_at(0) == ["1000"]
    assert backend.get_row_at(1) == ["hi"]


def test_negative_timestamps() -> None:
    dt = datetime(1, 1, 1, tzinfo=timezone.utc)
    arr = pa.array([dt, dt, dt], type=pa.timestamp("s", tz="America/New_York"))
    tab = pa.table([arr], names=["created_at"])
    backend = ArrowBackend(data=tab)
    assert backend.column_content_widths == [26]
    assert backend.get_column_at(0) == [datetime.min, datetime.min, datetime.min]
    assert backend.get_row_at(0) == [datetime.min]
    assert backend.get_cell_at(0, 0) is datetime.min
