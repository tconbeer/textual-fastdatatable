from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from textual_fastdatatable import ArrowBackend
from textual_fastdatatable.format import measure_width

MAX_64BIT_INT = 2**63 - 1


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
    pq.write_table(tbl, str(p))

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


TS_NS = 1_704_164_645_123_456_789
"""2024-01-02 03:04:05.123456789 UTC, which a `datetime` cannot hold."""


@pytest.mark.parametrize(
    "column_type,stored,expected_type,expected_text",
    [
        (pa.timestamp("ns"), TS_NS, datetime, "2024-01-02 03:04:05.123456789"),
        (
            pa.timestamp("ns", tz="America/New_York"),
            TS_NS,
            datetime,
            "2024-01-01 22:04:05.123456789-05:00",
        ),
        (pa.time64("ns"), 11_045_123_456_789, time, "03:04:05.123456789"),
        (pa.duration("ns"), 123_456_789, timedelta, "0:00:00.123456789"),
        (pa.duration("ns"), -1, timedelta, "-1 day, 23:59:59.999999999"),
        (pa.timestamp("ns"), -1, datetime, "1969-12-31 23:59:59.999999999"),
    ],
)
def test_nanosecond_values_keep_their_nanoseconds(
    column_type: pa.DataType, stored: int, expected_type: type, expected_text: str
) -> None:
    column = pa.array([stored, None], type=pa.int64()).cast(column_type)
    backend = ArrowBackend(pa.table({"ns": column, "n": [1, 2]}))

    cell = backend.get_cell_at(0, 0)
    assert isinstance(cell, expected_type)
    assert str(cell) == expected_text
    assert [str(value) for value in backend.get_row_at(0)] == [expected_text, "1"]
    assert backend.get_row_at(1) == [None, 2]
    assert [str(value) for value in backend.get_column_at(0)] == [
        expected_text,
        "None",
    ]
    assert backend.column_content_widths[0] == measure_width(cell)


def test_nanosecond_values_a_datetime_can_hold_are_datetimes() -> None:
    column = pa.array([1_000, 0], type=pa.int64()).cast(pa.timestamp("ns"))
    backend = ArrowBackend(pa.table({"ns": column}))

    assert backend.get_column_at(0) == [
        datetime(1970, 1, 1, microsecond=1),
        datetime(1970, 1, 1),
    ]
    assert type(backend.get_cell_at(0, 0)) is datetime


def test_infinite_nanosecond_timestamps() -> None:
    """duckdb stores `'infinity'::timestamp_ns` as the largest int64."""
    column = pa.array([MAX_64BIT_INT, -MAX_64BIT_INT], type=pa.int64()).cast(
        pa.timestamp("ns")
    )
    backend = ArrowBackend(pa.table({"ns": column}))

    assert backend.get_column_at(0) == [datetime.max, datetime.min]
    assert backend.get_row_at(0) == [datetime.max]
    assert backend.get_cell_at(1, 0) is datetime.min


def test_nested_nanosecond_values_keep_their_nanoseconds() -> None:
    timestamp = pa.scalar(TS_NS, type=pa.timestamp("ns"))
    table = pa.table(
        {
            "list": pa.array([[timestamp, None], None]),
            "struct": pa.array(
                [{"ts": timestamp, "n": 1}, None],
                type=pa.struct([("ts", pa.timestamp("ns")), ("n", pa.int64())]),
            ),
            "map": pa.array(
                [[("k", timestamp)], []],
                type=pa.map_(pa.string(), pa.timestamp("ns")),
            ),
        }
    )
    backend = ArrowBackend(table)

    expected = "2024-01-02 03:04:05.123456789"
    nested_list, nested_struct, nested_map = backend.get_row_at(0)
    assert [str(value) for value in nested_list] == [expected, "None"]
    assert str(nested_struct["ts"]) == expected
    assert nested_struct["n"] == 1
    assert [(key, str(value)) for key, value in nested_map] == [("k", expected)]
    assert backend.get_row_at(1) == [None, None, []]
    assert backend.get_column_at(0)[1] is None
    assert str(backend.get_cell_at(0, 2)[0][1]) == expected
    assert all(width > 0 for width in backend.column_content_widths)


def test_updating_a_nanosecond_cell_keeps_the_other_rows() -> None:
    column = pa.array([TS_NS, TS_NS], type=pa.int64()).cast(pa.timestamp("ns"))
    backend = ArrowBackend(pa.table({"ns": column}))

    backend.update_cell(1, 0, datetime(2020, 1, 1))

    assert str(backend.get_cell_at(0, 0)) == "2024-01-02 03:04:05.123456789"
    assert backend.get_cell_at(1, 0) == datetime(2020, 1, 1)
    assert backend.data.column(0).type == pa.timestamp("ns")


def test_a_row_with_an_overflowing_value_keeps_its_other_values() -> None:
    column = pa.array([MAX_64BIT_INT], type=pa.int64()).cast(pa.timestamp("s"))
    backend = ArrowBackend(pa.table({"ts": column, "n": [5]}))

    assert backend.get_row_at(0) == [datetime.max, 5]


def test_nanosecond_values_without_pandas() -> None:
    """pyarrow converts nanoseconds through pandas, when it can import it."""
    script = (
        "import importlib.abc, sys\n"
        "class NoPandas(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.partition('.')[0] == 'pandas':\n"
        "            raise ModuleNotFoundError(name)\n"
        "sys.meta_path.insert(0, NoPandas())\n"
        "import pyarrow as pa\n"
        "from textual_fastdatatable.backend import ArrowBackend\n"
        f"column = pa.array([{TS_NS}], type=pa.int64()).cast(pa.timestamp('ns'))\n"
        "backend = ArrowBackend(pa.table({'ts': column}))\n"
        "print(backend.column_content_widths[0])\n"
        "print(backend.get_row_at(0)[0])\n"
        "print(backend.get_column_at(0)[0])\n"
        "print(backend.get_cell_at(0, 0))\n"
        "print('pandas' in sys.modules)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )

    assert proc.returncode == 0, proc.stderr
    expected = "2024-01-02 03:04:05.123456789"
    assert proc.stdout.split("\n") == ["23", expected, expected, expected, "False", ""]
