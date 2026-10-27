from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

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

TS_US = datetime(2024, 1, 2, 3, 4, 5, 123456)


def _array(values: list[Any], data_type: pa.DataType | None = None) -> pa.Array:
    """`pa.array`, narrowed: these arrays are never chunked."""
    array = pa.array(values, type=data_type)
    assert isinstance(array, pa.Array)
    return array


def _ns_array(values: list[int | None], data_type: pa.DataType) -> pa.Array:
    return _array(values, pa.int64()).cast(data_type)


@pytest.mark.parametrize(
    "column_type,stored,expected",
    [
        (pa.timestamp("ns"), TS_NS, TS_US),
        (
            pa.timestamp("ns", tz="America/New_York"),
            TS_NS,
            TS_US.replace(tzinfo=timezone.utc),
        ),
        (pa.time64("ns"), 11_045_123_456_789, time(3, 4, 5, 123456)),
        (pa.duration("ns"), 123_456_789, timedelta(microseconds=123456)),
        (pa.timestamp("ns"), -1, datetime(1969, 12, 31, 23, 59, 59, 999999)),
    ],
)
def test_nanosecond_values_are_truncated_to_microseconds(
    column_type: pa.DataType, stored: int, expected: Any
) -> None:
    backend = ArrowBackend(
        pa.table({"ns": _ns_array([stored, None], column_type), "n": [1, 2]})
    )

    cell = backend.get_cell_at(0, 0)
    assert cell == expected
    assert type(cell) is type(expected)
    assert backend.get_row_at(0) == [expected, 1]
    assert backend.get_row_at(1) == [None, 2]
    assert backend.get_column_at(0) == [expected, None]
    assert backend.column_content_widths[0] == measure_width(cell)


def test_nanosecond_timestamps_keep_their_time_zone() -> None:
    column_type = pa.timestamp("ns", tz="America/New_York")
    backend = ArrowBackend(pa.table({"ns": _ns_array([TS_NS], column_type)}))

    cell = backend.get_cell_at(0, 0)
    assert cell.utcoffset() == timedelta(hours=-5)
    assert cell.isoformat() == "2024-01-01T22:04:05.123456-05:00"


def test_infinite_nanosecond_timestamps() -> None:
    """duckdb stores `'infinity'::timestamp_ns` as the largest int64."""
    column = _ns_array([MAX_64BIT_INT, -MAX_64BIT_INT], pa.timestamp("ns"))
    backend = ArrowBackend(pa.table({"ns": column}))

    assert backend.get_column_at(0) == [datetime.max, datetime.min]
    assert backend.get_row_at(0) == [datetime.max]
    assert backend.get_cell_at(1, 0) is datetime.min
    assert backend.column_content_widths == [26]


def _nested(values: list[Any], int_type: pa.DataType, ns_type: pa.DataType) -> Any:
    return pa.array(values, type=int_type).cast(ns_type)


TIMESTAMPS = _ns_array([TS_NS, TS_NS], pa.timestamp("ns"))
_UNION_CHILDREN = ["ts", "n"]


@pytest.mark.parametrize(
    "column,expected",
    [
        (
            _nested(
                [[TS_NS, TS_NS], []],
                pa.list_(pa.int64()),
                pa.list_(pa.timestamp("ns")),
            ),
            [[TS_US, TS_US], []],
        ),
        (
            _nested(
                [{"ts": TS_NS, "n": 1}, {"ts": TS_NS, "n": 2}],
                pa.struct([("ts", pa.int64()), ("n", pa.int64())]),
                pa.struct([("ts", pa.timestamp("ns")), ("n", pa.int64())]),
            ),
            [{"ts": TS_US, "n": 1}, {"ts": TS_US, "n": 2}],
        ),
        (
            _nested(
                [[("a", TS_NS)], [("b", TS_NS)]],
                pa.map_(pa.string(), pa.int64()),
                pa.map_(pa.string(), pa.timestamp("ns")),
            ),
            [[("a", TS_US)], [("b", TS_US)]],
        ),
        (TIMESTAMPS.dictionary_encode(), [TS_US, TS_US]),
        (
            pa.UnionArray.from_sparse(
                _array([0, 1], pa.int8()),
                [TIMESTAMPS, _array([7, 8])],
                field_names=_UNION_CHILDREN,
            ),
            [TS_US, 8],
        ),
        (
            pa.UnionArray.from_dense(
                _array([0, 1], pa.int8()),
                _array([0, 0], pa.int32()),
                [TIMESTAMPS.slice(0, 1), _array([7])],
                field_names=_UNION_CHILDREN,
            ),
            [TS_US, 7],
        ),
        (
            pa.RunEndEncodedArray.from_arrays(
                _array([2], pa.int32()), TIMESTAMPS.slice(0, 1)
            ),
            [TS_US, TS_US],
        ),
    ],
    ids=["list", "struct", "map", "dictionary", "sparse_union", "dense_union", "ree"],
)
def test_nested_nanosecond_values_are_truncated_to_microseconds(
    column: pa.Array, expected: list[Any]
) -> None:
    backend = ArrowBackend(pa.table({"nested": column, "n": [1, 2]}))

    assert backend.get_column_at(0) == expected
    assert backend.get_row_at(1) == [expected[1], 2]
    assert backend.get_cell_at(0, 0) == expected[0]
    assert backend.column_content_widths[0] > 0


def test_nanosecond_values_are_not_pandas_types() -> None:
    """pyarrow converts nanoseconds through pandas when it can import it."""
    backend = ArrowBackend.from_pandas(
        pa.table({"ts": _ns_array([TS_NS], pa.timestamp("ns"))}).to_pandas()
    )

    assert type(backend.get_cell_at(0, 0)) is datetime
    assert type(backend.get_row_at(0)[0]) is datetime
    assert type(backend.get_column_at(0)[0]) is datetime


def test_updating_a_nanosecond_cell_keeps_the_other_rows() -> None:
    column = _ns_array([TS_NS, TS_NS], pa.timestamp("ns"))
    backend = ArrowBackend(pa.table({"ns": column}))

    backend.update_cell(1, 0, datetime(2020, 1, 1))

    stored = backend.data.column(0)
    assert stored.type == pa.timestamp("ns")
    assert stored.cast(pa.int64()).to_pylist() == [TS_NS, 1_577_836_800_000_000_000]
    assert backend.get_cell_at(1, 0) == datetime(2020, 1, 1)


def test_updating_a_nanosecond_cell_indexes_rows_as_other_columns_do() -> None:
    column = _ns_array([TS_NS, TS_NS], pa.timestamp("ns"))
    backend = ArrowBackend(pa.table({"ns": column}))

    backend.update_cell(-1, 0, datetime(2020, 1, 1))
    assert backend.get_column_at(0) == [TS_US, datetime(2020, 1, 1)]

    with pytest.raises(IndexError):
        backend.update_cell(2, 0, datetime(2020, 1, 1))
    with pytest.raises(IndexError):
        backend.update_cell(-3, 0, datetime(2020, 1, 1))


BEFORE_1970 = datetime(1969, 12, 31, 23, 59, 59, 999999)
EPOCH = datetime(1970, 1, 1)
LAST_NS_TIMESTAMP = datetime(2262, 4, 11, 23, 47, 16, 854775)
"""2**63 - 1 nanoseconds, truncated to microseconds."""


@pytest.mark.parametrize(
    "column,expected",
    [
        (
            _ns_array([-1, MAX_64BIT_INT], pa.timestamp("ns")).dictionary_encode(),
            [BEFORE_1970, datetime.max],
        ),
        (
            _nested(
                [[-1], [MAX_64BIT_INT]],
                pa.list_(pa.int64()),
                pa.list_(pa.timestamp("ns")),
            ),
            [[EPOCH], [LAST_NS_TIMESTAMP]],
        ),
        (
            _nested(
                [{"ts": -1}, {"ts": MAX_64BIT_INT}],
                pa.struct([("ts", pa.int64())]),
                pa.struct([("ts", pa.timestamp("ns"))]),
            ),
            [{"ts": EPOCH}, {"ts": LAST_NS_TIMESTAMP}],
        ),
        (
            pa.UnionArray.from_sparse(
                _array([0, 0], pa.int8()),
                [_ns_array([-1, MAX_64BIT_INT], pa.timestamp("ns"))],
                field_names=["ts"],
            ),
            [EPOCH, LAST_NS_TIMESTAMP],
        ),
    ],
    ids=["dictionary", "list", "struct", "union"],
)
def test_every_getter_converts_a_nanosecond_value_alike(
    column: pa.Array, expected: list[Any]
) -> None:
    """Only a column's own timestamps, or a dictionary's, are floored and infinite."""
    backend = ArrowBackend(pa.table({"c": column}))

    assert backend.get_column_at(0) == expected
    assert [backend.get_row_at(i)[0] for i in range(2)] == expected
    assert [backend.get_cell_at(i, 0) for i in range(2)] == expected


def test_a_row_with_an_overflowing_value_keeps_its_other_values() -> None:
    column = pa.array([MAX_64BIT_INT], type=pa.int64()).cast(pa.timestamp("s"))
    backend = ArrowBackend(pa.table({"ts": column, "n": [5]}))

    assert backend.get_row_at(0) == [datetime.max, 5]


def test_nanosecond_values_without_pandas() -> None:
    """The crash in tconbeer/harlequin#1207, which needs pandas to be missing."""
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
    expected = "2024-01-02 03:04:05.123456"
    assert proc.stdout.split("\n") == ["23", expected, expected, expected, "False", ""]
