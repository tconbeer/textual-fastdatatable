from __future__ import annotations

import copy
import pickle
from datetime import datetime, time, timedelta, timezone
from typing import Any

import pyarrow as pa
import pytest

from textual_fastdatatable.format import display_text
from textual_fastdatatable.nanoseconds import (
    NanosecondDatetime,
    NanosecondTime,
    NanosecondTimedelta,
    holds_nanoseconds,
    nanosecond_values,
)

OFFSET = timezone(timedelta(hours=-5))

VALUES: list[Any] = [
    NanosecondDatetime.from_datetime(datetime(2024, 1, 2, 3, 4, 5, 123456), 789),
    NanosecondDatetime.from_datetime(
        datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=OFFSET), 789
    ),
    NanosecondTime.from_time(time(3, 4, 5, 123456), 789),
    NanosecondTimedelta.from_timedelta(timedelta(microseconds=123456), 789),
    NanosecondTimedelta.from_timedelta(timedelta(seconds=1), 1),
]


@pytest.mark.parametrize(
    "value,expected",
    zip(
        VALUES,
        [
            "2024-01-02 03:04:05.123456789",
            "2024-01-02 03:04:05.123456789-05:00",
            "03:04:05.123456789",
            "0:00:00.123456789",
            "0:00:01.000000001",
        ],
        strict=True,
    ),
)
def test_text_keeps_every_digit(value: Any, expected: str) -> None:
    assert str(value) == expected


def test_isoformat_keeps_every_digit_unless_told_otherwise() -> None:
    value = VALUES[1]
    assert value.isoformat() == "2024-01-02T03:04:05.123456789-05:00"
    assert value.isoformat(timespec="microseconds") == (
        "2024-01-02T03:04:05.123456-05:00"
    )
    assert VALUES[2].isoformat(timespec="seconds") == "03:04:05"


@pytest.mark.parametrize(
    "value,expected",
    zip(
        VALUES,
        [
            "2024-01-02T03:04:05.123",
            "2024-01-02T03:04:05.123-05:00",
            "03:04:05.123",
            "0:00:00.123456789",
            "0:00:01.000000001",
        ],
        strict=True,
    ),
)
def test_a_cell_renders_as_its_base_type_does(value: Any, expected: str) -> None:
    assert display_text(value) == expected


@pytest.mark.parametrize("value", VALUES)
def test_copies_and_pickles_keep_the_nanoseconds(value: Any) -> None:
    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        unpickled = pickle.loads(pickle.dumps(value, protocol=protocol))
        assert type(unpickled) is type(value)
        assert str(unpickled) == str(value)
    assert str(copy.deepcopy(value)) == str(value)


def test_repr_shows_the_nanoseconds() -> None:
    assert repr(VALUES[2]) == "NanosecondTime(3, 4, 5, 123456, nanosecond=789)"


@pytest.mark.parametrize(
    "data_type,expected",
    [
        (pa.timestamp("ns"), True),
        (pa.timestamp("us"), False),
        (pa.time64("ns"), True),
        (pa.duration("ns"), True),
        (pa.duration("ms"), False),
        (pa.list_(pa.timestamp("ns")), True),
        (pa.list_(pa.timestamp("ns"), 2), True),
        (pa.large_list(pa.int64()), False),
        (pa.struct([("a", pa.int64()), ("b", pa.time64("ns"))]), True),
        (pa.map_(pa.string(), pa.duration("ns")), True),
        (pa.map_(pa.string(), pa.int64()), False),
    ],
)
def test_holds_nanoseconds(data_type: pa.DataType, expected: bool) -> None:
    assert holds_nanoseconds(data_type) is expected


def test_values_without_nanoseconds_are_the_base_type() -> None:
    values = nanosecond_values([1_000, None, 1_001], pa.duration("ns"))
    assert values == [timedelta(microseconds=1), None, timedelta(microseconds=1)]
    assert type(values[0]) is timedelta
    assert isinstance(values[2], NanosecondTimedelta)
    assert values[2].nanosecond == 1
