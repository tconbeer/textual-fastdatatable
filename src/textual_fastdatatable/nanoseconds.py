"""Python values for Arrow's nanosecond temporal types.

A `datetime`, `time` or `timedelta` stops at microseconds, and pyarrow converts a
nanosecond value through pandas when it can import it and raises when it cannot.
`nanosecond_values()` converts them the same way either way, into subclasses that
carry the rest, so a cell renders as any other temporal value does while the
value's text keeps every digit.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any, cast

import pyarrow as pa
import pyarrow.types as pt

_INT64_MAX = 2**63 - 1
"""duckdb's `'infinity'::timestamp_ns`; its `'-infinity'` is the negation."""


class _Nanoseconds:
    """The nanoseconds past a value's microsecond, kept through repr and pickling."""

    nanosecond: int = 0

    def __repr__(self) -> str:
        return f"{super().__repr__()[:-1]}, nanosecond={self.nanosecond})"

    def __reduce_ex__(self, protocol: Any) -> tuple[Any, ...]:
        # the base pickles only the fields it has
        reduced = cast("tuple[Any, ...]", super().__reduce_ex__(protocol))
        constructor, args = reduced[:2]
        return (constructor, args, {"nanosecond": self.nanosecond})


class NanosecondDatetime(_Nanoseconds, datetime):
    """A `datetime` with the nanoseconds past its microsecond."""

    @classmethod
    def from_datetime(cls, value: datetime, nanosecond: int) -> NanosecondDatetime:
        instance = cls(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.tzinfo,
            fold=value.fold,
        )
        instance.nanosecond = nanosecond
        return instance

    def isoformat(self, sep: str = "T", timespec: str = "auto") -> str:
        if timespec != "auto" or not self.nanosecond:
            return super().isoformat(sep, timespec)
        text = super().isoformat(sep, "microseconds")
        # the fraction ends 26 characters in, ahead of any UTC offset
        return f"{text[:26]}{self.nanosecond:03d}{text[26:]}"

    def __str__(self) -> str:
        return self.isoformat(sep=" ")


class NanosecondTime(_Nanoseconds, time):
    """A `time` with the nanoseconds past its microsecond."""

    @classmethod
    def from_time(cls, value: time, nanosecond: int) -> NanosecondTime:
        instance = cls(
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.tzinfo,
            fold=value.fold,
        )
        instance.nanosecond = nanosecond
        return instance

    def isoformat(self, timespec: str = "auto") -> str:
        if timespec != "auto" or not self.nanosecond:
            return super().isoformat(timespec)
        text = super().isoformat("microseconds")
        # the fraction ends 15 characters in, ahead of any UTC offset
        return f"{text[:15]}{self.nanosecond:03d}{text[15:]}"

    def __str__(self) -> str:
        return self.isoformat()


class NanosecondTimedelta(_Nanoseconds, timedelta):
    """A `timedelta` with the nanoseconds past its microseconds."""

    @classmethod
    def from_timedelta(cls, value: timedelta, nanosecond: int) -> NanosecondTimedelta:
        instance = cls(value.days, value.seconds, value.microseconds)
        instance.nanosecond = nanosecond
        return instance

    def __str__(self) -> str:
        text = super().__str__()
        if not self.nanosecond:
            return text
        if not self.microseconds:
            text = f"{text}.000000"
        return f"{text}{self.nanosecond:03d}"


def is_nanosecond_temporal(data_type: pa.DataType) -> bool:
    """Whether pyarrow converts this type's values through pandas."""
    if pt.is_timestamp(data_type) or pt.is_time64(data_type):
        return data_type.unit == "ns"
    if pt.is_duration(data_type):
        return data_type.unit == "ns"
    return False


def holds_nanoseconds(data_type: pa.DataType) -> bool:
    """Whether this type is, or nests, a nanosecond temporal type."""
    if is_nanosecond_temporal(data_type):
        return True
    if (
        pt.is_list(data_type)
        or pt.is_large_list(data_type)
        or pt.is_fixed_size_list(data_type)
    ):
        return holds_nanoseconds(data_type.value_field.type)
    if pt.is_struct(data_type) or pt.is_map(data_type):
        # a map's one child is its entries struct, of a key and an item
        return any(
            holds_nanoseconds(data_type.field(i).type)
            for i in range(data_type.num_fields)
        )
    return False


def nanosecond_values(
    nanoseconds: list[int | None], data_type: pa.DataType
) -> list[Any]:
    """The Python values of a nanosecond temporal type, from the integers it stores.

    A value with no nanoseconds past its microsecond is the base type itself, as
    pyarrow converts it.
    """
    whole_type: pa.DataType
    if pt.is_timestamp(data_type):
        whole_type = pa.timestamp("us", tz=data_type.tz)
    elif pt.is_time64(data_type):
        whole_type = pa.time64("us")
    else:
        whole_type = pa.duration("us")
    # floored, so that the nanoseconds past a microsecond are never negative; and
    # pyarrow converts the microseconds, and with them any time zone
    microseconds = [None if value is None else value // 1_000 for value in nanoseconds]
    wholes = pa.array(microseconds, pa.int64()).cast(whole_type).to_pylist()

    values: list[Any] = []
    for stored, whole in zip(nanoseconds, wholes, strict=True):
        nanosecond = 0 if stored is None else stored % 1_000
        if (
            stored is not None
            and pt.is_timestamp(data_type)
            and abs(stored) >= _INT64_MAX
        ):
            values.append(datetime.max if stored > 0 else datetime.min)
        elif not nanosecond:
            values.append(whole)
        elif isinstance(whole, datetime):
            values.append(NanosecondDatetime.from_datetime(whole, nanosecond))
        elif isinstance(whole, time):
            values.append(NanosecondTime.from_time(whole, nanosecond))
        else:
            values.append(NanosecondTimedelta.from_timedelta(whole, nanosecond))
    return values
