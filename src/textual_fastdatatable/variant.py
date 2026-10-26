"""Parquet Variant binary to Python values, for the `arrow.parquet.variant` type.

A variant is two buffers: metadata holding the keys of every object in the value,
and the value, which refers to those keys by position. The encoding is Parquet's
(`VariantEncoding.md` in apache/parquet-format). A value converts to the one
duckdb's Python client gives for the same VARIANT, which
`test_variant.py::test_values_match_duckdb` pins against a corpus duckdb
generated.

Stdlib only, so that `backend` can render a variant without another dependency.
"""

from __future__ import annotations

import struct
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from itertools import pairwise

_PRIMITIVE = 0
_SHORT_STRING = 1
_OBJECT = 2

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

_INTEGER_SIZES = {3: 1, 4: 2, 5: 4, 6: 8}
_DECIMAL_SIZES = {8: 4, 9: 8, 10: 16}
_FLOAT_FORMATS = {7: "<d", 14: "<f"}

MAX_NESTING_DEPTH = 128
"""How deeply a value may nest before it is read as not being a variant.

Objects and arrays recurse once per level, so bytes that nest deeply enough would
exhaust the interpreter's stack rather than raise `VariantError`.
"""


class VariantError(ValueError):
    """Raised for bytes that are not the variant they claim to be."""


class _Reader:
    """One value buffer and the keys its objects name."""

    __slots__ = ("budget", "data", "depth", "keys")

    def __init__(self, data: bytes, keys: list[str]) -> None:
        self.data = data
        self.keys = keys
        self.depth = 0
        # every value a well-formed buffer holds takes at least one byte of its
        # own, so a buffer whose offsets share values cannot decode to more values
        # than it has bytes, however they nest
        self.budget = len(data)

    def uint(self, position: int, size: int) -> int:
        end = position + size
        if position < 0 or end > len(self.data):
            raise VariantError("variant ended mid-value")
        return int.from_bytes(self.data[position:end], "little")

    def signed(self, position: int, size: int) -> int:
        value = self.uint(position, size)
        return value - (1 << (8 * size)) if value >> (8 * size - 1) else value

    def chunk(self, position: int, size: int) -> bytes:
        if size < 0 or position + size > len(self.data):
            raise VariantError("variant ended mid-value")
        return self.data[position : position + size]

    def text(self, position: int, size: int) -> str:
        try:
            return self.chunk(position, size).decode()
        except UnicodeDecodeError as e:
            raise VariantError("variant string is not UTF-8") from e

    def value(self, position: int) -> object:
        self.depth += 1
        self.budget -= 1
        if self.depth > MAX_NESTING_DEPTH:
            raise VariantError(f"variant nests deeper than {MAX_NESTING_DEPTH} values")
        if self.budget < 0:
            raise VariantError("variant holds more values than it has bytes")
        try:
            return self._value(position)
        finally:
            self.depth -= 1

    def _value(self, position: int) -> object:
        header = self.uint(position, 1)
        basic_type, value_header = header & 0x03, header >> 2
        body = position + 1
        if basic_type == _PRIMITIVE:
            return self._primitive(body, value_header)
        if basic_type == _SHORT_STRING:
            return self.text(body, value_header)
        if basic_type == _OBJECT:
            offset_size = (value_header & 0x03) + 1
            id_size = ((value_header >> 2) & 0x03) + 1
            count_size = 4 if value_header & 0x10 else 1
            count = self.uint(body, count_size)
            ids_start = body + count_size
            offsets_start = ids_start + count * id_size
            values_start = offsets_start + (count + 1) * offset_size
            fields: list[tuple[str, object]] = []
            for i in range(count):
                key_id = self.uint(ids_start + i * id_size, id_size)
                if key_id >= len(self.keys):
                    raise VariantError(f"{key_id} is not a key in the variant")
                offset = self.uint(offsets_start + i * offset_size, offset_size)
                fields.append((self.keys[key_id], self.value(values_start + offset)))
            # by name, as the encoding orders them and duckdb's client returns them
            return dict(sorted(fields, key=lambda field: field[0]))
        offset_size = (value_header & 0x03) + 1
        count_size = 4 if value_header & 0x04 else 1
        count = self.uint(body, count_size)
        offsets_start = body + count_size
        values_start = offsets_start + (count + 1) * offset_size
        return [
            self.value(
                values_start + self.uint(offsets_start + i * offset_size, offset_size)
            )
            for i in range(count)
        ]

    def _primitive(self, body: int, type_id: int) -> object:
        if type_id == 0:
            return None
        if type_id in (1, 2):
            return type_id == 1
        if type_id in _INTEGER_SIZES:
            return self.signed(body, _INTEGER_SIZES[type_id])
        if type_id in _FLOAT_FORMATS:
            float_format = _FLOAT_FORMATS[type_id]
            data = self.chunk(body, struct.calcsize(float_format))
            float_value: float = struct.unpack(float_format, data)[0]
            return float_value
        if type_id in _DECIMAL_SIZES:
            scale = self.uint(body, 1)
            unscaled = self.signed(body + 1, _DECIMAL_SIZES[type_id])
            # from text, which is exact, where arithmetic rounds to the context's
            # precision, and DECIMAL(38) has more digits than the default context
            return Decimal(f"{unscaled}e-{scale}")
        if type_id == 11:
            return _clamped(_EPOCH_DATE, days=self.signed(body, 4))
        if type_id in (12, 13):
            micros = self.signed(body, 8)
            epoch = _EPOCH_UTC if type_id == 12 else _EPOCH
            return _clamped(epoch, microseconds=micros)
        if type_id in (18, 19):
            micros = self.signed(body, 8) // 1000
            epoch = _EPOCH_UTC if type_id == 18 else _EPOCH
            return _clamped(epoch, microseconds=micros)
        if type_id == 17:
            micros = self.signed(body, 8)
            if not 0 <= micros < 86_400_000_000:
                raise VariantError(f"{micros} microseconds is not a time of day")
            return (_EPOCH + timedelta(microseconds=micros)).time()
        if type_id in (15, 16):
            size = self.uint(body, 4)
            if type_id == 16:
                return self.text(body + 4, size)
            return self.chunk(body + 4, size)
        if type_id == 20:
            return uuid.UUID(bytes=self.chunk(body, 16))
        raise VariantError(f"{type_id} is not a variant primitive type")


def _clamped(epoch: date, days: int = 0, microseconds: int = 0) -> date:
    """`epoch` moved by the offset, or the end of the range it would pass.

    The end is naive, as the overflowed temporals elsewhere in this package are,
    so that it renders as infinity.
    """
    try:
        return epoch + timedelta(days=days, microseconds=microseconds)
    except OverflowError:
        kind = datetime if isinstance(epoch, datetime) else date
        return kind.max if days > 0 or microseconds > 0 else kind.min


def _keys(metadata: bytes) -> list[str]:
    """The keys a variant's metadata holds, in the order its objects number them."""
    if not metadata:
        raise VariantError("variant metadata is empty")
    header = metadata[0]
    if header & 0x0F != 1:
        raise VariantError(f"{header & 0x0F} is not a variant metadata version")
    offset_size = (header >> 6) + 1
    reader = _Reader(metadata, [])
    count = reader.uint(1, offset_size)
    offsets_start = 1 + offset_size
    strings_start = offsets_start + (count + 1) * offset_size
    offsets = [
        reader.uint(offsets_start + i * offset_size, offset_size)
        for i in range(count + 1)
    ]
    return [
        reader.text(strings_start + start, end - start)
        for start, end in pairwise(offsets)
    ]


def variant_to_python(metadata: bytes, value: bytes) -> object:
    """The Python value one variant holds.

    Raises `VariantError` for bytes that do not decode as one, which is what makes
    a column that is tagged as a variant but is not renderable as its storage.
    """
    return _Reader(value, _keys(metadata)).value(0)
