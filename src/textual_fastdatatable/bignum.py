"""duckdb's BIGNUM encoding to Python integers.

duckdb hands a BIGNUM (called VARINT before 1.4) to Arrow as an `arrow.opaque`
over binary storage, or before 1.2 as binary tagged `duckdb.varint`. The storage
holds a three-byte big-endian header -- its top bit set for a value that is not
negative, the other 23 the count of bytes that follow -- and then the value's
magnitude, big-endian. A negative value has every bit of both inverted, which is
what makes the bytes sort in the value's order. A value converts to the integer
whose text is duckdb's own cast to VARCHAR, which
`test_bignum.py::test_values_match_duckdb` pins against a corpus duckdb generated.

Stdlib only, so that `backend` can render a BIGNUM without another dependency.
"""

from __future__ import annotations

import sys
from decimal import Decimal

_HEADER_SIZE = 3
_NOT_NEGATIVE = 0x800000
_HEADER_BITS = 0xFFFFFF


class BignumError(ValueError):
    """Raised for bytes that are not the BIGNUM they claim to be."""


def _int_text_limit() -> int:
    """The most digits this Python converts an int to text with, or 0 for any.

    A setting, read on every call, since a program can change it."""
    get_limit = getattr(sys, "get_int_max_str_digits", None)
    return 0 if get_limit is None else int(get_limit())


def bignum_to_python(data: bytes) -> int | Decimal:
    """The integer one BIGNUM holds.

    An int, or a Decimal of it when it has more digits than this Python converts
    an int to text with, since a cell could not render that int. Raises
    `BignumError` for bytes that do not decode as one, so that a cell tagged as a
    BIGNUM that is not one can show its bytes instead.
    """
    if len(data) <= _HEADER_SIZE:
        raise BignumError("BIGNUM ended before its first byte of value")
    header = int.from_bytes(data[:_HEADER_SIZE], "big")
    negative = not header & _NOT_NEGATIVE
    if negative:
        header ^= _HEADER_BITS
    size = header & ~_NOT_NEGATIVE
    if size != len(data) - _HEADER_SIZE:
        raise BignumError(
            f"BIGNUM declares {size} bytes of value and holds "
            f"{len(data) - _HEADER_SIZE}"
        )
    magnitude = int.from_bytes(data[_HEADER_SIZE:], "big")
    if negative:
        magnitude ^= (1 << (8 * size)) - 1

    value = -magnitude if negative else magnitude
    limit = _int_text_limit()
    # at most this many digits, since log10(2) < 0.30103
    if limit and magnitude.bit_length() * 30103 // 100000 + 1 > limit:
        return Decimal(value)
    return value
