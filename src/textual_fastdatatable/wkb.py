"""Well-Known Binary to Well-Known Text, for the `geoarrow.wkb` extension type.

The text is the one duckdb's `ST_AsText` produces, so that a geometry reads the
same in a cell as it does in a file the same query exported;
`test_wkb.py::test_wkt_matches_duckdb` pins that against a corpus duckdb
generated. A coordinate is formatted as Python's `repr`, which is the shortest
string that round-trips, as duckdb's own double-to-text is.

Stdlib only, so that `backend` can render a geometry without a geospatial
dependency.
"""

from __future__ import annotations

import struct

_GEOMETRY_NAMES = {
    1: "POINT",
    2: "LINESTRING",
    3: "POLYGON",
    4: "MULTIPOINT",
    5: "MULTILINESTRING",
    6: "MULTIPOLYGON",
    7: "GEOMETRYCOLLECTION",
}

_DIMENSION_TAGS = {0: "", 1: " Z", 2: " M", 3: " ZM"}
_COORDINATE_COUNTS = {0: 2, 1: 3, 2: 3, 3: 4}

_POINT = 1
_LINESTRING = 2
_POLYGON = 3
_MULTIPOINT = 4
_GEOMETRYCOLLECTION = 7

_WKB_SRID_FLAG = 0x20000000
"""Set in the type code when an EWKB geometry carries an SRID before its body."""

MAX_NESTING_DEPTH = 64
"""How deeply a geometry may nest before it is read as not being one.

A collection holds geometries, so a chain of them recurses once per level and
bytes that nest deeply enough would exhaust the interpreter's stack rather than
raise `WkbError`. No producer emits anything near this -- a collection of
collections is already unusual -- so past it the bytes are treated as the
corrupt or hostile input they are, and shown as a blob like any other.
"""


class WkbError(ValueError):
    """Raised for bytes that are not the WKB geometry they claim to be."""


def _format_coordinate(value: float) -> str:
    """One ordinate, as duckdb spells it."""
    if value != value:
        return "nan"
    if value == float("inf"):
        return "inf"
    if value == float("-inf"):
        return "-inf"
    text = repr(value)
    # repr gives an integral double a trailing ".0"; WKT has no decimal point
    return text[:-2] if text.endswith(".0") else text


class _Reader:
    """A cursor over a WKB buffer. Each read advances it."""

    __slots__ = ("data", "depth", "position")

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0
        self.depth = 0

    def descend(self) -> None:
        self.depth += 1
        if self.depth > MAX_NESTING_DEPTH:
            raise WkbError(f"WKB nests deeper than {MAX_NESTING_DEPTH} geometries")

    def ascend(self) -> None:
        self.depth -= 1

    def byte_order(self) -> str:
        try:
            order = self.data[self.position]
        except IndexError:
            raise WkbError("WKB ended where a byte order was expected") from None
        self.position += 1
        if order == 1:
            return "<"
        if order == 0:
            return ">"
        raise WkbError(f"{order} is not a WKB byte order")

    def uint32(self, order: str) -> int:
        try:
            value: int = struct.unpack_from(f"{order}I", self.data, self.position)[0]
        except struct.error:
            raise WkbError("WKB ended mid-geometry") from None
        self.position += 4
        return value

    def doubles(self, order: str, count: int) -> tuple[float, ...]:
        try:
            values: tuple[float, ...] = struct.unpack_from(
                f"{order}{count}d", self.data, self.position
            )
        except struct.error:
            raise WkbError("WKB ended mid-coordinate") from None
        self.position += 8 * count
        return values


def _read_point(reader: _Reader, order: str, coordinates: int) -> str:
    return " ".join(_format_coordinate(v) for v in reader.doubles(order, coordinates))


def _read_points(reader: _Reader, order: str, coordinates: int) -> str:
    count = reader.uint32(order)
    return ", ".join(_read_point(reader, order, coordinates) for _ in range(count))


def _read_geometry(reader: _Reader) -> str:
    reader.descend()
    try:
        return _read_one_geometry(reader)
    finally:
        reader.ascend()


def _read_one_geometry(reader: _Reader) -> str:
    order = reader.byte_order()
    type_code = reader.uint32(order)
    if type_code & _WKB_SRID_FLAG:
        reader.uint32(order)  # the SRID, which WKT does not spell
    # ISO WKB encodes the dimensions in the thousands digit: 1001 is POINT Z
    iso_code = type_code & 0xFFFF
    geometry_type = iso_code % 1000
    dimensions = iso_code // 1000
    if geometry_type not in _GEOMETRY_NAMES or dimensions not in _DIMENSION_TAGS:
        raise WkbError(f"{type_code} is not a WKB geometry type")
    name = _GEOMETRY_NAMES[geometry_type] + _DIMENSION_TAGS[dimensions]
    coordinates = _COORDINATE_COUNTS[dimensions]

    if geometry_type == _POINT:
        point = reader.doubles(order, coordinates)
        # an empty point has nowhere to put its emptiness but its ordinates
        if all(ordinate != ordinate for ordinate in point):
            return f"{name} EMPTY"
        return f"{name} ({' '.join(_format_coordinate(v) for v in point)})"

    if geometry_type == _LINESTRING:
        points = _read_points(reader, order, coordinates)
        return f"{name} ({points})" if points else f"{name} EMPTY"

    if geometry_type == _POLYGON:
        ring_count = reader.uint32(order)
        if not ring_count:
            return f"{name} EMPTY"
        rings = [
            f"({_read_points(reader, order, coordinates)})" for _ in range(ring_count)
        ]
        return f"{name} ({', '.join(rings)})"

    part_count = reader.uint32(order)
    if not part_count:
        return f"{name} EMPTY"
    parts = [_read_geometry(reader) for _ in range(part_count)]
    if geometry_type != _GEOMETRYCOLLECTION:
        # a multi-geometry's parts are written bare, without their own tag, and a
        # multipoint's without parentheses either
        parts = [
            _untag(part, bare_point=geometry_type == _MULTIPOINT) for part in parts
        ]
    return f"{name} ({', '.join(parts)})"


def _untag(geometry: str, bare_point: bool) -> str:
    """One part of a multi-geometry, as WKT writes it inside its parent."""
    body = geometry.split(" (", 1)
    if len(body) == 1:
        return "EMPTY"  # the part is empty, and wrote its tag and nothing else
    return body[1][:-1] if bare_point else f"({body[1]}"


def wkb_to_wkt(data: bytes) -> str:
    """The WKT for one WKB geometry.

    Raises `WkbError` for bytes that do not decode as one, which is what makes a
    column that is tagged as geometry but is not renderable as its bytes instead.
    """
    reader = _Reader(data)
    wkt = _read_geometry(reader)
    if reader.position != len(data):
        raise WkbError(
            f"{len(data) - reader.position} bytes left over after the WKB geometry"
        )
    return wkt
