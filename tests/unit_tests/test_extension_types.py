from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from textual_fastdatatable.backend import ArrowBackend
from textual_fastdatatable.extension_types import (
    EXTENSION_METADATA_KEY,
    EXTENSION_NAME_KEY,
    GeoArrowWkbType,
    canonicalize,
)

WKB_POINT = bytes.fromhex("010100000068d0d03fc17b5dc00e15e3fc4d2c4140")
WKT_POINT = "POINT (-117.93367 34.34613)"
WKB_LINESTRING = bytes.fromhex(
    "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
)
WKT_LINESTRING = "LINESTRING (0 0, 1 1)"

GEOARROW_WKB = {EXTENSION_NAME_KEY: b"geoarrow.wkb", EXTENSION_METADATA_KEY: b"{}"}


def geometry_field(name: str = "geom", storage: pa.DataType | None = None) -> pa.Field:
    """A field tagged the way duckdb tags a GEOMETRY column."""
    return pa.field(name, storage or pa.binary(), metadata=GEOARROW_WKB)


def geometry_table(*values: bytes | None) -> pa.Table:
    column = pa.array(list(values), type=pa.binary())
    return pa.Table.from_arrays([column], schema=pa.schema([geometry_field()]))


def test_a_tagged_field_becomes_an_extension_type() -> None:
    canonicalized = canonicalize(geometry_table(WKB_POINT))
    (field,) = canonicalized.schema

    assert isinstance(field.type, GeoArrowWkbType)
    assert field.type.extension_name == "geoarrow.wkb"
    # the two keys are the type now, and pyarrow writes them back out from it
    assert field.metadata is None


def test_a_geometry_converts_to_the_text_it_means() -> None:
    canonicalized = canonicalize(geometry_table(WKB_POINT, WKB_LINESTRING, None))

    assert canonicalized.column(0).to_pylist() == [WKT_POINT, WKT_LINESTRING, None]


def test_a_table_with_nothing_tagged_is_returned_unchanged() -> None:
    """Identity, so that the ordinary table pays nothing for this."""
    data = pa.table({"a": [1, 2], "b": ["x", "y"]})

    assert canonicalize(data) is data


def test_a_field_already_read_as_an_extension_type_is_left_alone() -> None:
    """What an extension type's values are is that type's class's to say."""
    storage = pa.array([uuid.UUID(int=1).bytes], type=pa.binary(16))
    assert isinstance(storage, pa.Array)
    data = pa.table({"u": pa.ExtensionArray.from_storage(pa.uuid(), storage)})

    assert canonicalize(data) is data


def test_a_name_with_no_class_here_is_left_as_its_storage() -> None:
    field = pa.field(
        "v", pa.binary(), metadata={EXTENSION_NAME_KEY: b"some.vendor.type"}
    )
    data = pa.Table.from_arrays(
        [pa.array([b"\x00"], type=pa.binary())], schema=pa.schema([field])
    )

    assert canonicalize(data) is data


def test_a_storage_type_the_name_does_not_describe_is_declined() -> None:
    """WKB is binary; a tag over anything else is not a geometry this can read."""
    data = pa.Table.from_arrays(
        [pa.array(["POINT (0 0)"], type=pa.string())],
        schema=pa.schema([geometry_field(storage=pa.string())]),
    )

    assert canonicalize(data) is data


@pytest.mark.parametrize(
    "storage", [pa.binary(), pa.large_binary(), pa.binary_view()], ids=str
)
def test_every_storage_type_wkb_is_stored_as_is_read(storage: pa.DataType) -> None:
    data = pa.Table.from_arrays(
        [pa.array([WKB_POINT], type=storage)],
        schema=pa.schema([geometry_field(storage=storage)]),
    )

    assert canonicalize(data).column(0).to_pylist() == [WKT_POINT]


def test_the_extension_metadata_the_field_declared_survives() -> None:
    """It carries the CRS, which a consumer downstream of this may need."""
    crs = b'{"crs":"OGC:CRS84"}'
    field = pa.field(
        "geom",
        pa.binary(),
        metadata={EXTENSION_NAME_KEY: b"geoarrow.wkb", EXTENSION_METADATA_KEY: crs},
    )
    data = pa.Table.from_arrays(
        [pa.array([WKB_POINT], type=pa.binary())], schema=pa.schema([field])
    )

    (canonicalized,) = canonicalize(data).schema
    assert isinstance(canonicalized.type, GeoArrowWkbType)
    assert canonicalized.type.__arrow_ext_serialize__() == crs


def test_metadata_besides_the_extension_keys_is_kept() -> None:
    field = pa.field(
        "geom",
        pa.binary(),
        metadata={**GEOARROW_WKB, b"comment": b"where the thing is"},
    )
    data = pa.Table.from_arrays(
        [pa.array([WKB_POINT], type=pa.binary())], schema=pa.schema([field])
    )

    (canonicalized,) = canonicalize(data).schema
    assert canonicalized.metadata == {b"comment": b"where the thing is"}


def test_the_schemas_own_metadata_is_kept() -> None:
    data = pa.Table.from_arrays(
        [pa.array([WKB_POINT], type=pa.binary())],
        schema=pa.schema([geometry_field()], metadata={b"geo": b"{}"}),
    )

    assert canonicalize(data).schema.metadata == {b"geo": b"{}"}


@pytest.mark.parametrize(
    ("column_type", "value", "expected"),
    [
        pytest.param(
            pa.list_(geometry_field("l")),
            [WKB_POINT, None],
            [WKT_POINT, None],
            id="list",
        ),
        pytest.param(
            pa.large_list(geometry_field("l")),
            [WKB_POINT],
            [WKT_POINT],
            id="large list",
        ),
        pytest.param(
            pa.list_(geometry_field("l"), 1), [WKB_POINT], [WKT_POINT], id="fixed list"
        ),
        pytest.param(
            pa.struct([geometry_field("g")]),
            {"g": WKB_POINT},
            {"g": WKT_POINT},
            id="struct",
        ),
        pytest.param(
            pa.map_(pa.string(), geometry_field("g")),
            [("here", WKB_POINT)],
            [("here", WKT_POINT)],
            id="map",
        ),
        pytest.param(
            pa.list_(pa.field("l", pa.struct([geometry_field("g")]))),
            [{"g": WKB_POINT}],
            [{"g": WKT_POINT}],
            id="list of struct",
        ),
    ],
)
def test_a_nested_geometry_is_converted_too(
    column_type: pa.DataType, value: object, expected: object
) -> None:
    """duckdb tags the geometry inside a LIST or a STRUCT, not the column."""
    column = pa.array([value], type=column_type)
    assert isinstance(column, pa.Array)
    # from_arrays with the schema, since pa.array() drops a map's item metadata
    data = pa.Table.from_arrays(
        [column], schema=pa.schema([pa.field("nested", column_type)])
    )

    assert canonicalize(data).column(0).to_pylist() == [expected]


def test_the_backend_shows_a_geometry_as_text_and_keeps_the_callers_bytes() -> None:
    backend = ArrowBackend(geometry_table(WKB_POINT, WKB_LINESTRING, None))

    assert backend.get_cell_at(0, 0) == WKT_POINT
    assert backend.get_row_at(1) == [WKT_LINESTRING]
    assert backend.get_column_at(0) == [WKT_POINT, WKT_LINESTRING, None]
    # the caller handed over WKB, and source_data is the caller's table
    assert backend.source_data.column(0).to_pylist() == [
        WKB_POINT,
        WKB_LINESTRING,
        None,
    ]
    assert backend.source_data.schema.field(0).metadata == GEOARROW_WKB


def test_a_geometry_column_is_as_wide_as_its_widest_text() -> None:
    backend = ArrowBackend(geometry_table(WKB_POINT, WKB_LINESTRING, None))

    assert backend.column_content_widths == [len(WKT_POINT)]


def test_a_column_of_only_nulls_is_measured_as_the_null_rep() -> None:
    backend = ArrowBackend(geometry_table(None, None))

    assert backend.column_content_widths == [0]


def test_bytes_tagged_as_a_geometry_that_are_not_one_stay_bytes() -> None:
    """A cell shows what it can; a mis-tagged column renders as the blob it is."""
    backend = ArrowBackend(geometry_table(b"\xff\xff\xff\xff"))

    assert backend.get_cell_at(0, 0) == b"\xff\xff\xff\xff"


def test_a_geometry_column_sorts_by_the_bytes_it_stores() -> None:
    """Arrow sorts no extension type, so the storage supplies the order."""
    backend = ArrowBackend(geometry_table(WKB_LINESTRING, WKB_POINT))

    backend.sort("geom")
    ascending = backend.get_column_at(0)
    backend.sort([("geom", "descending")])

    assert ascending == [WKT_POINT, WKT_LINESTRING]
    assert backend.get_column_at(0) == [WKT_LINESTRING, WKT_POINT]


def test_sorting_by_another_column_moves_the_geometry_with_it() -> None:
    data = pa.Table.from_arrays(
        [
            pa.array([WKB_POINT, WKB_LINESTRING], type=pa.binary()),
            pa.array([2, 1], type=pa.int64()),
        ],
        schema=pa.schema([geometry_field(), pa.field("n", pa.int64())]),
    )
    backend = ArrowBackend(data)

    backend.sort("n")

    assert backend.get_column_at(0) == [WKT_LINESTRING, WKT_POINT]
    assert backend.get_column_at(1) == [1, 2]


def test_a_uuid_column_sorts_by_the_bytes_it_stores() -> None:
    """Every extension type gets the same order, not only the ones defined here."""
    storage = pa.array(
        [uuid.UUID(int=2).bytes, uuid.UUID(int=1).bytes], type=pa.binary(16)
    )
    assert isinstance(storage, pa.Array)
    data = pa.table({"u": pa.ExtensionArray.from_storage(pa.uuid(), storage)})
    backend = ArrowBackend(data)

    backend.sort("u")

    assert backend.get_column_at(0) == [uuid.UUID(int=1), uuid.UUID(int=2)]
