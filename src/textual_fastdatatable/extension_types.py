"""Arrow extension types this package gives a class of its own.

An extension type is a storage type with a meaning attached, and only the type
says which of the two a cell shows. pyarrow materializes the canonical ones
(`arrow.uuid`, `arrow.json`, …) as types of their own, but a type it has no class
for arrives as its bare storage with `ARROW:extension:name` in the field's
metadata -- which is how a geometry column reaches a table, as WKB tagged
`geoarrow.wkb`, from duckdb's spatial extension and from GeoArrow producers
generally, and a VARIANT, as the two binary buffers of `arrow.parquet.variant`.
duckdb's BIGNUM arrives as `arrow.opaque`, which pyarrow does have a class for,
but one that only names the vendor's type and leaves the value as its storage.
Their bytes are not what a cell shows.

`canonicalize()` gives such a field one of the types below, whose scalars convert
to the text the value means, so that the rest of this package measures and
renders a geometry through the extension-type path it already has for
`arrow.uuid` rather than as the binary preview a blob gets. A name with no class
here is left as its storage, which is what pyarrow does with it too.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Callable, Iterable, cast

import pyarrow as pa
import pyarrow.types as pt

from textual_fastdatatable.bignum import BignumError, bignum_to_python
from textual_fastdatatable.variant import VariantError, variant_to_python
from textual_fastdatatable.wkb import WkbError, wkb_to_wkt

EXTENSION_NAME_KEY = b"ARROW:extension:name"
EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"
OPAQUE_NAME = b"arrow.opaque"

_NO_EXTENSION_METADATA = b"{}"


class GeoArrowWkbScalar(pa.ExtensionScalar):
    """A geometry, whose Python value is the WKT a cell shows."""

    def as_py(self, **kwargs: Any) -> str | bytes | None:
        storage = self.value
        if storage is None:
            return None
        data: bytes = storage.as_py()
        try:
            return wkb_to_wkt(data)
        except WkbError:
            # tagged a geometry but not one: the bytes are all it is known to be,
            # and render as any other blob does
            return data


class GeoArrowWkbType(pa.ExtensionType):
    """`geoarrow.wkb`: a geometry, stored as Well-Known Binary."""

    def __init__(
        self, storage_type: pa.DataType, serialized: bytes = _NO_EXTENSION_METADATA
    ) -> None:
        # held so that the CRS and edge type the field declared survive a
        # round trip through this type
        self._serialized = serialized
        super().__init__(storage_type, "geoarrow.wkb")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._serialized

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "GeoArrowWkbType":
        return cls(storage_type, serialized)

    def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
        return GeoArrowWkbScalar


def _geoarrow_wkb(storage_type: pa.DataType, serialized: bytes) -> pa.DataType | None:
    if not _is_binary(storage_type):
        return None
    return GeoArrowWkbType(storage_type, serialized)


class ParquetVariantScalar(pa.ExtensionScalar):
    """A variant, whose Python value is the one it holds."""

    def as_py(self, **kwargs: Any) -> object:
        storage = self.value
        if storage is None:
            return None
        buffers: dict[str, bytes | None] = storage.as_py()
        return _variant_or_buffers(buffers["metadata"], buffers["value"])


class ParquetVariantArray(pa.ExtensionArray):
    """A column of variants, decoded from its buffers in one pass."""

    def to_pylist(self, **kwargs: Any) -> list[object]:
        # a scalar per row costs more than decoding it does
        storage = cast(pa.StructArray, self.storage)
        metadata = storage.field("metadata").to_pylist()
        values = storage.field("value").to_pylist()
        present = storage.is_valid().to_pylist()
        return [
            _variant_or_buffers(row_metadata, row_value) if row_present else None
            for row_present, row_metadata, row_value in zip(
                present, metadata, values, strict=True
            )
        ]


def _variant_or_buffers(metadata: bytes | None, value: bytes | None) -> object:
    if metadata is None or value is None:
        return None
    try:
        return variant_to_python(metadata, value)
    except VariantError:
        # tagged a variant but not one: the buffers are all it is known to be
        return {"metadata": metadata, "value": value}


class ParquetVariantType(pa.ExtensionType):
    """`arrow.parquet.variant`: a value of any type, in Parquet's Variant encoding."""

    values_share_a_type = False
    """Read by the backend's measuring, which may measure one value for all."""

    def __init__(self, storage_type: pa.DataType, serialized: bytes = b"") -> None:
        self._serialized = serialized
        super().__init__(storage_type, "arrow.parquet.variant")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._serialized

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "ParquetVariantType":
        return cls(storage_type, serialized)

    def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
        return ParquetVariantScalar

    def __arrow_ext_class__(self) -> type[pa.ExtensionArray]:
        return ParquetVariantArray


class DuckDBBignumScalar(pa.ExtensionScalar):
    """A BIGNUM, whose Python value is the integer it holds."""

    def as_py(self, **kwargs: Any) -> int | Decimal | bytes | None:
        storage = self.value
        if storage is None:
            return None
        return _bignum_or_bytes(storage.as_py())


class DuckDBBignumArray(pa.ExtensionArray):
    """A column of BIGNUMs, decoded from its storage in one pass."""

    def to_pylist(self, **kwargs: Any) -> list[int | Decimal | bytes | None]:
        # a scalar per row costs more than decoding it does
        return [
            None if data is None else _bignum_or_bytes(data)
            for data in self.storage.to_pylist()
        ]


def _bignum_or_bytes(data: bytes) -> int | Decimal | bytes:
    try:
        return bignum_to_python(data)
    except BignumError:
        # tagged a BIGNUM but not one: the bytes are all it is known to be
        return data


_DUCKDB_BIGNUM_OPAQUE = b'{"type_name":"bignum","vendor_name":"DuckDB"}'
_DUCKDB_VARINT_OPAQUE = b'{"type_name":"varint","vendor_name":"DuckDB"}'


class DuckDBBignumType(pa.ExtensionType):
    """duckdb's BIGNUM, an integer of any size, in duckdb's own encoding."""

    def __init__(
        self, storage_type: pa.DataType, serialized: bytes = _DUCKDB_BIGNUM_OPAQUE
    ) -> None:
        # held so that the type is written back out as the opaque type it was read
        # from, which names duckdb's type
        self._serialized = serialized
        super().__init__(storage_type, OPAQUE_NAME.decode())

    def __arrow_ext_serialize__(self) -> bytes:
        return self._serialized

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "DuckDBBignumType":
        return cls(storage_type, serialized)

    def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
        return DuckDBBignumScalar

    def __arrow_ext_class__(self) -> type[pa.ExtensionArray]:
        return DuckDBBignumArray


def _duckdb_varint(storage_type: pa.DataType, serialized: bytes) -> pa.DataType | None:
    if not _is_binary(storage_type):
        return None
    # what duckdb 1.2 and 1.3 type the same VARINT as
    return DuckDBBignumType(storage_type, _DUCKDB_VARINT_OPAQUE)


_DUCKDB_BIGNUM_NAMES = {("DuckDB", "bignum"), ("DuckDB", "varint")}
"""The (vendor_name, type_name) of duckdb's BIGNUM, which was VARINT before 1.4."""


def _arrow_opaque(storage_type: pa.DataType, serialized: bytes) -> pa.DataType | None:
    try:
        names = json.loads(serialized)
    except (ValueError, RecursionError):
        return None
    if not isinstance(names, dict) or not _is_binary(storage_type):
        return None
    vendor_type = (names.get("vendor_name"), names.get("type_name"))
    if not all(isinstance(name, str) for name in vendor_type):
        return None
    if vendor_type not in _DUCKDB_BIGNUM_NAMES:
        return None
    return DuckDBBignumType(storage_type, serialized)


def _is_binary(data_type: pa.DataType) -> bool:
    return (
        pt.is_binary(data_type)
        or pt.is_large_binary(data_type)
        or pt.is_binary_view(data_type)
    )


def _parquet_variant(
    storage_type: pa.DataType, serialized: bytes
) -> pa.DataType | None:
    # exactly the two buffers, so a shredded variant, which adds a `typed_value`,
    # is left as its storage
    if not pt.is_struct(storage_type):
        return None
    children = {child.name: child.type for child in _children(storage_type)}
    if set(children) != {"metadata", "value"} or not all(
        _is_binary(child_type) for child_type in children.values()
    ):
        return None
    return ParquetVariantType(storage_type, serialized)


EXTENSION_TYPES: dict[bytes, Callable[[pa.DataType, bytes], pa.DataType | None]] = {
    b"geoarrow.wkb": _geoarrow_wkb,
    b"arrow.parquet.variant": _parquet_variant,
    OPAQUE_NAME: _arrow_opaque,
    b"duckdb.varint": _duckdb_varint,
}
"""The extension name a field declares, to the type this package reads it as.

A factory rather than a type, so that it sees the storage type the field actually
has -- WKB is `binary`, `large_binary` or `binary_view` -- and can decline a
storage type the name does not describe.
"""


def canonicalize(data: pa.Table) -> pa.Table:
    """`data` with every field `EXTENSION_TYPES` has a type for given that type.

    Nested fields included: duckdb tags the geometry inside a `LIST(GEOMETRY)` or
    a `STRUCT`, not the column around it. The cast is a reinterpretation of the
    same buffers, so this costs a schema walk and no copy.
    """
    old_fields = list(data.schema)
    new_fields = [_canonicalized_field(field) for field in old_fields]
    if all(field is None for field in new_fields):
        return data
    schema = pa.schema(
        [new or old for new, old in zip(new_fields, old_fields, strict=False)],
        metadata=data.schema.metadata,
    )
    # Arrow casts one extension type to another only by way of its storage, so a
    # field pyarrow read as `arrow.opaque` is first cast to that
    storage_schema = pa.schema(
        [_storage_field(field) or field for field in schema],
        metadata=data.schema.metadata,
    )
    if storage_schema.types != data.schema.types:
        data = data.cast(storage_schema)
    return data.cast(schema)


_DECODED_TYPES = (GeoArrowWkbType, ParquetVariantType, DuckDBBignumType)


def _storage_field(field: pa.Field) -> pa.Field | None:
    """`field` with every type of this package's under it replaced by its storage."""
    if isinstance(field.type, _DECODED_TYPES):
        return field.with_type(field.type.storage_type)
    nested_type = _rebuilt_type(field.type, _storage_field)
    return None if nested_type is None else field.with_type(nested_type)


def _declared_extension(field: pa.Field) -> tuple[bytes, bytes] | None:
    """The extension name and metadata of a type `field` declares that this
    pyarrow has not read as one of its own, or None if it declares none."""
    if isinstance(field.type, pa.OpaqueType):
        # pyarrow's class for it says only which vendor's type the storage holds
        names = {
            "type_name": field.type.type_name,
            "vendor_name": field.type.vendor_name,
        }
        return OPAQUE_NAME, json.dumps(names, separators=(",", ":")).encode()
    # an extension type pyarrow has any other class for has already been read as
    # one, and what its values are is that class's to say
    if isinstance(field.type, pa.BaseExtensionType):
        return None
    metadata = field.metadata or {}
    extension_name = metadata.get(EXTENSION_NAME_KEY)
    if extension_name is None:
        return None
    return extension_name, metadata.get(EXTENSION_METADATA_KEY, _NO_EXTENSION_METADATA)


def _canonicalized_field(field: pa.Field) -> pa.Field | None:
    """`field` under the extension type it declares, or None if it declares none."""
    declared = _declared_extension(field)
    build = None if declared is None else EXTENSION_TYPES.get(declared[0])
    if declared is not None and build is not None:
        storage_type = field.type
        if isinstance(storage_type, pa.BaseExtensionType):
            storage_type = storage_type.storage_type
        extension_type = build(storage_type, declared[1])
        if extension_type is not None:
            # the two keys are the type now, and pyarrow writes them back out
            # from it, so carrying them as metadata too would duplicate them
            metadata = field.metadata or {}
            remaining = {
                key: value
                for key, value in metadata.items()
                if key not in (EXTENSION_NAME_KEY, EXTENSION_METADATA_KEY)
            }
            return pa.field(
                field.name, extension_type, field.nullable, remaining or None
            )

    nested_type = _rebuilt_type(field.type, _canonicalized_field)
    return None if nested_type is None else field.with_type(nested_type)


_FieldRebuild = Callable[[pa.Field], "pa.Field | None"]


def _rebuilt_type(data_type: pa.DataType, rebuild: _FieldRebuild) -> pa.DataType | None:
    """`data_type` with `rebuild` applied to its children, or None if none changed."""
    if pt.is_struct(data_type):
        children = _children(data_type)
        rebuilt = _rebuilt_children(children, rebuild)
        return None if rebuilt is None else pa.struct(rebuilt)

    if pt.is_map(data_type):
        # a map's one child is its entries struct, of exactly a key and an item
        entries = _children(data_type.field(0).type)
        rebuilt = _rebuilt_children(entries, rebuild)
        if rebuilt is None:
            return None
        # duckdb 1.1 exports a key as nullable, which `pa.map_` refuses to build
        key_field = rebuilt[0].with_nullable(False)
        return pa.map_(key_field, rebuilt[1], keys_sorted=data_type.keys_sorted)

    if pt.is_list(data_type):
        value_field = rebuild(data_type.value_field)
        return None if value_field is None else pa.list_(value_field)

    if pt.is_large_list(data_type):
        value_field = rebuild(data_type.value_field)
        return None if value_field is None else pa.large_list(value_field)

    if pt.is_fixed_size_list(data_type):
        value_field = rebuild(data_type.value_field)
        if value_field is None:
            return None
        return pa.list_(value_field, data_type.list_size)

    return None


def _children(data_type: pa.DataType) -> list[pa.Field]:
    return [data_type.field(i) for i in range(data_type.num_fields)]


def _rebuilt_children(
    children: Iterable[pa.Field], rebuild: _FieldRebuild
) -> list[pa.Field] | None:
    """Every child with `rebuild` applied, or None if none changed."""
    old_fields = list(children)
    new_fields = [rebuild(field) for field in old_fields]
    if all(field is None for field in new_fields):
        return None
    return [new or old for new, old in zip(new_fields, old_fields, strict=False)]
