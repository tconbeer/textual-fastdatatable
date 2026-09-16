"""Arrow extension types this package gives a class of its own.

An extension type is a storage type with a meaning attached, and only the type
says which of the two a cell shows. pyarrow materializes the canonical ones
(`arrow.uuid`, `arrow.json`, …) as types of their own, but a type it has no class
for arrives as its bare storage with `ARROW:extension:name` in the field's
metadata -- which is how a geometry column reaches a table, as WKB tagged
`geoarrow.wkb`, from duckdb's spatial extension and from GeoArrow producers
generally. Its bytes are not what a cell shows.

`canonicalize()` gives such a field one of the types below, whose scalars convert
to the text the value means, so that the rest of this package measures and
renders a geometry through the extension-type path it already has for
`arrow.uuid` rather than as the binary preview a blob gets. A name with no class
here is left as its storage, which is what pyarrow does with it too.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.types as pt

from textual_fastdatatable.wkb import WkbError, wkb_to_wkt

EXTENSION_NAME_KEY = b"ARROW:extension:name"
EXTENSION_METADATA_KEY = b"ARROW:extension:metadata"

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
    if not (
        pt.is_binary(storage_type)
        or pt.is_large_binary(storage_type)
        or pt.is_binary_view(storage_type)
    ):
        return None
    return GeoArrowWkbType(storage_type, serialized)


EXTENSION_TYPES: dict[bytes, Callable[[pa.DataType, bytes], pa.DataType | None]] = {
    b"geoarrow.wkb": _geoarrow_wkb,
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
    return data.cast(schema)


def _canonicalized_field(field: pa.Field) -> pa.Field | None:
    """`field` under the extension type it declares, or None if it declares none."""
    metadata = field.metadata or {}
    extension_name = metadata.get(EXTENSION_NAME_KEY)
    # an extension type pyarrow has a class for has already been read as one, and
    # what its values are is that class's to say
    if extension_name is not None and not isinstance(field.type, pa.BaseExtensionType):
        build = EXTENSION_TYPES.get(extension_name)
        if build is not None:
            extension_type = build(
                field.type,
                metadata.get(EXTENSION_METADATA_KEY, _NO_EXTENSION_METADATA),
            )
            if extension_type is not None:
                # the two keys are the type now, and pyarrow writes them back out
                # from it, so carrying them as metadata too would duplicate them
                remaining = {
                    key: value
                    for key, value in metadata.items()
                    if key not in (EXTENSION_NAME_KEY, EXTENSION_METADATA_KEY)
                }
                return pa.field(
                    field.name, extension_type, field.nullable, remaining or None
                )

    nested_type = _canonicalized_type(field.type)
    return None if nested_type is None else field.with_type(nested_type)


def _canonicalized_type(data_type: pa.DataType) -> pa.DataType | None:
    """`data_type` with its children canonicalized, or None if none changed."""
    if pt.is_struct(data_type):
        children = _children(data_type)
        rebuilt = _canonicalized_children(children)
        return None if rebuilt is None else pa.struct(rebuilt)

    if pt.is_map(data_type):
        # a map's one child is its entries struct, of exactly a key and an item
        entries = _children(data_type.field(0).type)
        rebuilt = _canonicalized_children(entries)
        return None if rebuilt is None else pa.map_(rebuilt[0], rebuilt[1])

    if pt.is_list(data_type):
        value_field = _canonicalized_field(data_type.value_field)
        return None if value_field is None else pa.list_(value_field)

    if pt.is_large_list(data_type):
        value_field = _canonicalized_field(data_type.value_field)
        return None if value_field is None else pa.large_list(value_field)

    if pt.is_fixed_size_list(data_type):
        value_field = _canonicalized_field(data_type.value_field)
        if value_field is None:
            return None
        return pa.list_(value_field, data_type.list_size)

    return None


def _children(data_type: pa.DataType) -> list[pa.Field]:
    return [data_type.field(i) for i in range(data_type.num_fields)]


def _canonicalized_children(children: Iterable[pa.Field]) -> list[pa.Field] | None:
    """Every child under the extension type it declares, or None if none does."""
    old_fields = list(children)
    new_fields = [_canonicalized_field(field) for field in old_fields]
    if all(field is None for field in new_fields):
        return None
    return [new or old for new, old in zip(new_fields, old_fields, strict=False)]
