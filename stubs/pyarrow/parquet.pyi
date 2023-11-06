from __future__ import annotations

from typing import Any, BinaryIO, Literal

from . import NativeFile, Schema, Table
from .compute import Expression
from .dataset import Partitioning
from .fs import FileSystem

class FileMetaData: ...

def read_table(
    source: str | NativeFile | BinaryIO,
    *,
    columns: list | None = None,
    use_threads: bool = True,
    metadata: FileMetaData | None = None,
    schema: Schema | None = None,
    use_pandas_metadata: bool = False,
    read_dictionary: list | None = None,
    memory_map: bool = False,
    buffer_size: int = 0,
    partitioning: Partitioning | str | list[str] = "hive",
    filesystem: FileSystem | None = None,
    filters: Expression | list[tuple] | list[list[tuple]] | None = None,
    use_legacy_dataset: bool = False,
    ignore_prefixes: list | None = None,
    pre_buffer: bool = True,
    coerce_int96_timestamp_unit: str | None = None,
    decryption_properties: Any | None = None,
    thrift_string_size_limit: int | None = None,
    thrift_container_size_limit: int | None = None,
) -> Table: ...
def write_table(
    table: Table,
    where: str | NativeFile,
    row_group_size: int | None = None,
    version: Literal["1.0", "2.4", "2.6"] = "2.6",
    use_dictionary: bool | list = True,
    compression: Literal["none", "snappy", "gzip", "brotli", "lz4", "zstd"]
    | dict[str, Literal["none", "snappy", "gzip", "brotli", "lz4", "zstd"]] = "snappy",
    write_statistics: bool | list = True,
    use_deprecated_int96_timestamps: bool | None = None,
    coerce_timestamps: str | None = None,
    allow_truncated_timestamps: bool = False,
    data_page_size: int | None = None,
    flavor: Literal["spark"] | None = None,
    filesystem: FileSystem | None = None,
    compression_level: int | dict | None = None,
    use_byte_stream_split: bool | list = False,
    column_encoding: str | dict | None = None,
    data_page_version: Literal["1.0", "2.0"] = "1.0",
    use_compliant_nested_type: bool = True,
    encryption_properties: Any | None = None,
    write_batch_size: int | None = None,
    dictionary_pagesize_limit: int | None = None,
    store_schema: bool = True,
    write_page_index: bool = False,
    **kwargs: Any,
) -> None: ...
