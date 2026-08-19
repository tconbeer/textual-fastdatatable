from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Literal

from . import DataType, MemoryPool, Scalar, _PandasConvertible

class Expression: ...
class ScalarAggregateOptions: ...

class CastOptions:
    def __init__(
        self,
        target_type: DataType | None = None,
        allow_int_overflow: bool | None = None,
        allow_time_truncate: bool | None = None,
        allow_time_overflow: bool | None = None,
        allow_decimal_truncate: bool | None = None,
        allow_float_truncate: bool | None = None,
        allow_invalid_utf8: bool | None = None,
    ) -> None: ...

def max(  # noqa: A001
    array: _PandasConvertible,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar: ...
def min(  # noqa: A001
    array: _PandasConvertible,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar: ...
def any(  # noqa: A001
    array: _PandasConvertible,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar: ...
def or_(
    x: _PandasConvertible,
    y: _PandasConvertible,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def match_substring(
    strings: _PandasConvertible,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def match_substring_regex(
    strings: _PandasConvertible,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def binary_length(
    strings: _PandasConvertible, /, *, memory_pool: MemoryPool | None = None
) -> _PandasConvertible: ...
def not_equal(
    x: _PandasConvertible,
    y: _PandasConvertible,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def replace_with_mask(
    values: _PandasConvertible,
    mask: _PandasConvertible,
    replacements: _PandasConvertible,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def unique(
    array: _PandasConvertible, /, *, memory_pool: MemoryPool | None = None
) -> _PandasConvertible: ...
def index_in(
    values: _PandasConvertible,
    /,
    value_set: _PandasConvertible,
    *,
    skip_nulls: bool = False,
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def take(
    data: _PandasConvertible,
    indices: _PandasConvertible,
    /,
    *,
    boundscheck: bool = True,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def utf8_length(
    strings: _PandasConvertible, /, *, memory_pool: MemoryPool | None = None
) -> _PandasConvertible: ...
def find_substring(
    strings: _PandasConvertible,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def greater_equal(
    x: _PandasConvertible,
    y: _PandasConvertible | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def less(
    x: _PandasConvertible,
    y: _PandasConvertible | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def add(
    x: _PandasConvertible,
    y: _PandasConvertible | Scalar | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def min_element_wise(
    *args: _PandasConvertible,
    skip_nulls: bool = True,
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def if_else(
    cond: _PandasConvertible,
    left: _PandasConvertible,
    right: _PandasConvertible,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
def register_scalar_function(
    func: Callable,
    function_name: str,
    function_doc: dict[Literal["summary", "description"], str],
    in_types: dict[str, DataType],
    out_type: DataType,
    func_registry: Any | None = None,
) -> None: ...
def call_function(
    function_name: str, target: list[_PandasConvertible]
) -> _PandasConvertible: ...
def assume_timezone(
    timestamps: _PandasConvertible | Scalar | datetime,
    /,
    timezone: str,
    *,
    ambiguous: Literal["raise", "earliest", "latest"] = "raise",
    nonexistent: Literal["raise", "earliest", "latest"] = "raise",
    options: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> _PandasConvertible: ...
