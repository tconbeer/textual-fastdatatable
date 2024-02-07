from __future__ import annotations

from . import DataType

def is_null(t: DataType) -> bool: ...
def is_struct(t: DataType) -> bool: ...
def is_boolean(t: DataType) -> bool: ...
def is_integer(t: DataType) -> bool: ...
def is_floating(t: DataType) -> bool: ...
def is_decimal(t: DataType) -> bool: ...
def is_temporal(t: DataType) -> bool: ...
def is_date(t: DataType) -> bool: ...
def is_time(t: DataType) -> bool: ...
def is_timestamp(t: DataType) -> bool: ...
