from __future__ import annotations

from . import DataType, Date32Type, Date64Type, TimestampType

def is_null(t: DataType) -> bool: ...
def is_struct(t: DataType) -> bool: ...
def is_boolean(t: DataType) -> bool: ...
def is_integer(t: DataType) -> bool: ...
def is_floating(t: DataType) -> bool: ...
def is_decimal(t: DataType) -> bool: ...
def is_temporal(t: DataType) -> bool: ...
def is_date(t: DataType) -> bool: ...
def is_date32(t: DataType) -> bool:
    if isinstance(t, Date32Type):
        return True
    return False

def is_date64(t: DataType) -> bool:
    if isinstance(t, Date64Type):
        return True
    return False

def is_time(t: DataType) -> bool: ...
def is_timestamp(t: DataType) -> bool:
    if isinstance(t, TimestampType):
        return True
    return False
