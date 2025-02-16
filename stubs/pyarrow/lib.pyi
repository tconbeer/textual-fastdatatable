from . import Date32Type, Date64Type, Scalar, TimestampType

class ArrowException(Exception): ...
class ArrowInvalid(ValueError, ArrowException): ...
class ArrowMemoryError(MemoryError, ArrowException): ...
class ArrowKeyError(KeyError, Exception): ...
class ArrowTypeError(TypeError, Exception): ...
class ArrowNotImplementedError(NotImplementedError, ArrowException): ...
class ArrowCapacityError(ArrowException): ...
class ArrowIndexError(IndexError, ArrowException): ...
class ArrowSerializationError(ArrowException): ...
class ArrowCancelled(ArrowException): ...

ArrowIOError = IOError

class Date32Scalar(Scalar):
    @property
    def type(self) -> Date32Type: ...
    @property
    def value(self) -> int: ...

class Date64Scalar(Scalar):
    @property
    def type(self) -> Date64Type: ...
    @property
    def value(self) -> int: ...

class TimestampScalar(Scalar):
    @property
    def type(self) -> TimestampType: ...
    @property
    def value(self) -> int: ...
