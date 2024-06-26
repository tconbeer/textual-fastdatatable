from __future__ import annotations

from typing import Sequence

import pytest
from textual_fastdatatable import ArrowBackend
from textual_fastdatatable.backend import PolarsBackend, ArrowBackend, DataTableBackend

@pytest.fixture
def pydict() -> dict[str, Sequence[str | int]]:
    return {
        "first column": [1, 2, 3, 4, 5],
        "two": ["a", "b", "c", "d", "asdfasdf"],
        "three": ["foo", "bar", "baz", "qux", "foofoo"],
    }


@pytest.fixture
def records(pydict: dict[str, Sequence[str | int]]) -> list[tuple[str | int, ...]]:
    header = tuple(pydict.keys())
    cols = list(pydict.values())
    num_rows = len(cols[0])
    data = [tuple([col[i] for col in cols]) for i in range(num_rows)]
    return [header, *data]



@pytest.fixture(params=[ArrowBackend, PolarsBackend])
def backend(request, pydict: dict[str, Sequence[str | int]]) -> DataTableBackend:
    return request.param.from_pydict(pydict)