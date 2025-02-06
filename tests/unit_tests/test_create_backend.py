from datetime import date, datetime

import pyarrow as pa
from textual_fastdatatable.backend import MAX_32BIT_INT, MAX_64BIT_INT, create_backend


def test_empty_sequence() -> None:
    backend = create_backend(data=[])
    assert backend
    assert backend.row_count == 0
    assert backend.column_count == 0
    assert backend.columns == []
    assert backend.column_content_widths == []


def test_infinity_timestamps() -> None:
    from_py = create_backend(
        data={"dt": [date.max, date.min], "ts": [datetime.max, datetime.min]}
    )
    assert from_py
    assert from_py.row_count == 2

    from_arrow = create_backend(
        data=pa.table(
            {
                "dt32": [
                    pa.scalar(MAX_32BIT_INT, type=pa.date32()),
                    pa.scalar(-MAX_32BIT_INT, type=pa.date32()),
                ],
                "dt64": [
                    pa.scalar(MAX_64BIT_INT, type=pa.date64()),
                    pa.scalar(-MAX_64BIT_INT, type=pa.date64()),
                ],
                "ts": [
                    pa.scalar(MAX_64BIT_INT, type=pa.timestamp('s')),
                    pa.scalar(-MAX_64BIT_INT, type=pa.timestamp('s')),
                ],
                "tns": [
                    pa.scalar(MAX_64BIT_INT, type=pa.timestamp('ns')),
                    pa.scalar(-MAX_64BIT_INT, type=pa.timestamp('ns')),
                ],
            }
        )
    )
    assert from_arrow
    assert from_arrow.row_count == 2
    assert from_arrow.get_row_at(0) == [date.max, date.max, datetime.max, datetime.max]
    assert from_arrow.get_row_at(1) == [date.min, date.min, datetime.min, datetime.min]
    assert from_arrow.get_column_at(0) == [date.max, date.min]
    assert from_arrow.get_column_at(2) == [datetime.max, datetime.min]
    assert from_arrow.get_cell_at(0,0) == date.max
