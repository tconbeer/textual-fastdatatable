from textual_fastdatatable.backend import create_backend


def test_empty_sequence() -> None:
    backend = create_backend(data=[])
    assert backend
    assert backend.row_count == 0
    assert backend.column_count == 0
    assert backend.columns == []
    assert backend.column_content_widths == []
