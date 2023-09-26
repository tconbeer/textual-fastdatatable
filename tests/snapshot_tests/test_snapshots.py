import pytest
from pathlib import Path

# These paths should be relative to THIS directory.
SNAPSHOT_APPS_DIR = Path("./snapshot_apps")


def test_auto_table(snap_compare) -> None:
    assert snap_compare(SNAPSHOT_APPS_DIR / "auto-table.py", terminal_size=(120, 40))


def test_datatable_render(snap_compare) -> None:
    press = ["tab", "down", "down", "right", "up", "left"]
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table.py", press=press)


def test_datatable_row_cursor_render(snap_compare) -> None:
    press = ["up", "left", "right", "down", "down"]
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_row_cursor.py", press=press)


def test_datatable_column_cursor_render(snap_compare) -> None:
    press = ["left", "up", "down", "right", "right"]
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_column_cursor.py", press=press)


@pytest.mark.skip(reason="Don't support sort")
def test_datatable_sort_multikey(snap_compare) -> None:
    press = ["down", "right", "s"]  # Also checks that sort doesn't move cursor.
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_sort.py", press=press)


@pytest.mark.skip(reason="Don't support mutating data.")
def test_datatable_remove_row(snap_compare) -> None:
    press = ["r"]
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_remove_row.py", press=press)


@pytest.mark.skip(reason="Don't support row labels.")
def test_datatable_labels_and_fixed_data(snap_compare) -> None:
    # Ensure that we render correctly when there are fixed rows/cols and labels.
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_row_labels.py")


@pytest.mark.xfail(
    reason=(
        "The data in this test includes formatting chars; the backend doesn't"
        "know these have zero width, so we draw the column wider than we used to"
    )
)
def test_datatable_style_ordering(snap_compare) -> None:
    # Regression test for https -> None://github.com/Textualize/textual/issues/2061
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_style_order.py")


def test_datatable_add_column(snap_compare) -> None:
    # Checking adding columns after adding rows
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_add_column.py")


@pytest.mark.skip(reason="No multi-height rows.")
def test_datatable_add_row_auto_height(snap_compare) -> None:
    # Check that rows added with auto height computation look right.
    assert snap_compare(SNAPSHOT_APPS_DIR / "data_table_add_row_auto_height.py")


@pytest.mark.skip(reason="No multi-height rows. No sorting.")
def test_datatable_add_row_auto_height_sorted(snap_compare) -> None:
    # Check that rows added with auto height computation look right.
    assert snap_compare(
        SNAPSHOT_APPS_DIR / "data_table_add_row_auto_height.py", press=["s"]
    )
