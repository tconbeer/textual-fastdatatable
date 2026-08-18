import subprocess
import sys

import pytest

# importing the backend must not import these; each is deferred to its use site,
# so that consumers that never render pay for none of them.
DEFERRED = ["textual", "rich", "pyarrow.parquet", "wcwidth"]


def _imported_modules(script: str) -> set[str]:
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    return set(proc.stdout.split())


@pytest.mark.parametrize("module", DEFERRED)
def test_backend_import_is_lazy(module: str) -> None:
    modules = _imported_modules(
        "import sys\n"
        "from textual_fastdatatable.backend import create_backend\n"
        "print('\\n'.join(sys.modules))\n"
    )
    assert module not in modules


def test_measuring_widths_imports_rich() -> None:
    """The deferred names still arrive when something actually needs them."""
    modules = _imported_modules(
        "import sys\n"
        "from textual_fastdatatable.backend import create_backend\n"
        "backend = create_backend([['a', 1], ['b', 2]])\n"
        "assert backend.column_content_widths == [1, 1]\n"
        "print('\\n'.join(sys.modules))\n"
    )
    assert "rich" in modules


def test_measuring_ascii_does_not_import_wcwidth() -> None:
    """The all-ASCII fast path in the cell-width UDF stays in Arrow."""
    modules = _imported_modules(
        "import sys\n"
        "from textual_fastdatatable.backend import create_backend\n"
        "backend = create_backend({'a': ['abc', 'de']})\n"
        "assert backend.column_content_widths == [3]\n"
        "print('\\n'.join(sys.modules))\n"
    )
    assert "wcwidth" not in modules


def test_measuring_wide_characters_imports_wcwidth() -> None:
    """A column that is not all ASCII has to be measured character by character."""
    modules = _imported_modules(
        "import sys\n"
        "from textual_fastdatatable.backend import create_backend\n"
        "backend = create_backend({'a': ['\u65e5\u672c\u8a9e', 'de']})\n"
        "assert backend.column_content_widths == [6]\n"
        "print('\\n'.join(sys.modules))\n"
    )
    assert "wcwidth" in modules


def test_console_is_built_on_first_measurement() -> None:
    """`format` owns the one Console, and does not build it at import time."""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from textual_fastdatatable import format\n"
            "print(format._console is None)\n"
            "format.measure_width('a')\n"
            "print(format._console is None)\n",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.split() == ["True", "False"]
