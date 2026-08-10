import subprocess
import sys

import pytest

# importing the backend must not import these; each is deferred to its use site,
# so that consumers that never render pay for none of them.
DEFERRED = ["textual", "rich", "pyarrow.parquet"]


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
