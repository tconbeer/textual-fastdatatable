"""Guards the "wheel pins" in pyproject.toml.

numpy, pandas, and pyarrow ship binary wheels for a Python version only from
some release onwards, so a floor that is too low lets a resolver pick a release
that has to be built from source on a newer interpreter. These tests resolve
this project's dependencies for every supported Python and platform with
`--only-binary :all:`, which fails if any dependency would need a source build.

They shell out to `uv` and hit the package index, so they are skipped when `uv`
is unavailable or the index cannot be reached.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"

PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
PLATFORMS = [
    "x86_64-manylinux_2_28",
    "aarch64-manylinux_2_28",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "windows",
]
NETWORK_ERRORS = ("failed to fetch", "error sending request", "request failed")


@pytest.fixture(scope="session")
def uv() -> str:
    exe = shutil.which("uv")
    if exe is None:
        pytest.skip("uv is not installed")
    return exe


@pytest.mark.parametrize("platform", PLATFORMS)
@pytest.mark.parametrize("python_version", PYTHON_VERSIONS)
def test_dependencies_resolve_to_wheels(
    uv: str, python_version: str, platform: str
) -> None:
    proc = subprocess.run(
        [
            uv,
            "pip",
            "compile",
            str(PYPROJECT),
            "--all-extras",
            "--only-binary",
            ":all:",
            "--python-version",
            python_version,
            "--python-platform",
            platform,
            "--no-header",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        if any(err in proc.stderr.lower() for err in NETWORK_ERRORS):
            pytest.skip(f"could not reach the package index: {proc.stderr.strip()}")
        pytest.fail(
            f"dependencies do not resolve to wheels on Python {python_version} "
            f"({platform}):\n{proc.stderr.strip()}"
        )
