"""Fixtures for parallel-http-client testing."""

import os
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def environment() -> Iterator[None]:
    with tempfile.TemporaryDirectory() as td:
        tdir = Path(td)
        old_cwd = Path.cwd()
        (tdir / "pg11.txt").write_text(
            (Path(__file__) / "data" / "pg11.txt").read_text()
        )
        os.chdir(tdir)
        yield
        os.chdir(old_cwd)


@pytest.fixture
def range_server(environment: Iterator[None]) -> Iterator[None]:
    """Start an HTTP Server that knows about ranges."""
    proc = subprocess.Popen(["python", "-m", "RangeHTTPServer"])
    yield
    proc.kill()
