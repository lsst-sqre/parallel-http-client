"""Fixtures for parallel-http-client testing."""

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem


@pytest.fixture
def range_server() -> Iterator[None]:
    """Start an HTTP Server that knows about ranges."""
    old_cwd = Path.cwd()
    os.chdir((Path(__file__).parent) / "data")
    proc = subprocess.Popen(["python", "-m", "RangeHTTPServer"])
    os.chdir(old_cwd)
    yield
    proc.kill()


@pytest.fixture
def fake_fs(fs: FakeFilesystem) -> FakeFilesystem:
    fs.create_dir("/work")
    fs.add_real_file(
        source_path=(Path(Path(__file__).parent) / "data" / "pg11.txt"),
        target_path=Path("/data") / "reference.txt",
    )

    return fs
