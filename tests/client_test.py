"""Tests for parallel-http-client."""

import os
from pathlib import Path

import httpx
import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

import parallel_http_client as phc


@pytest.mark.usefixtures("range_server", "fake_fs")
def test_singlethreaded(range_server: None, fake_fs: FakeFilesystem) -> None:
    """Test single-threaded, single-process download."""
    reference = Path("/data") / "reference.txt"
    output = Path("/work") / "pg11-singlethreaded.txt"
    os.chdir("/work")
    _ = phc.ParallelHTTPClient(
        httpx.URL("http://localhost:8000/pg11.txt"),
        output=output,
        max_threads=1,
        max_procs=1,
        chunk_size=16384,
        debug=True,
    )
    assert reference.read_text() == output.read_text()


@pytest.mark.usefixtures("range_server", "fake_fs")
def test_multithreaded(range_server: None, fake_fs: FakeFilesystem) -> None:
    """Test single-threaded, single-process download."""
    reference = Path("/data") / "reference.txt"
    output = Path("/work") / "pg11-multithreaded.txt"
    os.chdir("/work")
    _ = phc.ParallelHTTPClient(
        httpx.URL("http://localhost:8000/pg11.txt"),
        output=output,
        max_threads=10,
        max_procs=1,
        chunk_size=16384,
        debug=True,
    )
    assert reference.read_text() == output.read_text()


# The multiprocess stuff doesn't work well with fake_fs.
# Seems to be fine in the field.


@pytest.mark.usefixtures("range_server", "fake_fs")
def test_big_chunk_singleprocess(
    range_server: None, fake_fs: FakeFilesystem
) -> None:
    """Test we get a single process if the chunk is bigger than the file."""
    reference = Path("/data") / "reference.txt"
    output = Path("/work") / "pg11-bigchunk.txt"
    os.chdir("/work")
    _ = phc.ParallelHTTPClient(
        httpx.URL("http://localhost:8000/pg11.txt"),
        output=output,
        max_threads=1,
        max_procs=3,
        debug=True,
    )
    assert reference.read_text() == output.read_text()
