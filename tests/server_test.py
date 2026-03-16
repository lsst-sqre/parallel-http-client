"""Tests for parallel-http-client."""

from pathlib import Path

import httpx
import pytest

import parallel_http_client as phc


@pytest.mark.usefixtures("range_server")
def test_singlethreaded(range_server: None) -> None:
    """Test single-threaded, single-process performance."""
    _ = phc.ParallelHTTPClient(
        httpx.URL("http://localhost:8000/pg11.txt"),
        output=Path("pg11-singlethreaded.txt"),
        max_threads=1,
        max_procs=1,
        chunk_size=16384,
    )
    assert (
        Path("pg11.txt").read_text()
        == Path("pg11-singlethreaded.txt").read_text()
    )
