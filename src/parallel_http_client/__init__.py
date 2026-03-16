"""Export client class."""

from ._cli import parallel_fetch
from ._client import ParallelHTTPClient

__all__ = ["ParallelHTTPClient", "parallel_fetch"]
