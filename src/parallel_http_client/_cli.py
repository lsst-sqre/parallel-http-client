"""CLI for parallel-http-client."""

import argparse
from pathlib import Path

import httpx

from ._client import ParallelHTTPClient, _ByteRange


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="parallel-fetch",
        description=(
            "Fetches URLs in parallel, using multiple threads and/or"
            ' multiple processes and the "Range" header'
        ),
    )
    parser.add_argument("url", help="URL to fetch")
    parser.add_argument(
        "-o",
        "--output",
        help=("Output file [default: URL filename in current directory]"),
    )
    parser.add_argument(
        "-w",
        "--working-dir",
        help=(
            "Working directory parent for file download and reassembly"
            " [default: system temporary directory, usually $TMPDIR or /tmp]"
        ),
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        help=("Number of processes [default: 1]"),
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        help=("Number of threads per process [default: 10]"),
    )
    parser.add_argument(
        "-c",
        "--chunksize",
        type=int,
        help=("Chunk size to read per HTTP request [default: 1048576]"),
    )
    parser.add_argument(
        "-r",
        "--report",
        action="store_true",
        help=("Write summary report when finished [default: False]"),
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help=("Enable debugging output [default: False]"),
    )
    parser.add_argument(
        "--byte-range",
        help=(
            "Byte range for subprocess: do not use on CLI."
            " This will be used when the client is spawning subprocesses."
        ),
    )
    parser.add_argument(
        "--filesize",
        type=int,
        help=(
            "File size for subprocess: do not use on CLI."
            " This will be used when the client is spawning subprocesses."
        ),
    )
    return _xform_args(parser.parse_args())


def _xform_args(args: argparse.Namespace) -> argparse.Namespace:
    if isinstance(args.url, str):
        args.url = httpx.URL(args.url)
    if args.output is None:
        args.output = Path(str(args.url)).name
    else:
        args.output = Path(args.output)
    if args.working_dir is not None:
        if isinstance(args.working_dir, str):
            args.working_dir = Path(args.working_dir)
    if args.byte_range is not None:
        if isinstance(args.byte_range, str):
            args.byte_range = _ByteRange.from_str(args.byte_range)
    return args


def parallel_fetch() -> None:
    """Download from a URL."""
    args = _parse_args()
    _ = ParallelHTTPClient(
        url=args.url,
        output=args.output,
        working_dir=args.working_dir,
        max_threads=args.threads,
        max_procs=args.processes,
        chunk_size=args.chunksize,
        report=args.report,
        debug=args.debug,
        byte_range=args.byte_range,
        filesize=args.filesize,
    )


if __name__ == "__main__":
    parallel_fetch()
