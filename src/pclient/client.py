#!/usr/bin/env python3
"""Client for parallel fetches.

We assume that everything needed is in this file, the standard
library, or httpx, and that this file is executable (which makes the
subprocess stuff a lot easier to deal with).

If parallelizing over subprocesses, each subprocess gets a contiguous region
of the target URL.  Each such region is chunked for download and divided among
worker threads using byte-range GET requests.
"""

import argparse
import asyncio
import collections
import datetime
import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Self, override

import httpx


@dataclass
class _TimingEvent:
    """Timing metric."""

    start: bool
    ordinal: int
    name: str
    time: datetime.datetime
    pid: int
    duration: datetime.timedelta | None = None


@dataclass
class _ByteRange:
    """Range of bytes within a byte stream."""

    first: int
    last: int

    @override
    def __str__(self) -> str:
        return f"{self.first}-{self.last}"

    @classmethod
    def from_str(cls, inp: str) -> Self:
        first, last = map(int, inp.split("-"))
        return cls(first=first, last=last)


class ParallelClient:
    """Download a URL using HTTP GET, using the Range header to pull the file
    in chunks, which the client then reassembles.
    """

    def __init__(
        self,
        url: httpx.URL,
        *,
        output: Path | None = None,
        working_dir: Path | None = None,
        max_threads: int | None = None,
        max_procs: int | None = None,
        chunk_size: int | None = None,
        report: bool = False,
        debug: bool = False,
        byte_range: _ByteRange | None = None,
        filesize: int | None = None,
    ) -> None:
        """
        If a range is specified, then only a single process may
        process that range.

        Typically you invoke the parent process without a range, and it
        does the division of the file into max_procs equally-sized ranges.
        """
        self._url = url
        self.output: Path | None = None
        if output is None:
            self._output = Path(Path(self._url.path).name)
        else:
            self._output = output
        self._working_dir = working_dir
        self._byte_range = byte_range
        if max_procs is None:
            max_procs = 1
        if max_threads is None:
            max_threads = 5
        if chunk_size is None:
            chunk_size = 2**20
        if self._byte_range is None:
            self._max_procs = max_procs
        else:
            self._max_procs = 1
        self._max_threads = max_threads
        self._chunk_size = chunk_size
        self._report = report
        self._debug = debug
        self._discard_output = False
        self._pid = os.getpid()
        self._work_dir: Path | None = None
        self._lock = threading.Lock()

        if self._output == Path("/dev/null"):
            self._discard_output = True
        self._filesize = filesize

        # Set all the things we need to protect with a lock
        self._lock.acquire()
        self._timings: list[_TimingEvent] = []
        self._ordinal = 1
        self._chunks: collections.deque[_ByteRange] = collections.deque()
        self._worker_threads = 0
        self._lock.release()

        # Set up logging
        self._logger = logging.getLogger("ParallelClient")
        loglevel = logging.DEBUG if self._debug else logging.INFO
        logging.basicConfig(level=loglevel)
        self._logger.setLevel(loglevel)

        self._me = Path(__file__).resolve()

        self._logger.debug(
            f"[{self._pid}]"
            f" __init()__ values for {self._me!s}:"
            f" URL: {self._url!s}"
            f" output: {self._output!s}"
            f" working dir: {self._working_dir!s}"
            f" max_procs: {self._max_procs}"
            f" max_threads: {self._max_threads}"
            f" chunk_size: {self._chunk_size}"
            f" report: {self._report}"
            f" debug: {self._debug}"
            f" byte_range: {self._byte_range}"
        )

        # Do the right thing if we are invoking ourself as a subprocess.
        if self._byte_range is None:
            asyncio.run(self._master_fetch())
        else:
            asyncio.run(self._subprocess_fetch())

    async def _master_fetch(self) -> None:
        """Fetch a URL.

        This will use the instance parameters to determine how many processes
        and threads to run in order to download the target URL.

        It will parallelize the fetch, and then stitch it back together,
        unless the output is the file ``/dev/null``, in which case output
        will be discarded.
        """
        url = self._url
        output = self._output
        work_dir = self._working_dir
        with tempfile.TemporaryDirectory(dir=work_dir) as wd:
            cwd = Path(wd)
            os.chdir(cwd)
            self._logger.debug(f"[{self._pid}] Working directory is now {wd}")
            if self._byte_range:
                raise ValueError(
                    "_master_fetch can only be called from parent process"
                )
            # Truncate output file
            with output.open("wb") as f:
                f.truncate()
            self._lock.acquire()
            self._ordinal = 1  # Reset event counter
            self._timings = []  # and timings
            self._lock.release()
            event = f"fetch {url}"
            self._start_stamp(event)  # Set first timestamp
            await self._get_filesize()
            if self._filesize is None:
                self._logger.warning(
                    f"[{self._pid}] {url} does not support byte ranges;"
                    f" single-threaded output in {output}"
                )
            else:
                await self._parallel_fetch()
            self._end_stamp(event)
            if not self._discard_output:
                await self._reassemble_file_parts()
            if self._report:
                await self._generate_report()
                await self._consolidate_reports()

    async def _subprocess_fetch(self) -> None:
        url = self._url
        if self._byte_range is None:
            raise ValueError(
                "Subprocess fetch can only be called from child process"
            )
        byte_range = self._byte_range
        event = f"subprocess fetch for {url}, byte range {byte_range}"
        self._start_stamp(event)
        await self._fetch_singleproc_parts(byte_range)
        self._end_stamp(event)
        if self._report:
            await self._generate_report()

    async def _parallel_fetch(self) -> None:
        """Master process only can use this."""
        event = "parallel fetch"
        size = self._filesize
        if not size:
            raise ValueError("Do not know filesize for parallel fetch")
        self._start_stamp(event)
        fullrange = _ByteRange(first=0, last=size - 1)
        if self._max_procs > 1:
            await self._divide_subprocs(fullrange)
        else:
            await self._fetch_singleproc_parts(fullrange)
        self._end_stamp(event)

    async def _get_filesize(self) -> None:
        """Get the expected size of the file and store it in self._filesize.

        If None, we just assume the whole file got returned, and write it.

        If the filesize is determined, also set self._chunks to a list of
        byte ranges that will be consumed when fetching.

        Note that we use GET with a range of 0-0 rather than HEAD, because
        signed URLs are method-specific, and we're mostly building this
        as a test harness for testing signed URL retrieval speeds.
        """
        url = self._url
        output = self._output
        event = f"Get filesize for {url} and chunk it"
        self._start_stamp(event)
        async with httpx.AsyncClient(http2=True, follow_redirects=True) as c:
            ev2 = f"Get range for {url}"
            self._start_stamp(ev2)
            headers = {"Range": "bytes=0-0"}
            self._logger.debug(
                f"[{self._pid}] About to fetch {url} for filesize"
            )
            r = await c.get(url, headers=headers)
            self._end_stamp(ev2)
            r.raise_for_status()
            crh = r.headers.get("content-range")
            filesize: int | None = None
            if crh is not None:
                _, t_filesize = crh.split("/")
                filesize = int(t_filesize)
            if r.status_code != 206 or not filesize:
                ev3 = f"Filesize for {url} failed; getting whole file"
                self._start_stamp(ev3)
                # Uh oh!  We're getting the whole file, not a range!
                if r.status_code != 200:
                    # Eh, whatever we got...it was neither OK nor Partial
                    # Content.  Do it again without the range.
                    ev3 = f"Download whole file from {url}"
                    self._start_stamp(ev3)
                    r = await c.get(url)
                    self._end_stamp(ev3)
                    r.raise_for_status()
                await self._write_file(output, r)
                self._end_stamp(ev2)
                self._end_stamp(event)
                return
            self._end_stamp(event)
            if self._filesize and self._filesize != filesize:
                self._logger.warning(
                    f"[{self._pid}] Stored filesize {self._filesize}"
                    f" != {filesize}"
                )
            self._filesize = filesize
            self._logger.debug(f"[{self._pid}] Stored filesize {filesize}")

    async def _chunk_range(self, inp: _ByteRange) -> None:
        event = f"Chunking range {inp}"
        self._start_stamp(event)
        offset = inp.first
        chunks: collections.deque[_ByteRange] = collections.deque()
        while offset <= inp.last:
            if offset + self._chunk_size < inp.last:
                end = offset + self._chunk_size - 1
            else:
                end = inp.last
            chunk = _ByteRange(first=offset, last=end)
            offset = offset + self._chunk_size
            chunks.append(chunk)
        self._lock.acquire()
        self._chunks = chunks
        self._lock.release()
        self._logger.debug(f"[{self._pid}] Chunks: {self._chunks}")
        self._end_stamp(event)

    async def _write_file(self, output: Path, r: httpx.Response) -> None:
        verb = "write"
        if self._discard_output:
            verb = "discard"
        event = f"{verb} file {output!s} from HTTP response"
        self._start_stamp(event)
        r.raise_for_status()
        if self._discard_output:
            self._end_stamp(event)
            return
        with output.open("wb") as f:
            async for data in r.aiter_bytes():
                self._logger.debug(
                    f"[{self._pid}] writing {len(data)} bytes to {output!s}"
                )
                f.write(data)
        self._end_stamp(event)

    def _start_stamp(self, event: str) -> None:
        self._logger.debug(f"[{self._pid}] Start {event}")
        self._stamp(event=event, start=True)

    def _end_stamp(self, event: str) -> None:
        self._stamp(event=event, start=False)
        self._logger.debug(f"[{self._pid}] Stop {event}")

    def _stamp(self, event: str, *, start: bool) -> None:
        now = datetime.datetime.now(tz=datetime.UTC)
        self._lock.acquire()
        self._timings.append(
            _TimingEvent(
                start=start,
                ordinal=self._ordinal,
                name=event,
                time=now,
                pid=self._pid,
            )
        )
        self._ordinal += 1
        self._lock.release()

    async def _generate_report(self) -> None:
        """Return event timings."""
        retval: list[str] = []
        self._lock.acquire()
        self._add_durations()
        for ev in self._timings:
            ev_id = f"{ev.pid}:{ev.ordinal}:{ev.name}"
            r_str = f"{ev_id}: {ev.time!s}"
            if ev.duration:
                r_str += f" duration {ev.duration!s}"
            r_str += "\n"
            retval.append(r_str)
        self._lock.release()
        pid = os.getpid()
        logfile = Path(f"client.{pid}.log")
        logfile.write_text(r_str)

    async def _consolidate_reports(self) -> None:
        logtxt = ""
        logfiles = Path().glob("client.*.log")
        for lg in logfiles:
            logtxt += lg.read_text()
        for line in logtxt.split("\n"):
            self._logger.info(line)

    def _add_durations(self) -> None:
        e_d: dict[str, _TimingEvent] = {}
        for ev in self._timings:
            if ev.duration is not None:
                continue  # We already have a timing
            ev_id = f"{ev.pid}:{ev.name}"
            if ev.start:
                e_d[ev_id] = ev
                continue
            if ev_id:
                continue
            # Add duration to both start and finish times
            ev.duration = ev.time - e_d[ev_id].time
            e_d[ev_id].duration = ev.time - e_d[ev_id].time
            # And remove it from the dict of things we're waiting for
            del e_d[ev_id]
        if e_d:
            self._logger.warning(
                f"[{self._pid}] Found start but no finish"
                f" for {list(e_d.keys())}"
            )

    async def _divide_subprocs(self, byte_range: _ByteRange) -> None:
        size = byte_range.last - byte_range.first + 1
        if self._max_procs < 2:
            self._logger.warning(
                f"[{self._pid}] Cannot subdivide process any further."
            )
            return
        chunks_per_process = int(size / (self._max_procs * self._chunk_size))
        leftover_chunks = int(
            (size - self._max_procs * chunks_per_process * self._chunk_size)
            / self._chunk_size
        )
        leftover_bytes = size - (
            (chunks_per_process + leftover_chunks) * self._chunk_size
        )
        if leftover_bytes:
            leftover_chunks += 1
        proc_ranges: list[_ByteRange] = []
        offset = 0
        for i in range(self._max_procs):
            first = offset
            last = offset + self._chunk_size * chunks_per_process - 1
            if i + leftover_chunks >= self._max_procs:
                last += self._chunk_size
            if last >= size:
                last = size - 1
            proc_ranges.append(_ByteRange(first=first, last=last))
            offset = last + 1
        await self._spawn_subprocs(proc_ranges, size)

    async def _spawn_subprocs(
        self, byte_ranges: list[_ByteRange], size: int
    ) -> None:
        """Spawn subprocesses, one per byte range, then wait for each."""
        child_procs: list[asyncio.subprocess.Process] = []
        for byte_range in byte_ranges:
            # max_procs for each child is 1
            # do not specify working directory--you're already there
            args = [
                str(self._url),
                "--byte-range",
                str(byte_range),
                "--filesize",
                str(self._filesize),
                "--output",
                str(self._output),
                "--processes",
                "1",
                "--threads",
                str(self._max_threads),
                "--chunksize",
                str(self._chunk_size),
            ]
            if self._report:
                args.append("--report")
            if self._debug:
                args.append("--debug")

            self._logger.debug(
                f"[{self._pid}] Spawn subprocess: {self._me!s} {args}"
            )
            proc = await asyncio.subprocess.create_subprocess_exec(
                str(self._me), *args
            )
            child_procs.append(proc)
        for proc in child_procs:
            await proc.wait()
            if proc.returncode != 0:
                self._logger.error(
                    f"[{self._pid}] Process {proc} exited with"
                    f" rc {proc.returncode}"
                )

    async def _fetch_singleproc_parts(self, byte_range: _ByteRange) -> None:
        """Use a single process with multiple threads to divide the range
        into chunks, then chunks out of the chunk queue and pull them to files.

        We assume that the working directory is the place we put chunk files.
        """
        output = self._output
        base_file = Path(output.name)
        tasks: set[asyncio.Task] = set()
        event = "fetch single process parts"
        self._start_stamp(event)
        await self._chunk_range(byte_range)
        while True:
            self._logger.debug(f"[{self._pid}] Acquiring lock to get chunk")
            self._lock.acquire()
            if len(self._chunks) < 1 and len(tasks) < 1:
                # We have handled each chunk and all our tasks are done.
                self._logger.debug(
                    f"[{self._pid}] Chunks and tasks finished; releasing lock"
                )
                self._lock.release()
                break
            tl = len(tasks)
            self._logger.debug(f"[{self._pid}] {tl} task[s] exist[s]")
            while tl < self._max_threads:
                if len(self._chunks) > 0:
                    chunk = self._chunks.popleft()
                    self._logger.debug(
                        f"[{self._pid}] Got chunk {chunk} to work on"
                    )
                    tasks.add(
                        asyncio.create_task(
                            self._fetch_range(chunk, base_file)
                        )
                    )
                    self._logger.debug(
                        f"[{self._pid}] Added task for {chunk}, {base_file}"
                    )
                    tl += 1
                else:
                    # If we run out of chunks, we can't refill the thread
                    # count.
                    self._logger.debug(f"[{self._pid}] Chunks exhausted")
                    break
            # Check each task for completion
            remove = set()
            for task in tasks:
                self._logger.debug(
                    f"[{self._pid}] Checking task {task} for completion"
                )
                if task.done():
                    self._logger.debug(f"[{self._pid}] Task {task} complete")
                    remove.add(task)  # Don't alter the set while iterating it.
            for task in remove:
                self._logger.debug(
                    f"[{self._pid}] Removing completed task {task}"
                )
                tasks.remove(task)
            self._logger.debug(
                f"[{self._pid}] {len(tasks)} task[s] remain[s];"
                " releasing lock."
            )
            self._lock.release()
            if len(tasks) == self._max_threads:
                self._logger.debug(
                    f"[{self._pid}] All task slots are full; waiting."
                )
            # Pause a bit.
            await asyncio.sleep(0.1)
        self._logger.debug(f"[{self._pid}] Left chunk task loop.")
        self._end_stamp(event)
        if self._report:
            await self._generate_report()

    async def _fetch_range(
        self, byte_range: _ByteRange, base_file: Path
    ) -> None:
        """Assume that we are already in a world that supports
        byte-range requests, and that we know how big the file is (we
        need that to be able to just lexically sort the files and
        reassemble them at the end).  We're just going to grab that
        range and write it to a file; we're going to worry about
        reassembling the file later.

        We're going to assume further that our working directory is the
        directory where we want to write this file.
        """
        url = self._url
        if self._filesize is None:
            self._logger.debug(f"[{self._pid}] filesize is unknown; using 1e9")
        filesize = self._filesize or 1e9
        digits = len(str(filesize))
        n_format = f"{{:0{digits}d}}"
        # Format these to allow lexical sorting
        s_text = n_format.format(byte_range.first)
        l_text = n_format.format(byte_range.last)
        r_text = f"bytes={byte_range!s}"
        part_file = Path(f"{base_file.name}.{s_text}-{l_text}.part")
        hdr = {"Range": r_text}
        event = f"GET {url!s} range {r_text}"
        self._start_stamp(event)
        async with httpx.AsyncClient(http2=True, follow_redirects=True) as c:
            async with c.stream("GET", url, headers=hdr) as r:
                await self._write_file(part_file, r)
        self._end_stamp(event)

    async def _reassemble_file_parts(
        self,
    ) -> None:
        if self._discard_output:
            return
        outp = self._output
        base_name = outp.name
        event = f"Reassemble {base_name}"
        self._start_stamp(event)
        parts = Path().glob(f"{base_name}.*.part")
        self._logger.debug(f"[{self._pid}] file parts: {parts}")
        async with asyncio.TaskGroup() as tg:
            tasks: set[asyncio.Task] = set()
            for partfile in parts:
                tasks.add(
                    tg.create_task(
                        self._write_file_chunk_to_target(partfile, outp)
                    )
                )
        self._end_stamp(event)

    async def _write_file_chunk_to_target(self, inp: Path, outp: Path) -> None:
        event = f"write chunk file {inp!s} to target {outp!s}"
        self._start_stamp(event)
        fname = inp.name
        parts = inp.name.split(".")
        if len(parts) < 3 or parts[-1] != "part":
            raise ValueError(f"{fname} doesn't look like a part file")
        first, last = map(int, parts[-2].split("-"))
        size = last - first + 1
        if size > self._chunk_size:
            self._logger.warning(
                f"[{self._pid}] Chunk size {size} > {self._chunk_size}"
            )
            size = self._chunk_size
        self._copy_file_part(inp, outp, size, offset=first)
        self._end_stamp(event)

    def _copy_file_part(
        self, inp: Path, outp: Path, read_size: int, offset: int
    ) -> None:
        """Copy a part file into the target file at a specific offset."""
        # Old-school!  Feels like I'm doing file I/O in C!
        event = (f"copy file part {inp!s} ({read_size} bytes) to"
                 f" {outp!s} at {offset})")
        self._start_stamp(event)
        with outp.open("ab") as outp_f:
            outp_f.seek(offset)
            data = inp.read_bytes()
            self._logger.debug(
                f"[{self._pid}] Read {len(data)} bytes from {inp!s}"
            )
            bw = outp_f.write(data)
            if bw < len(data):
                raise ValueError(f"Short write: {bw}/{len(data)} to {outp!s}")
        self._end_stamp(event)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pclient",
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


def main() -> None:
    """Download from a URL."""
    args = _parse_args()
    _ = ParallelClient(
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
    main()
