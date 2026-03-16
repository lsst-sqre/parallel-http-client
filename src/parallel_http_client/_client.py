#!/usr/bin/env python3
"""Client for parallel fetches.

If parallelizing over subprocesses, each subprocess gets a contiguous region
of the target URL.  Each such region is chunked for download and divided among
worker threads using byte-range GET requests.
"""

import asyncio
import collections
import datetime
import logging
import os
import sys
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

    @override
    def __str__(self) -> str:
        retval = f"[{self.pid}] {self.time!s}"
        if self.duration:
            retval += f" (duration {self.duration})"
        retval += f" {self.name}"
        return retval


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
        first_s, last_s = inp.split("-")
        return cls(first=int(first_s), last=int(last_s))


class ParallelHTTPClient:
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
        self._timings: dict[str, _TimingEvent] = {}
        self._ordinal = 1
        self._chunks: collections.deque[_ByteRange] = collections.deque()
        self._worker_threads = 0
        self._lock.release()

        # Set up logging
        self._logger = logging.getLogger("ParallelClient")
        loglevel = logging.DEBUG if self._debug else logging.INFO
        logging.basicConfig()
        self._logger.setLevel(loglevel)

        self._me = Path(sys.argv[0]).resolve()

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
            self._timings = {}  # and timings
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
        if self._max_procs > 1 and size > self._chunk_size:
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
        timing_id = f"[{self._pid}] {event}"
        if timing_id in self._timings:
            ev = self._timings[timing_id]
            if ev.start:
                self._logger.warning(
                    f"{timing_id} start requested and already seen;"
                    " overwriting"
                )
            else:
                if ev.duration:
                    self._logger.warning(
                        f"{timing_id} stop requested and already seen;"
                        " overwriting"
                    )
                ev.duration = now - ev.start
        else:
            self._timings[timing_id] = _TimingEvent(
                start=start,
                ordinal=self._ordinal,
                name=event,
                time=now,
                pid=self._pid,
            )
            self._ordinal += 1
        self._lock.release()

    async def _generate_report(self) -> None:
        """Return event timings."""
        retval: list[str] = []
        self._lock.acquire()
        events = list(self._timings.values())
        events.sort(key=lambda x: x.time)
        for ev in events:
            r_str = str(ev) + "\n"
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

    async def _divide_subprocs(self, byte_range: _ByteRange) -> None:
        size = byte_range.last - byte_range.first + 1
        max_procs = self._max_procs
        total_chunks = int(size / self._chunk_size)
        if (size % self._chunk_size) != 0:
            total_chunks += 1
        max_procs = min(max_procs, total_chunks)
        if max_procs < 2:
            self._logger.warning(
                f"[{self._pid}] Cannot subdivide process any further."
            )
            await self._fetch_singleproc_parts(byte_range)
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
        event = f"fetch single process parts for {byte_range!s}"
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
        event = (
            f"copy file part {inp!s} ({read_size} bytes) to"
            f" {outp!s} at {offset})"
        )
        self._start_stamp(event)
        with outp.open("ab") as outp_f:
            outp_f.seek(offset)
            data = inp.read_bytes()
            self._logger.debug(
                f"[{self._pid}] Read {len(data)} bytes from {inp!s}"
            )
            bw = outp_f.write(data)
            if bw != len(data):
                raise ValueError(f"Bad write: {bw}/{len(data)} to {outp!s}")
        self._end_stamp(event)
