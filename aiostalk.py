import socket
from typing import Any, BinaryIO, Dict, Iterable, List, Optional, Tuple, Union
from asyncio import Lock, Queue, open_connection, get_running_loop, current_task
from greenstalk import *
from greenstalk import _parse_response, _parse_chunk, _parse_stats, _parse_list, _to_id



class Client:
    """A client implementing the beanstalk protocol. Upon creation a connection
    with beanstalkd is established and tubes are initialized.

    :param address: A socket address pair (host, port)
    :param encoding: The encoding used to encode and decode job bodies.
    :param use: The tube to use after connecting.
    :param watch: The tubes to watch after connecting. The ``default`` tube will
                  be ignored if it's not included.
    """

    def __init__(self,
                 address: Address,
                 encoding: Optional[str] = 'utf-8',
                 use: str = DEFAULT_TUBE,
                 watch: Union[str, Iterable[str]] = DEFAULT_TUBE) -> None:

        self.address = address
        self.encoding = encoding
        self.used_tube = use
        self.watched_tubes = watch

    async def __aenter__(self) -> 'Client':
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def connect(self):
        "connect to beanstalkd server, by default to standard port 11300"
        host, port = self.address
        self._reader, self._writer = await open_connection(host, port)

        if self.used_tube != DEFAULT_TUBE:
            await self.use(self.used_tube)

        if isinstance(self.watched_tubes, str):
            if self.watched_tubes != DEFAULT_TUBE:
                await self.watch(self.watched_tubes)
                await self.ignore(DEFAULT_TUBE)
        else:
            for tube in self.watched_tubes:
                await self.watch(tube)
            if DEFAULT_TUBE not in self.watched_tubes:
                await self.ignore(DEFAULT_TUBE)

    async def close(self) -> None:
        """Closes the connection to beanstalkd. The client instance should not
        be used after calling this method."""
        self._writer.close()
        await self._writer.wait_closed()

    async def _send_cmd(self, cmd: bytes, expected: bytes) -> List[bytes]:
        self._writer.write(cmd + b'\r\n')
        #self._reader.readuntil(separator=b'\r\n')
        line = await self._reader.readline()
        return _parse_response(line, expected)

    async def _read_chunk(self, size: int) -> bytes:
        data = await self._reader.readexactly(size + 2)
        return _parse_chunk(data, size)

    async def _int_cmd(self, cmd: bytes, expected: bytes) -> int:
        n, = await self._send_cmd(cmd, expected)
        return int(n)

    async def _job_cmd(self, cmd: bytes, expected: bytes) -> Job:
        id, size = (int(n) for n in await self._send_cmd(cmd, expected))
        chunk = await self._read_chunk(size)
        if self.encoding is None:
            body = chunk  # type: Body
        else:
            body = chunk.decode(self.encoding)
        return Job(id, body)

    async def _peek_cmd(self, cmd: bytes) -> Job:
        return await self._job_cmd(cmd, b'FOUND')

    async def _stats_cmd(self, cmd: bytes) -> Stats:
        size = await self._int_cmd(cmd, b'OK')
        chunk = await self._read_chunk(size)
        return _parse_stats(chunk)

    async def _list_cmd(self, cmd: bytes) -> List[str]:
        size = await self._int_cmd(cmd, b'OK')
        chunk = await self._read_chunk(size)
        return _parse_list(chunk)

    async def put(self,
            body: Body,
            priority: int = DEFAULT_PRIORITY,
            delay: int = DEFAULT_DELAY,
            ttr: int = DEFAULT_TTR) -> int:
        """Inserts a job into the currently used tube and returns the job ID.

        :param body: The data representing the job.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        :param ttr: The maximum number of seconds the job can be reserved for
                    before timing out.
        """
        if isinstance(body, str):
            if self.encoding is None:
                raise TypeError("Unable to encode string with no encoding set")
            body = body.encode(self.encoding)
        cmd = b'put %d %d %d %d\r\n%b' % (priority, delay, ttr, len(body), body)
        return await self._int_cmd(cmd, b'INSERTED')

    async def use(self, tube: str) -> None:
        """Changes the currently used tube.

        :param tube: The tube to use.
        """
        await self._send_cmd(b'use %b' % tube.encode('ascii'), b'USING')

    async def reserve(self, timeout: Optional[int] = None) -> Job:
        """Reserves a job from a tube on the watch list, giving this client
        exclusive access to it for the TTR. Returns the reserved job.

        This blocks until a job is reserved unless a ``timeout`` is given,
        which will raise a :class:`TimedOutError <greenstalk.TimedOutError>` if
        a job cannot be reserved within that time.

        :param timeout: The maximum number of seconds to wait.
        """
        if timeout is None:
            cmd = b'reserve'
        else:
            cmd = b'reserve-with-timeout %d' % timeout
        return await self._job_cmd(cmd, b'RESERVED')

    async def reserve_job(self, id: int) -> Job:
        """Reserves a job by ID, giving this client exclusive access to it for
        the TTR. Returns the reserved job.

        A :class:`NotFoundError <greenstalk.NotFoundError>` is raised if a job
        with the specified ID could not be reserved.

        :param id: The ID of the job to reserve.
        """
        return await self._job_cmd(b'reserve-job %d' % id, b'RESERVED')

    async def delete(self, job: JobOrID) -> None:
        """Deletes a job.

        :param job: The job or job ID to delete.
        """
        await self._send_cmd(b'delete %d' % _to_id(job), b'DELETED')

    async def release(self,
                job: Job,
                priority: int = DEFAULT_PRIORITY,
                delay: int = DEFAULT_DELAY) -> None:
        """Releases a reserved job.

        :param job: The job to release.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        """
        await self._send_cmd(b'release %d %d %d' % (job.id, priority, delay), b'RELEASED')

    async def bury(self, job: Job, priority: int = DEFAULT_PRIORITY) -> None:
        """Buries a reserved job.

        :param job: The job to bury.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        """
        await self._send_cmd(b'bury %d %d' % (job.id, priority), b'BURIED')

    async def touch(self, job: Job) -> None:
        """Refreshes the TTR of a reserved job.

        :param job: The job to touch.
        """
        await self._send_cmd(b'touch %d' % job.id, b'TOUCHED')

    async def watch(self, tube: str) -> int:
        """Adds a tube to the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to watch.
        """
        return await self._int_cmd(b'watch %b' % tube.encode('ascii'), b'WATCHING')

    async def ignore(self, tube: str) -> int:
        """Removes a tube from the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to ignore.
        """
        return await self._int_cmd(b'ignore %b' % tube.encode('ascii'), b'WATCHING')

    async def peek(self, id: int) -> Job:
        """Returns a job by ID.

        :param id: The ID of the job to peek.
        """
        return await self._peek_cmd(b'peek %d' % id)

    async def peek_ready(self) -> Job:
        """Returns the next ready job in the currently used tube."""
        return await self._peek_cmd(b'peek-ready')

    async def peek_delayed(self) -> Job:
        """Returns the next available delayed job in the currently used tube."""
        return await self._peek_cmd(b'peek-delayed')

    async def peek_buried(self) -> Job:
        """Returns the oldest buried job in the currently used tube."""
        return await self._peek_cmd(b'peek-buried')

    async def kick(self, bound: int) -> int:
        """Moves delayed and buried jobs into the ready queue and returns the
        number of jobs effected.

        Only jobs from the currently used tube are moved.

        A kick will only move jobs in a single state. If there are any buried
        jobs, only those will be moved. Otherwise delayed jobs will be moved.

        :param bound: The maximum number of jobs to kick.
        """
        return await self._int_cmd(b'kick %d' % bound, b'KICKED')

    async def kick_job(self, job: JobOrID) -> None:
        """Moves a delayed or buried job into the ready queue.

        :param job: The job or job ID to kick.
        """
        self._send_cmd(b'kick-job %d' % _to_id(job), b'KICKED')

    async def stats_job(self, job: JobOrID) -> Stats:
        """Returns job statistics.

        :param job: The job or job ID to return statistics for.
        """
        return await self._stats_cmd(b'stats-job %d' % _to_id(job))

    async def stats_tube(self, tube: str) -> Stats:
        """Returns tube statistics.

        :param tube: The tube to return statistics for.
        """
        return await self._stats_cmd(b'stats-tube %b' % tube.encode('ascii'))

    async def stats(self) -> Stats:
        """Returns system statistics."""
        return await self._stats_cmd(b'stats')

    async def tubes(self) -> List[str]:
        """Returns a list of all existing tubes."""
        return await self._list_cmd(b'list-tubes')

    async def using(self) -> str:
        """Returns the tube currently being used by the client."""
        tube, = await self._send_cmd(b'list-tube-used', b'USING')
        return tube.decode('ascii')

    async def watching(self) -> List[str]:
        """Returns a list of tubes currently being watched by the client."""
        return await self._list_cmd(b'list-tubes-watched')

    async def pause_tube(self, tube: str, delay: int) -> None:
        """Prevents jobs from being reserved from a tube for a period of time.

        :param tube: The tube to pause.
        :param delay: The number of seconds to pause the tube for.
        """
        await self._send_cmd(b'pause-tube %b %d' % (tube.encode('ascii'), delay), b'PAUSED')

