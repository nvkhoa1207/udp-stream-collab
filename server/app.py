"""Server TCP handshake; đăng nhập và UDP sẽ bổ sung sau."""

import asyncio
import logging

from common.codec import encode_tcp_record, read_tcp_record
from common.config import TCP_PORT


logger = logging.getLogger(__name__)


class ServerApp:
    def __init__(
        self,
        host: str = "127.0.0.1",
        tcp_port: int = TCP_PORT,
        handshake_timeout: float = 10.0,
        max_clients: int = 100,
    ) -> None:
        if type(tcp_port) is not int or not 0 <= tcp_port <= 65535:
            raise ValueError("Cổng TCP không hợp lệ")

        if handshake_timeout <= 0 or max_clients <= 0:
            raise ValueError("Timeout và giới hạn client phải dương")

        self.host = host
        self.tcp_port = tcp_port
        self.handshake_timeout = handshake_timeout
        self.max_clients = max_clients

        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task] = set()
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None:
            raise RuntimeError("Server chưa được khởi động")

        host, port = self._server.sockets[0].getsockname()[:2]
        return host, port

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("Server đã được khởi động")

        self._server = await asyncio.start_server(
            self._accept_client,
            self.host,
            self.tcp_port,
        )

        logger.info(
            "TCP handshake server đang nghe tại %s:%s",
            *self.address,
        )

    def _accept_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        if len(self._tasks) >= self.max_clients:
            writer.close()
            return

        task = asyncio.create_task(
            self._handle_client(reader, writer)
        )

        self._tasks.add(task)
        self._writers.add(writer)

        task.add_done_callback(self._tasks.discard)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            record = await read_tcp_record(
                reader,
                self.handshake_timeout,
            )

            valid = (
                set(record) == {"type", "version", "client_name"}
                and record["type"] == "HELLO"
                and type(record["version"]) is int
                and record["version"] == 1
                and isinstance(record["client_name"], str)
                and 1 <= len(record["client_name"]) <= 64
                and record["client_name"].isprintable()
            )

            if not valid:
                raise ValueError("Record HELLO không hợp lệ")

            response = {
                "type": "HELLO_ACK",
                "version": 1,
                "server_name": "UDP Stream Collab",
            }

            writer.write(encode_tcp_record(response))

            await asyncio.wait_for(
                writer.drain(),
                timeout=self.handshake_timeout,
            )

            # A3 sẽ tiếp tục đọc AUTH tại đây.

        except (
            ValueError,
            asyncio.IncompleteReadError,
            TimeoutError,
            OSError,
        ):
            logger.debug("Handshake lỗi hoặc client đã ngắt kết nối")

        finally:
            writer.close()

            try:
                await asyncio.wait_for(
                    writer.wait_closed(),
                    timeout=1.0,
                )
            except (OSError, TimeoutError):
                pass

            self._writers.discard(writer)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for writer in tuple(self._writers):
            writer.close()

        tasks = tuple(self._tasks)

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        self._tasks.clear()
        self._writers.clear()

    async def run(self) -> None:
        await self.start()

        try:
            await self._server.serve_forever()
        finally:
            await self.close()