"""TCP authentication and optional reliable UDP relay with bounded lifecycle."""

import asyncio
import logging

from common.codec import encode_tcp_record, read_tcp_record
from common.config import TCP_PORT, UDP_PORT
from server.auth import AuthService
from server.sessions import SessionRegistry
from common.models import MessageType, Packet
from transport.reliable_udp import ReliableUdpTransport
from server.router import Router


logger = logging.getLogger(__name__)


class ServerApp:
    def __init__(
        self,
        host: str = "127.0.0.1",
        tcp_port: int = TCP_PORT,
        handshake_timeout: float = 10.0,
        max_clients: int = 100,
        *,
        auth: AuthService | None = None,
        sessions: SessionRegistry | None = None,
        udp_port: int = UDP_PORT,
        enable_udp: bool = False,
    ) -> None:
        if type(tcp_port) is not int or not 0 <= tcp_port <= 65535:
            raise ValueError("Cổng TCP không hợp lệ")

        if handshake_timeout <= 0 or max_clients <= 0:
            raise ValueError("Timeout và giới hạn client phải dương")

        if type(udp_port) is not int or not 0 <= udp_port <= 65535:
            raise ValueError("Cổng UDP không hợp lệ")

        self.auth = auth if auth is not None else AuthService()
        self.sessions = sessions if sessions is not None else SessionRegistry()
        self.udp_port = udp_port
        self.transport = (ReliableUdpTransport((host, udp_port),
                          bootstrap=self._bind, authorize=self._authorize)
                          if enable_udp else None)

        self.host = host
        self.tcp_port = tcp_port
        self.handshake_timeout = handshake_timeout
        self.max_clients = max_clients

        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._maintenance_task = None
        self._udp_task = None
        self.router = Router(self.transport, self.sessions) if self.transport else None
        if self.transport is not None:
            self.transport.identity = 'server'

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None:
            raise RuntimeError("Server chưa được khởi động")

        host, port = self._server.sockets[0].getsockname()[:2]
        return host, port

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("Server đã được khởi động")

        if self.transport is not None:
            await self.transport.start()
            self.udp_port = self.transport.local_address[1]
        try:
            self._server = await asyncio.start_server(
                self._accept_client, self.host, self.tcp_port)
        except BaseException:
            if self.transport is not None:
                await self.transport.close()
            raise
        if self.transport is not None:
            self._udp_task = asyncio.create_task(self._udp_loop())
        self._maintenance_task = asyncio.create_task(self._maintenance())

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

            await self._authenticate(reader, writer)

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


    async def _authenticate(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        record = await read_tcp_record(reader, self.handshake_timeout)
        session_id = None
        delivered = False

        try:
            valid = (
                set(record) == {"type", "username", "password"}
                and record["type"] == "AUTH"
                and isinstance(record["username"], str)
                and isinstance(record["password"], str)
            )

            if not valid:
                response = {
                    "type": "AUTH_ERROR",
                    "code": "BAD_AUTH",
                    "message": "Record AUTH không hợp lệ",
                }

            elif not await self.auth.verify(
                record["username"], record["password"]
            ):
                response = {
                    "type": "AUTH_ERROR",
                    "code": "INVALID_CREDENTIALS",
                    "message": "Sai tài khoản hoặc mật khẩu",
                }

            else:
                peer = writer.get_extra_info("peername")

                try:
                    session_id = self.sessions.create(
                        record["username"], peer[0]
                    )
                except ValueError:
                    response = {
                        "type": "AUTH_ERROR",
                        "code": "SESSION_UNAVAILABLE",
                        "message": "Tài khoản đang sử dụng hoặc server đầy",
                    }
                else:
                    response = {
                        "type": "AUTH_OK",
                        "session_id": session_id,
                        "udp_port": self.udp_port,
                    }

            writer.write(encode_tcp_record(response))
            await asyncio.wait_for(
                writer.drain(),
                timeout=self.handshake_timeout,
            )
            delivered = True

        finally:
            if session_id is not None and not delivered:
                self.sessions.remove(session_id)


    async def close(self) -> None:
        if self._udp_task is not None:
            self._udp_task.cancel()
            await asyncio.gather(self._udp_task, return_exceptions=True)
            self._udp_task = None
        if self.router is not None:
            await self.router.close()
        if self._maintenance_task is not None:
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
            self._maintenance_task = None
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
        if self.transport is not None:
            await self.transport.close()
        self.sessions.clear()

    async def _maintenance(self) -> None:
        while True:
            await asyncio.sleep(0.1)
            for session in self.sessions.sweep():
                if self.transport is not None and session.peer is not None:
                    self.router.peer_removed(session)
            if self.router is not None:
                self.router.publish_users()
                self.router.expire()

    async def _udp_loop(self):
        while True:
            received = await self.transport.receive()
            await self.router.handle(received)

    def _bind(self, peer, packet):
        session = self.sessions.get(packet.session_id)
        if (packet.sender != session.username or packet.recipient != 'server'
                or packet.stream_id != 'control'):
            raise ValueError('Invalid BIND identity')
        self.sessions.bind_peer(packet.session_id, peer)
        try:
            self.transport.register_peer(peer, packet.session_id)
        except ValueError:
            self.sessions.remove(packet.session_id)
            raise
        self.transport._write(peer, Packet(kind=MessageType.BIND_OK,
            sender='server', recipient=session.username, stream_id='control',
            session_id=packet.session_id))
        self.router.publish_users()

    def _authorize(self, peer, packet):
        session = self.sessions.get(packet.session_id)
        if session.peer != peer or session.username != packet.sender:
            return False
        self.sessions.touch(packet.session_id)
        return True

    async def run(self) -> None:
        await self.start()

        try:
            await self._server.serve_forever()
        finally:
            await self.close()
