import asyncio
import unittest

from common.codec import encode_tcp_record, read_tcp_record
from common.config import MAX_TCP_RECORD_BYTES
from server.app import ServerApp


class HandshakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = ServerApp(
            tcp_port=0,
            handshake_timeout=0.5,
        )
        await self.app.start()
        self.clients = []

    async def asyncTearDown(self):
        for writer in self.clients:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

        await self.app.close()

    async def connect(self):
        reader, writer = await asyncio.open_connection(
            *self.app.address
        )
        self.clients.append(writer)
        return reader, writer

    async def handshake(self):
        reader, writer = await self.connect()

        writer.write(encode_tcp_record({
            "type": "HELLO",
            "version": 1,
            "client_name": "Hải",
        }))
        await writer.drain()

        response = await read_tcp_record(reader)

        self.assertEqual(response, {
            "type": "HELLO_ACK",
            "version": 1,
            "server_name": "UDP Stream Collab",
        })

        self.assertEqual(
            await asyncio.wait_for(reader.read(), timeout=2),
            b"",
        )

    async def test_handshake(self):
        await self.handshake()

    async def test_twenty_clients(self):
        await asyncio.gather(
            *(self.handshake() for _ in range(20))
        )

    async def test_bad_hello_does_not_stop_server(self):
        invalid_records = [
            {"type": "AUTH"},
            {
                "type": "HELLO",
                "version": True,
                "client_name": "client",
            },
            {
                "type": "HELLO",
                "version": 2,
                "client_name": "client",
            },
        ]

        for record in invalid_records:
            with self.subTest(record=record):
                reader, writer = await self.connect()
                writer.write(encode_tcp_record(record))
                await writer.drain()

                self.assertEqual(
                    await asyncio.wait_for(reader.read(), timeout=2),
                    b"",
                )

        await self.handshake()

    async def test_oversized_prefix(self):
        reader, writer = await self.connect()

        writer.write(
            (MAX_TCP_RECORD_BYTES + 1).to_bytes(4, "big")
        )
        await writer.drain()

        self.assertEqual(
            await asyncio.wait_for(reader.read(), timeout=2),
            b"",
        )

        await self.handshake()

    async def test_idle_client_timeout(self):
        reader, _ = await self.connect()

        self.assertEqual(
            await asyncio.wait_for(reader.read(), timeout=2),
            b"",
        )

        await self.handshake()

    async def test_partial_record_disconnect(self):
        _, writer = await self.connect()
        writer.write(b"\x00\x00")
        await writer.drain()

        writer.close()
        await writer.wait_closed()

        await self.handshake()

    async def test_close_cleans_pending_clients(self):
        reader, _ = await self.connect()

        await self.app.close()

        self.assertEqual(
            await asyncio.wait_for(reader.read(), timeout=2),
            b"",
        )
        self.assertFalse(self.app._tasks)
        self.assertFalse(self.app._writers)

        await self.app.close()


if __name__ == "__main__":
    unittest.main()