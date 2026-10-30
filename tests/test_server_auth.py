import asyncio
import unittest

from common.codec import encode_tcp_record, read_tcp_record
from server.app import ServerApp
from server.auth import AuthService
from server.sessions import SessionRegistry


class ServerAuthTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.auth = AuthService()
        cls.auth.add_user("hai", "Demo123")

    async def asyncSetUp(self):
        self.sessions = SessionRegistry()
        self.app = ServerApp(
            tcp_port=0,
            auth=self.auth,
            sessions=self.sessions,
            handshake_timeout=0.5,
        )
        self.clients = []
        await self.app.start()

    async def asyncTearDown(self):
        await self.app.close()

        for writer in self.clients:
            writer.close()

            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def connect(self):
        reader, writer = await asyncio.open_connection(
            *self.app.address
        )
        self.clients.append(writer)

        writer.write(encode_tcp_record({
            "type": "HELLO",
            "version": 1,
            "client_name": "test",
        }))
        await writer.drain()

        response = await read_tcp_record(reader)
        self.assertEqual(response["type"], "HELLO_ACK")

        return reader, writer

    async def login(self, record):
        reader, writer = await self.connect()

        writer.write(encode_tcp_record(record))
        await writer.drain()

        response = await read_tcp_record(reader)

        self.assertEqual(
            await asyncio.wait_for(reader.read(), 2),
            b"",
        )
        return response

    async def test_success_creates_pending_session(self):
        result = await self.login({
            "type": "AUTH",
            "username": "hai",
            "password": "Demo123",
        })

        self.assertEqual(result["type"], "AUTH_OK")
        self.assertEqual(result["udp_port"], 9001)

        session = self.sessions.get(result["session_id"])

        self.assertEqual(session.username, "hai")
        self.assertEqual(session.tcp_ip, "127.0.0.1")
        self.assertIsNone(session.peer)
        self.assertEqual(self.sessions.online_users(), [])

    async def test_wrong_password(self):
        result = await self.login({
            "type": "AUTH",
            "username": "hai",
            "password": "wrong",
        })

        self.assertEqual(
            result["code"], "INVALID_CREDENTIALS"
        )

    async def test_unknown_user(self):
        result = await self.login({
            "type": "AUTH",
            "username": "missing",
            "password": "Demo123",
        })

        self.assertEqual(
            result["code"], "INVALID_CREDENTIALS"
        )

    async def test_malformed_auth(self):
        result = await self.login({
            "type": "AUTH",
            "username": "hai",
        })

        self.assertEqual(result["code"], "BAD_AUTH")

    async def test_duplicate_login(self):
        record = {
            "type": "AUTH",
            "username": "hai",
            "password": "Demo123",
        }

        first = await self.login(record)
        second = await self.login(record)

        self.assertEqual(first["type"], "AUTH_OK")
        self.assertEqual(
            second["code"], "SESSION_UNAVAILABLE"
        )

    async def test_concurrent_duplicate_login(self):
        record = {
            "type": "AUTH",
            "username": "hai",
            "password": "Demo123",
        }

        results = await asyncio.gather(
            self.login(record),
            self.login(record),
        )

        self.assertEqual(
            sorted(result["type"] for result in results),
            ["AUTH_ERROR", "AUTH_OK"],
        )

    async def test_auth_timeout(self):
        reader, _ = await self.connect()

        self.assertEqual(
            await asyncio.wait_for(reader.read(), 2),
            b"",
        )

    async def test_relogin_after_remove(self):
        record = {
            "type": "AUTH",
            "username": "hai",
            "password": "Demo123",
        }

        first = await self.login(record)
        self.sessions.remove(first["session_id"])

        second = await self.login(record)

        self.assertEqual(second["type"], "AUTH_OK")
        self.assertNotEqual(
            first["session_id"],
            second["session_id"],
        )