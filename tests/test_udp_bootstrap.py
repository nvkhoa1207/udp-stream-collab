import asyncio
import unittest
from dataclasses import replace

from common.models import MessageType as M, Packet
from server.app import ServerApp
from server.sessions import SessionRegistry
from transport.reliable_udp import ReliableUdpTransport


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now = [0.0]
        self.sessions = SessionRegistry(clock=lambda: self.now[0])
        self.app = ServerApp(tcp_port=0, udp_port=0, enable_udp=True, sessions=self.sessions)
        await self.app.start()
        self.peer = self.app.transport.local_address
        self.client = ReliableUdpTransport(('127.0.0.1', 0))
        await self.client.start()
        self.sid = self.sessions.create('hai', '127.0.0.1')
        self.client.register_peer(self.peer, self.sid)
        self.bind = Packet(kind=M.BIND, sender='hai', recipient='server',
                           stream_id='control', session_id=self.sid)

    async def asyncTearDown(self):
        await self.client.close()
        await self.app.close()

    async def test_repeat_and_lost_reply(self):
        write = self.app.transport._write
        self.app.transport._write = lambda *args: None
        await self.client.send(self.peer, self.bind)
        await asyncio.sleep(0.02)
        self.app.transport._write = write
        for _ in range(2):
            await self.client.send(self.peer, self.bind)
            got = await asyncio.wait_for(self.client.receive(), 1)
            self.assertEqual(got.packet.kind, M.BIND_OK)
        self.assertEqual(self.sessions.online_users(), ['hai'])
        self.assertEqual(self.sessions.get(self.sid).peer, self.client.local_address)

    async def test_wrong_identity_endpoint_expiry_and_malformed(self):
        await self.client.send(self.peer, replace(self.bind, sender='khoa'))
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.client.receive(), 0.05)
        await self.client.send(self.peer, self.bind)
        await asyncio.wait_for(self.client.receive(), 1)
        other = ReliableUdpTransport(('127.0.0.1', 0))
        await other.start()
        other.register_peer(self.peer, self.sid)
        try:
            await other.send(self.peer, self.bind)
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(other.receive(), 0.05)
            self.client._socket.sendto(b'broken-json', self.peer)
            self.now[0] = 20
            await self.client.send(self.peer, self.bind)
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(self.client.receive(), 0.05)
            self.assertGreaterEqual(self.app.transport.rejected, 3)
        finally:
            await other.close()
