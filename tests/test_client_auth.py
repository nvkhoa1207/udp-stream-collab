import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from client.auth import read_record, bind_session
from client.service import ClientService
from common.models import Packet, MessageType, ReceivedPacket


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_bind_preserves_early_chat_and_bounds_pending_packets(self):
        peer = ('127.0.0.1', 9001)
        chat = ReceivedPacket(peer, Packet(
            kind=MessageType.MSG, sender='hai', recipient='khoa',
            stream_id='chat-hai', session_id='a' * 32, payload=b'early chat'))
        bound = ReceivedPacket(peer, Packet(
            kind=MessageType.BIND_OK, sender='server', recipient='khoa',
            stream_id='control', session_id='a' * 32))
        transport = AsyncMock()
        transport.receive.side_effect = [chat, bound]
        self.assertEqual(await bind_session(transport, peer, 'khoa', 'a' * 32), [chat])
        transport.receive.side_effect = [chat] * 101
        with self.assertRaisesRegex(ValueError, 'buffer is full'):
            await bind_session(transport, peer, 'khoa', 'a' * 32)

    async def test_fragmented_and_truncated_records(self):
        body = json.dumps({'type': 'HELLO_ACK'}).encode()
        reader = asyncio.StreamReader()
        for byte in len(body).to_bytes(4, 'big') + body:
            reader.feed_data(bytes([byte]))
        self.assertEqual(await read_record(reader), {'type': 'HELLO_ACK'})
        reader = asyncio.StreamReader()
        reader.feed_data(b'\x00\x00')
        reader.feed_eof()
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_record(reader)

    async def test_rejects_oversized_record_before_body(self):
        reader = asyncio.StreamReader()
        reader.feed_data((65537).to_bytes(4, 'big'))
        with self.assertRaises(ValueError):
            await read_record(reader)

    async def test_bind_ignores_wrong_endpoint_and_session(self):
        from dataclasses import replace
        peer = ('127.0.0.1', 9001)
        packet = Packet(kind=MessageType.BIND_OK, sender='server', recipient='khoa',
                        stream_id='control', session_id='a' * 32)
        transport = AsyncMock()
        transport.receive.side_effect = [ReceivedPacket(('bad', 1), packet),
            ReceivedPacket(peer, replace(packet, session_id='b' * 32)), ReceivedPacket(peer, packet)]
        await bind_session(transport, peer, 'khoa', 'a' * 32)
        self.assertEqual(transport.receive.await_count, 3)

    async def test_login_failure_closes_and_disallows_chat(self):
        transport = AsyncMock()
        client = ClientService(transport, ('127.0.0.1', 9001), 'khoa', tcp_peer=('localhost', 9000))
        with patch('client.service.authenticate', AsyncMock(side_effect=PermissionError())):
            with self.assertRaises(PermissionError):
                await client.login('secret')
        transport.close.assert_awaited_once()
        with self.assertRaises(ConnectionError):
            await client.send_chat('*', 'hello')

    async def test_login_registers_before_bind_and_logout_closes(self):
        from unittest.mock import Mock
        transport = AsyncMock()
        transport.register_peer = Mock()
        transport.receive.side_effect = lambda: None
        client = ClientService(transport, ('localhost', 9001), 'khoa', tcp_peer=('localhost', 9000))
        async def receive():
            await asyncio.Event().wait()
        client.receive_events = receive
        async def bind(*args):
            transport.register_peer.assert_called_once_with(('127.0.0.1', 9002), 'a' * 32)
            return []
        with patch('client.service.authenticate', AsyncMock(return_value=(('127.0.0.1', 9002), 'a' * 32))), patch('client.service.bind_session', bind):
            await client.login('secret')
            self.assertEqual(client.session_id, 'a' * 32)
            await client.logout()
        self.assertIsNone(client.session_id)
        transport.close.assert_awaited_once()
