"""Tùy chọn: mở TCP loopback thật; không gọi đây là kiểm thử UDP/LAN."""
import asyncio
import os
import unittest
from client.auth import authenticate, read_record, write_record


@unittest.skipUnless(os.environ.get('BTL_NETWORK_TEST') == '1', 'Bật BTL_NETWORK_TEST=1 để mở TCP loopback')
class TcpNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_success_and_rejection_on_real_tcp(self):
        clients = set()
        async def handle(reader, writer):
            task = asyncio.current_task()
            clients.add(task)
            try:
                hello = await read_record(reader)
                self.assertEqual(hello['type'], 'HELLO')
                await write_record(writer, dict(type='HELLO_ACK', version=1, server_name='test'))
                auth = await read_record(reader)
                if auth['password'] == 'test-only':
                    await write_record(writer, dict(type='AUTH_OK', session_id='a'*32, udp_port=9001))
                else:
                    await write_record(writer, dict(type='AUTH_ERROR', code='AUTH_ERROR', message='Rejected'))
            finally:
                writer.close()
                await writer.wait_closed()
                clients.discard(task)
        server = await asyncio.start_server(handle, '127.0.0.1', 0)
        peer = server.sockets[0].getsockname()[:2]
        try:
            self.assertEqual(await authenticate(peer, 'khoa', 'test-only'), (('127.0.0.1', 9001), 'a'*32))
            with self.assertRaises(PermissionError):
                await authenticate(peer, 'khoa', 'wrong')
        finally:
            server.close()
            await server.wait_closed()
            await asyncio.gather(*clients)
