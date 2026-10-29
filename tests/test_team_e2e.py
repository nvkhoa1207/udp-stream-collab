"""Thử client và server TCP/UDP thật; tất cả file đầu ra nằm trong thư mục tạm."""
import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path

from client.files import FileReceiver
from client.service import ClientService
from server.app import ServerApp
from server.auth import AuthService
from transport.reliable_udp import ReliableUdpTransport


async def until(predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


class TeamE2ETests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        auth = await asyncio.to_thread(AuthService)
        for name in ('hai', 'khoa', 'third'):
            await asyncio.to_thread(auth.add_user, name, 'test-password')
        self.app = ServerApp(tcp_port=0, udp_port=0, auth=auth, enable_udp=True)
        await self.app.start()
        self.clients = []

    async def client(self, name):
        client = ClientService(ReliableUdpTransport(('127.0.0.1', 0),
            retry_seconds=0.03, probe_seconds=0.03), self.app.transport.local_address,
            name, tcp_peer=self.app.address, file_receiver=FileReceiver(self.root / name))
        self.clients.append(client)
        await client.login('test-password')
        return client

    async def asyncTearDown(self):
        for client in self.clients:
            await client.logout()
        await self.app.close()
        self.assertFalse(self.app.router._workers)
        self.assertFalse(self.app.transport._pending)
        self.directory.cleanup()

    async def event(self, client, kind):
        async with asyncio.timeout(5):
            while True:
                event = await client.events.get()
                client.transport.notify_delivery()
                if event['type'] == kind:
                    return event

    async def test_online_private_broadcast_concurrent_senders_and_logout(self):
        a, b, c = [await self.client(name) for name in ('hai', 'khoa', 'third')]
        await until(lambda: b.user_list.revision >= 3)
        await asyncio.gather(a.send_chat('khoa', 'private'), c.send_chat('khoa', 'other'))
        events = [await self.event(b, 'message') for _ in range(2)]
        self.assertEqual({e['text'] for e in events}, {'private', 'other'})
        await a.send_chat('*', 'broadcast')
        self.assertEqual((await self.event(b, 'message'))['text'], 'broadcast')
        self.assertEqual((await self.event(c, 'message'))['text'], 'broadcast')
        self.assertFalse(any(e.get('text') == 'private' for e in list(c.events._queue)))
        await c.logout()
        await until(lambda: self.app.sessions.online_users() == ['hai', 'khoa'])
        await until(lambda: b.user_list.revision >= 4)

    async def transfer(self, a, b, size, name):
        path = self.root / name
        data = os.urandom(size)
        await asyncio.to_thread(path.write_bytes, data)
        start = time.monotonic()
        await asyncio.wait_for(a.send_file(b.username, path), 90)
        files = list((self.root / b.username).glob('*_' + name))
        self.assertEqual(len(files), 1)
        self.assertEqual(await asyncio.to_thread(files[0].read_bytes), data)
        self.assertFalse(list((self.root / b.username).glob('*.part')))
        self.assertTrue(any(e['status'] == 'completed' for e in a.poll_file_updates()))
        b.poll_file_updates()
        await until(lambda: not self.app.router.transfers)
        print(f'E2E {size} bytes: {time.monotonic()-start:.3f}s', flush=True)

    async def test_empty_binary_unicode_and_multichunk_files(self):
        a, b = await self.client('hai'), await self.client('khoa')
        for size, name in ((0, 'empty.bin'), (1024, 'tiếng_Việt.bin'), (32768, 'binary.bin')):
            await self.transfer(a, b, size, name)
        self.assertLessEqual(len(self.app.transport._rx), 2)

    async def test_chat_during_file_and_receiver_disconnect(self):
        a, b, c = [await self.client(name) for name in ('hai', 'khoa', 'third')]
        path = self.root / 'large.bin'
        await asyncio.to_thread(path.write_bytes, os.urandom(1024 * 1024))
        sending = asyncio.create_task(a.send_file('khoa', path))
        await until(lambda: bool(self.app.router.transfers))
        await c.send_chat('hai', 'during file')
        self.assertEqual((await self.event(a, 'message'))['text'], 'during file')
        await b.logout()
        with self.assertRaises(Exception):
            await asyncio.wait_for(sending, 8)
        await until(lambda: not self.app.router.transfers)
        self.assertFalse(list((self.root / 'khoa').glob('*.part')))

    async def test_sender_and_receiver_cancel_cleanup_and_next_transfer(self):
        a, b = await self.client('hai'), await self.client('khoa')
        path = self.root / 'cancel.bin'
        await asyncio.to_thread(path.write_bytes, os.urandom(131072))
        for receiver_cancel in (False, True):
            sending = asyncio.create_task(a.send_file('khoa', path))
            await until(lambda: bool(b.file_offers.owners))
            transfer_id = a._file_sender.transfer_id
            if receiver_cancel:
                self.assertTrue(await b.cancel_receive_file(transfer_id))
            else:
                self.assertTrue(await a.cancel_file(transfer_id))
            with self.assertRaises(Exception):
                await asyncio.wait_for(sending, 5)
            await until(lambda: not b.file_offers.receiver.transfers)
            await until(lambda: not self.app.router.transfers)
            a.poll_file_updates()
            b.poll_file_updates()
            self.assertFalse(list((self.root / 'khoa').glob('*.part')))
            await asyncio.sleep(0.03)
            self.assertFalse(any(e.get('message') == 'Invalid file operation or quota reached'
                                 for e in list(b.events._queue)))
        await self.transfer(a, b, 1024, 'after-cancel.bin')

    async def test_one_mebibyte_real_file(self):
        # Tắt chế độ debug để đo giống lúc chạy ứng dụng bình thường.
        # Debug từng lần nhận UDP làm kết quả tốc độ bị chậm đi nhiều.
        asyncio.get_running_loop().set_debug(False)
        a, b = await self.client('hai'), await self.client('khoa')
        await self.transfer(a, b, 1024 * 1024, 'one-MiB.bin')

    async def test_lost_complete_ack_and_late_terminal_data(self):
        a, b = await self.client('hai'), await self.client('khoa')
        write = self.app.transport._write
        dropped = []
        from common.models import MessageType as M
        def filtered(peer, packet):
            if packet.kind == M.ACK and peer == b.transport.local_address and packet.ack == 2 and not dropped:
                dropped.append(packet.stream_id)
                return
            write(peer, packet)
        self.app.transport._write = filtered
        await self.transfer(a, b, 0, 'lost-complete-ack.bin')
        await until(lambda: (self.app.transport.local_address, dropped[0]) in b.transport._tombstones)
        self.assertEqual(len(list((self.root / 'khoa').glob('*_lost-complete-ack.bin'))), 1)

    async def test_network_rejects_path_traversal_missing_chunks_and_bad_hash(self):
        import hashlib
        import json
        from uuid import uuid4
        from common.models import MessageType as M, Packet
        a, b = await self.client('hai'), await self.client('khoa')
        for case in ('path', 'missing', 'hash'):
            transfer_id = uuid4().hex
            metadata = dict(name='../escape.bin' if case == 'path' else case+'.bin',
                size=1, sha256=hashlib.sha256(b'y').hexdigest(), chunk_size=480, total_chunks=1)
            def packet(kind, payload=b'', chunk_index=None):
                return Packet(kind=kind, sender='hai', recipient='khoa', stream_id=transfer_id,
                              payload=payload, chunk_index=chunk_index)
            await a.transport.send(a.server_peer, packet(M.FILE_META, json.dumps(metadata).encode()))
            if case != 'path':
                await until(lambda: transfer_id in b.file_offers.receiver.transfers)
                if case == 'hash':
                    await a.transport.send(a.server_peer, packet(M.FILE_CHUNK, b'x', 0))
                await a.transport.send(a.server_peer, packet(M.FILE_END))
            self.assertEqual((await self.event(a, 'error'))['type'], 'error')
            await until(lambda: not self.app.router.transfers)
            self.assertFalse(list((self.root / 'khoa').glob('*.part')))
            self.assertFalse(list((self.root / 'khoa').glob('*_' + metadata['name'])))
        self.assertFalse((self.root / 'escape.bin').exists())

    async def test_periodic_expiry_cleans_disconnected_sender_and_notifies_receiver(self):
        now = [0.0]
        self.app.sessions._clock = lambda: now[0]
        a, b = await self.client('hai'), await self.client('khoa')
        path = self.root / 'disconnect.bin'
        await asyncio.to_thread(path.write_bytes, os.urandom(1024*1024))
        sending = asyncio.create_task(a.send_file('khoa', path))
        await until(lambda: bool(b.file_offers.receiver.transfers))
        await a.transport.close()  # Mất kết nối đột ngột, không gửi LOGOUT hay CANCEL.
        now[0] = 9
        self.app.sessions.touch(b.session_id)
        now[0] = 11
        with self.assertRaises(Exception):
            await asyncio.wait_for(sending, 5)
        await until(lambda: self.app.sessions.online_users() == ['khoa'])
        try:
            await until(lambda: not b.file_offers.receiver.transfers)
        except TimeoutError:
            key = (b.transport.local_address, 'control')
            remote = (self.app.transport.local_address, 'control')
            tx = self.app.transport._tx.get(key)
            rx = b.transport._rx.get(remote)
            self.fail(f'Cleanup stalled: server tx sequence={tx.sequence if tx else None}, '
                      f'client consumed={rx.consumed if rx else None}, '
                      f'file queue={b._file_queue.qsize()}, connected={b.session_id is not None}, '
                      f'worker={b._file_worker_running}, relay failures={self.app.router.failures}')
        self.assertFalse(list((self.root / 'khoa').glob('*.part')))

    async def test_wrong_udp_sender_session_endpoint_and_malformed_datagram(self):
        from dataclasses import replace
        from common.codec import encode_packet
        from common.models import MessageType as M, Packet
        a, b = await self.client('hai'), await self.client('khoa')
        forged = Packet(kind=M.MSG, sender='third', recipient='khoa',
                        stream_id='chat-forged', payload=b'forged')
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(a.transport.send(a.server_peer, forged), 0.1)
        packet = replace(forged, sender='hai', seq=1, session_id=a.session_id)
        b.transport._socket.sendto(encode_packet(packet), self.app.transport.local_address)
        a.transport._write(a.server_peer, replace(packet, session_id='b'*32))
        a.transport._socket.sendto(b'not-json', self.app.transport.local_address)
        a.transport._socket.sendto(b'x' * 1300, self.app.transport.local_address)
        await a.send_chat('khoa', 'legitimate')
        self.assertEqual((await self.event(b, 'message'))['text'], 'legitimate')
        self.assertGreaterEqual(self.app.transport.rejected, 4)

    async def test_full_broadcast_destination_does_not_block_other_recipients(self):
        from common.models import MessageType as M, Packet
        a, b, c = [await self.client(name) for name in ('hai', 'khoa', 'third')]
        packet = Packet(kind=M.MSG, sender='hai', recipient='*', stream_id='chat-*', payload=b'independent')
        # Giả lập một hàng đợi đầy để kết quả thử ổn định.
        # Gói đến người còn lại và phản hồi lỗi vẫn đi qua UDP thật.
        key = (b.transport.local_address, self.app.router._stream(packet))
        blocked = asyncio.Queue(maxsize=1)
        blocked.put_nowait(None)
        self.app.router._queues[key] = blocked
        await a.send_chat('*', 'independent')
        self.assertEqual((await self.event(c, 'message'))['text'], 'independent')
        self.assertEqual((await self.event(a, 'error'))['message'], 'Relay queue quota reached')
