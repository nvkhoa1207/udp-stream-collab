"""Real team client + server TCP/UDP; every output lives in a temporary directory."""
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
        await self.transfer(a, b, 1024, 'after-cancel.bin')

    async def test_one_mebibyte_real_file(self):
        # Match normal app execution; asyncio's per-callback debug instrumentation
        # drastically changes throughput of tens of thousands of UDP callbacks.
        asyncio.get_running_loop().set_debug(False)
        a, b = await self.client('hai'), await self.client('khoa')
        await self.transfer(a, b, 1024 * 1024, 'one-MiB.bin')
