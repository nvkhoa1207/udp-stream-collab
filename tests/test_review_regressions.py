"""Kiểm tra các lỗi biên R1–R4 bằng server và client thật."""
import asyncio
import json
import unittest
from common.models import MessageType, Packet
from ui.runtime import ClientRuntime
from tests import test_team_e2e as e2e

until = e2e.until


class ReviewRegressionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = e2e.TeamE2ETests.asyncSetUp
    asyncTearDown = e2e.TeamE2ETests.asyncTearDown
    client = e2e.TeamE2ETests.client
    event = e2e.TeamE2ETests.event
    transfer = e2e.TeamE2ETests.transfer

    async def test_twenty_clients_all_pairs_repeat_and_file(self):
        names = ['hai', 'khoa', 'third'] + ['user' + str(i) for i in range(17)]
        for name in names[3:]:
            await asyncio.to_thread(self.app.auth.add_user, name, 'test-password')
        write = self.app.transport._write
        dropped = []
        def drop_last_bind(peer, packet):
            if packet.kind == MessageType.BIND_OK and packet.recipient == names[-1] and not dropped:
                dropped.append(packet)
                return
            write(peer, packet)
        self.app.transport._write = drop_last_bind
        clients = [await self.client(name) for name in names]
        await until(lambda: all(c.user_list.revision == 20 for c in clients))
        self.assertEqual(len(dropped), 1)
        original_write = clients[0].transport._write
        sent = []
        def capture(peer, packet):
            if packet.kind == MessageType.MSG:
                sent.append(packet)
            original_write(peer, packet)
        clients[0].transport._write = capture
        for round_number in range(2):
            for sender in clients:
                for receiver in clients:
                    if receiver is not sender:
                        await asyncio.wait_for(sender.send_chat(receiver.username, str(round_number)), 5)
            for receiver in clients:
                messages = [await self.event(receiver, 'message') for _ in range(19)]
                self.assertEqual({m['sender'] for m in messages}, set(names) - {receiver.username})
                self.assertTrue(all(m['recipient'] == receiver.username for m in messages))
                self.assertTrue(all(m['text'] == str(round_number) for m in messages))
            self.assertEqual(len(self.app.transport._rx), 40)
        original_write(clients[0].server_peer, sent[0])
        await asyncio.sleep(0.1)
        self.assertFalse(any(e.get('type') == 'message'
                             for client in clients for e in client.events._queue))
        await self.transfer(clients[0], clients[1], 1024, 'after-chat.bin')
        peer = clients[0].transport.local_address
        await clients[0].logout()
        await until(lambda: peer not in self.app.transport._peers)
        self.assertFalse(any(key[0] == peer for key in self.app.transport._rx))

    async def test_late_cancel_keeps_session_file_and_next_transfer(self):
        a, b = await self.client('hai'), await self.client('khoa')
        await self.transfer(a, b, 0, 'completed.bin')
        transfer_id = next(iter(b.file_offers.results))
        await until(lambda: (b.server_peer, transfer_id) in b.transport._tombstones)
        for _ in range(3):
            await a.transport.send(a.server_peer, Packet(
                kind=MessageType.FILE_CANCEL, sender='hai', recipient='khoa',
                stream_id='control', payload=json.dumps({
                    'transfer_id': transfer_id, 'reason': 'Late cancellation'
                }).encode()))
        await until(lambda: b._file_queue.empty())
        await asyncio.sleep(0.1)
        self.assertIsNotNone(b.session_id)
        self.assertTrue(all(not task.done() for task in b._session_tasks))
        self.assertEqual(len(list((self.root / 'khoa').glob('*_completed.bin'))), 1)
        await b.send_chat('hai', 'still connected')
        self.assertEqual((await self.event(a, 'message'))['text'], 'still connected')
        await self.transfer(a, b, 1024, 'after-cancel.bin')

    async def test_lost_bind_ok_preserves_online_list_without_membership_change(self):
        write = self.app.transport._write
        dropped = []
        def drop_first(peer, packet):
            if packet.kind == MessageType.BIND_OK and not dropped:
                dropped.append(packet)
                return
            write(peer, packet)
        self.app.transport._write = drop_first
        client = await self.client('hai')
        event = await self.event(client, 'users')
        self.assertEqual(event['users'], ['hai'])
        self.assertEqual(len(dropped), 1)
        self.assertIsNotNone(client.session_id)

    async def test_runtime_relogin_cleans_dead_session_and_transfers(self):
        runtime = ClientRuntime(self.root / 'hai')
        body = dict(host=self.app.address[0], port=self.app.address[1],
                    username='hai', password='test-password')
        try:
            await runtime.login(body)
            old = runtime.client
            old._session_tasks[0].cancel()
            await until(lambda: old.session_id is None)
            # Mô phỏng server đã thu hồi phiên sau khi client mất kết nối.
            session = next(s for s in self.app.sessions.active() if s.username == 'hai')
            self.app.sessions.remove(session.session_id)
            self.app.router.peer_removed(session)
            await runtime.login(body)
            self.assertIsNot(runtime.client, old)
            self.assertTrue(old.transport._closed)
            self.assertTrue(all(task.done() for task in old._session_tasks))
            other = await self.client('khoa')
            await runtime.client.send_chat('khoa', 'reconnected')
            self.assertEqual((await self.event(other, 'message'))['text'], 'reconnected')
            path = self.root / 'reconnect.bin'
            path.write_bytes(b'after reconnect')
            await runtime.client.send_file('khoa', path)
            self.assertEqual(next((self.root / 'khoa').glob('*_reconnect.bin')).read_bytes(),
                             b'after reconnect')
        finally:
            await runtime.close()

    async def test_heartbeat_timeout_then_runtime_login_on_new_server(self):
        from server.app import ServerApp
        runtime = ClientRuntime(self.root / 'hai')
        body = dict(host=self.app.address[0], port=self.app.address[1],
                    username='hai', password='test-password')
        try:
            await runtime.login(body)
            old = runtime.client
            await self.app.close()
            await until(lambda: old.session_id is None, timeout=16)
            self.app = ServerApp(tcp_port=0, udp_port=0, auth=self.app.auth, enable_udp=True)
            await self.app.start()
            body['port'] = self.app.address[1]
            await runtime.login(body)
            self.assertIsNotNone(runtime.client.session_id)
            self.assertTrue(old.transport._closed)
            self.assertTrue(all(task.done() for task in old._session_tasks))
            other = await self.client('khoa')
            await runtime.client.send_chat('khoa', 'after heartbeat timeout')
            self.assertEqual((await self.event(other, 'message'))['text'], 'after heartbeat timeout')
        finally:
            await runtime.close()
