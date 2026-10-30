import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock
from ui.runtime import ClientRuntime


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_file_error_updates_matching_card_without_global_banner(self):
        runtime = ClientRuntime(Path('.'))
        for transfer_id in ('a' * 32, 'b' * 32):
            runtime.emit(dict(type='file', direction='send', transfer_id=transfer_id,
                              status='sending', name=transfer_id + '.bin'))
        runtime.emit(dict(type='file_error', direction='send', transfer_id='a' * 32,
                          code='RELAY_FAILED', message='Relay timeout'))
        result = await runtime.poll(0)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['events'][-1]['transfer_id'], 'a' * 32)
        self.assertEqual(result['events'][-1]['status'], 'failed')
        self.assertEqual(result['events'][1]['status'], 'sending')
        runtime.emit(dict(type='file_error', direction='send', transfer_id='c' * 32,
                          message='Unknown old transfer'))
        self.assertIn('c' * 32, (await runtime.poll(0))['errors'][0]['message'])

    async def test_download_requires_completed_receive_and_keeps_duplicate_names(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime = ClientRuntime(Path(folder))
            for transfer_id, data in [('a' * 32, b'first'), ('b' * 32, b'second')]:
                (Path(folder) / (transfer_id + '_same.txt')).write_bytes(data)
                runtime.emit(dict(type='file', direction='receive', transfer_id=transfer_id,
                                  status='verifying', name='same.txt'))
                with self.assertRaises(FileNotFoundError):
                    await runtime.download(transfer_id)
                runtime.emit(dict(type='file', direction='receive', transfer_id=transfer_id,
                                  status='completed', name='same.txt'))
                self.assertEqual(await runtime.download(transfer_id), ('same.txt', data))
            self.assertEqual((await runtime.download('a' * 32))[1], b'first')
            for value in ('../same.txt', 'c' * 32):
                with self.assertRaises(FileNotFoundError):
                    await runtime.download(value)
            (Path(folder) / ('a' * 32 + '_same.txt')).unlink()
            with self.assertRaises(FileNotFoundError):
                await runtime.download('a' * 32)

    async def test_online_snapshot_is_empty_when_session_ends(self):
        runtime = ClientRuntime(Path('.'))
        runtime.client = AsyncMock(session_id='session', username='hai')
        runtime.emit(dict(type='users', users=['hai', 'khoa']))
        self.assertEqual((await runtime.poll(0))['users'], ['hai', 'khoa'])
        runtime.client.session_id = None
        self.assertEqual((await runtime.poll(0))['users'], [])

    async def test_dismiss_does_not_hide_new_errors_or_replay_old_ones(self):
        runtime = ClientRuntime(Path('.'))
        runtime.emit(dict(type='error', scope='session', message='Old session ended'))
        old_id = runtime.sequence
        runtime.emit(dict(type='error', scope='session', message='New session ended'))
        await runtime.command('dismiss-errors', {'ids': [old_id]})
        self.assertEqual((await runtime.poll(0))['errors'][0]['message'], 'New session ended')
        await runtime.command('dismiss-errors', {'ids': [runtime.sequence]})
        self.assertEqual((await runtime.poll(0))['errors'], [])
        self.assertEqual(len((await runtime.poll(0))['events']), 2)

    async def test_login_success_resolves_only_session_error(self):
        runtime = ClientRuntime(Path('.'), factory=lambda *args: AsyncMock())
        runtime.emit(dict(type='error', scope='session', message='Session ended'))
        runtime.emit(dict(type='error', message='Other failure'))
        await runtime.login(dict(host='localhost', port=9000, username='hai', password='test'))
        runtime.pump.cancel()
        await asyncio.gather(runtime.pump, return_exceptions=True)
        self.assertEqual([e['message'] for e in (await runtime.poll(0))['errors']], ['Other failure'])

    async def test_requested_file_cancel_uses_snapshot_without_generic_error(self):
        from client.file_cancel import FileTransferCancelled
        runtime = ClientRuntime(Path('.'))
        async def cancelled():
            raise FileTransferCancelled('test cancellation')
        runtime.start_job(cancelled())
        await asyncio.gather(*tuple(runtime.jobs))
        self.assertFalse(runtime.events)

    async def test_disconnected_operations_rejected_and_cursor_bounded(self):
        runtime = ClientRuntime(Path('.'))
        with self.assertRaises(ValueError):
            await runtime.command('messages', {'recipient':'hai','text':'hello'})
        for n in range(600):
            runtime.emit({'type':'message', 'text':str(n)})
        value = await runtime.poll(0)
        self.assertTrue(value['reset'])
        self.assertEqual(len(value['events']),512)
        self.assertEqual((await runtime.poll(600))['events'],[])

    async def test_upload_job_owns_and_removes_temp_directory(self):
        runtime = ClientRuntime(Path('.'))
        client = AsyncMock()
        client.session_id = 'session'
        entered, release = asyncio.Event(), asyncio.Event()
        async def send(recipient, path):
            self.assertEqual(path.read_bytes(), b'hello')
            entered.set()
            await release.wait()
        client.send_file.side_effect = send
        runtime.client = client
        owner = tempfile.TemporaryDirectory()
        path = Path(owner.name) / 'test.bin'
        path.write_bytes(b'hello')
        await runtime.upload('hai', path, owner)
        await entered.wait()
        self.assertTrue((await runtime.poll(0))['sending_file'])
        with self.assertRaisesRegex(ValueError, 'current file'):
            await runtime.upload('hai', path)
        self.assertEqual(client.send_file.call_count, 1)
        self.assertTrue(path.exists())
        jobs = list(runtime.jobs)
        for task in jobs:
            task.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        self.assertFalse(Path(owner.name).exists())
        self.assertFalse((await runtime.poll(0))['sending_file'])

    async def test_login_failure_does_not_create_connected_state(self):
        client = AsyncMock()
        client.login.side_effect = NotImplementedError()
        runtime = ClientRuntime(Path('.'), factory=lambda *args: client)
        with self.assertRaises(NotImplementedError):
            await runtime.login(dict(host='localhost',port=9000,username='khoa',password='secret'))
        self.assertFalse((await runtime.poll(0))['connected'])

    async def test_runtime_upload_through_two_real_client_services_with_mock_transport(self):
        from client.service import ClientService
        from client.files import FileReceiver
        from mocks.memory_transport import MemoryHub, MemoryTransport
        hub = MemoryHub()
        server = MemoryTransport(hub, ('server', 1))
        local = MemoryTransport(hub, ('khoa', 2))
        remote = MemoryTransport(hub, ('hai', 3))
        for transport in (server, local, remote):
            await transport.start()
        with tempfile.TemporaryDirectory() as folder:
            receiver = FileReceiver(Path(folder))
            khoa = ClientService(local, server.address, 'khoa')
            hai = ClientService(remote, server.address, 'hai', file_receiver=receiver)
            runtime = ClientRuntime(Path(folder))
            runtime.client = khoa
            khoa.session_id = 'mock-session'
            async def relay():
                while True:
                    packet = (await server.receive()).packet
                    await server.send(remote.address if packet.recipient == 'hai' else local.address, packet)
            tasks = [asyncio.create_task(khoa.receive_events()), asyncio.create_task(hai.receive_events()),
                     asyncio.create_task(relay()), asyncio.create_task(runtime.collect())]
            owner = tempfile.TemporaryDirectory()
            source = Path(owner.name) / 'upload.bin'
            source.write_bytes(b'abc' * 1000)
            try:
                await asyncio.sleep(0)
                await runtime.upload('hai', source, owner)
                await asyncio.wait_for(asyncio.gather(*list(runtime.jobs)), timeout=3)
                await asyncio.sleep(0.15)
                events = (await runtime.poll(0))['events']
                self.assertTrue(any(e.get('status') == 'completed' for e in events))
                files = list(Path(folder).iterdir())
                self.assertEqual(len(files), 1)
                self.assertEqual(files[0].read_bytes(), b'abc' * 1000)
                self.assertFalse(Path(owner.name).exists())
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                owner.cleanup()
        for transport in (local, remote, server):
            await transport.close()
