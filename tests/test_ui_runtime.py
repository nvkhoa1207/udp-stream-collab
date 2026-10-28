import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock
from ui.runtime import ClientRuntime


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
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
        self.assertTrue(path.exists())
        jobs = list(runtime.jobs)
        for task in jobs:
            task.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        self.assertFalse(Path(owner.name).exists())

    async def test_login_failure_does_not_create_connected_state(self):
        client = AsyncMock()
        client.login.side_effect = NotImplementedError()
        runtime = ClientRuntime(Path('.'), factory=lambda *args: client)
        with self.assertRaises(NotImplementedError):
            await runtime.login(dict(host='localhost',port=9000,username='khoa',password='secret'))
        self.assertFalse((await runtime.poll(0))['connected'])
