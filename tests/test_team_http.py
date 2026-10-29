"""Thử hai HTTP bridge trên localhost với server TCP/UDP thật của nhóm."""
import asyncio
import http.client
import json
import threading
import unittest
from http.server import HTTPServer

from tests import test_team_e2e as e2e
from ui.bridge import make_handler
from ui.runtime import ClientRuntime


class TeamHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = e2e.TeamE2ETests.asyncSetUp
    asyncTearDown = e2e.TeamE2ETests.asyncTearDown

    async def test_two_http_bridges_real_login_chat_upload_poll_logout(self):
        loop = asyncio.get_running_loop()
        runtimes = [ClientRuntime(self.root / name) for name in ('hai', 'khoa')]
        servers = [HTTPServer(('127.0.0.1', 0), make_handler(runtime, loop, 'local-test-token'))
                   for runtime in runtimes]
        threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
        for thread in threads:
            thread.start()

        async def request(index, method, path, value=None, raw=None):
            def send():
                port = servers[index].server_port
                headers = {'X-Client-Token': 'local-test-token', 'Origin': f'http://127.0.0.1:{port}'}
                body = raw
                if value is not None:
                    body = json.dumps(value).encode()
                    headers['Content-Type'] = 'application/json'
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
                try:
                    connection.request(method, path, body=body, headers=headers)
                    response = connection.getresponse()
                    return response.status, response.read()
                finally:
                    connection.close()
            status, body = await asyncio.to_thread(send)
            self.assertEqual(status, 200, body)
            return json.loads(body) if path.startswith('/api/') else body
        try:
            html = await request(0, 'GET', '/')
            self.assertIn(b'client-token', html)
            self.assertNotIn(b'{{TOKEN}}', html)
            for index, name in enumerate(('hai', 'khoa')):
                result = await request(index, 'POST', '/api/login', dict(
                    host='127.0.0.1', port=self.app.address[1], username=name, password='test-password'))
                self.assertTrue(result['connected'])
            await request(0, 'POST', '/api/messages', dict(recipient='khoa', text='HTTP to UDP'))
            await e2e.until(lambda: any(e.get('text') == 'HTTP to UDP' for e in runtimes[1].events))
            data = b'\x00\xffbridge-data' * 100
            await request(0, 'POST', '/api/files?recipient=khoa&name=http.bin', raw=data)
            await e2e.until(lambda: any(e.get('direction') == 'send' and e.get('status') == 'completed'
                                     for e in runtimes[0].events), timeout=15)
            files = list((self.root / 'khoa').glob('*_http.bin'))
            self.assertEqual(len(files), 1)
            self.assertEqual(await asyncio.to_thread(files[0].read_bytes), data)
            events = await request(0, 'GET', '/api/events?after=0')
            self.assertTrue(events['connected'])
            completed = next(e for e in events['events'] if e.get('status') == 'completed')
            self.assertEqual(completed['name'], 'http.bin')
            self.assertFalse((await request(0, 'POST', '/api/logout', {}))['connected'])
        finally:
            for runtime in runtimes:
                await runtime.close()
            for server in servers:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
            for thread in threads:
                await asyncio.to_thread(thread.join, 2)
