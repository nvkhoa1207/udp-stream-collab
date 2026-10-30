import asyncio
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from ui.bridge import make_handler
from ui.runtime import ClientRuntime


class HttpTests(unittest.TestCase):
    def request(self, method, path, headers='', body=b'', runtime=None):
        output = io.BytesIO()
        raw = f'{method} {path} HTTP/1.0\r\n{headers}\r\n'.encode() + body
        base = make_handler(runtime or ClientRuntime(Path('.')), None, 'token')
        class Handler(base):
            def setup(self):
                self.rfile = io.BytesIO(raw)
                self.wfile = output
            def finish(self):
                pass
            def call(self, coroutine):
                return asyncio.run(coroutine)
        Handler(None, ('127.0.0.1', 1234), SimpleNamespace(server_port=8000))
        return output.getvalue()

    def test_download_requires_token_and_returns_original_name_and_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime = ClientRuntime(Path(folder))
            transfer_id = 'a' * 32
            name = 'bài.txt'
            (Path(folder) / (transfer_id + '_' + name)).write_bytes(b'file contents')
            runtime.emit(dict(type='file', direction='receive', transfer_id=transfer_id,
                              status='completed', name=name))
            url = '/api/download?id=' + transfer_id
            headers = 'Host: 127.0.0.1:8000\r\n'
            self.assertIn(b'403', self.request('GET', url, headers, runtime=runtime).split(b'\r\n')[0])
            headers += 'X-Client-Token: token\r\n'
            response = self.request('GET', url, headers, runtime=runtime)
            self.assertIn(b'200 OK', response)
            self.assertIn(b"filename*=UTF-8''b%C3%A0i.txt", response)
            self.assertEqual(response.split(b'\r\n\r\n')[1], b'file contents')
            self.assertIn(b'404', self.request('GET', '/api/download?id=../../private', headers,
                                             runtime=runtime).split(b'\r\n')[0])

    def test_api_rejects_cross_origin_and_wrong_host_or_token(self):
        for extra in ('Host: evil.test\r\nX-Client-Token: token\r\n',
                      'Host: 127.0.0.1:8000\r\nX-Client-Token: wrong\r\n'):
            self.assertIn(b'403', self.request('GET','/api/events',extra).split(b'\r\n')[0])
        headers = 'Host: 127.0.0.1:8000\r\nX-Client-Token: token\r\nOrigin: http://evil.test\r\nContent-Length: 2\r\n'
        self.assertIn(b'403', self.request('POST','/api/logout',headers,b'{}').split(b'\r\n')[0])

    def test_events_and_upload_validation(self):
        headers = 'Host: 127.0.0.1:8000\r\nX-Client-Token: token\r\n'
        response = self.request('GET','/api/events?after=0',headers)
        self.assertIn(b'200 OK',response)
        self.assertIn(b'"connected": false',response)
        headers += 'Origin: http://127.0.0.1:8000\r\nContent-Length: 0\r\n'
        self.assertIn(b'400',self.request('POST','/api/files?name=../bad&recipient=hai',headers).split(b'\r\n')[0])
