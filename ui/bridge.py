"""Phục vụ giao diện trên localhost và kiểm tra các yêu cầu HTTP."""
import asyncio
import json
import secrets
import tempfile
import threading
from concurrent.futures import TimeoutError as FutureTimeout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs, quote
from ui.runtime import ClientRuntime

MAX_UPLOAD = 16 * 1024 * 1024
MAX_JSON_BODY = 8192
STATIC_FILES = {
    '/': ('index.html', 'text/html; charset=utf-8'),
    '/app.js': ('app.js', 'text/javascript'),
    '/style.css': ('style.css', 'text/css'),
}
CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
)


def make_handler(runtime, loop, token):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Không ghi password/session/token vào log.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def reply(self, status, value, kind='application/json', filename=None):
            data = json.dumps(value).encode() if kind == 'application/json' else value
            self.send_response(status)
            self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', CSP)
            if filename is not None:
                self.send_header('Content-Disposition', "attachment; filename*=UTF-8''" + quote(filename, safe=''))
            self.end_headers()
            self.wfile.write(data)

        def allowed(self):
            host = '127.0.0.1:' + str(self.server.server_port)
            if self.headers.get('Host') != host:
                return False
            if self.command == 'POST' and self.headers.get('Origin') != 'http://' + host:
                return False
            return secrets.compare_digest(self.headers.get('X-Client-Token', ''), token)

        def call(self, coroutine):
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            try:
                return future.result(timeout=25)
            except FutureTimeout:
                future.cancel()
                raise TimeoutError('Client operation timed out')

        def serve_static(self, path):
            host = '127.0.0.1:' + str(self.server.server_port)
            if self.headers.get('Host') != host:
                self.reply(403, {'error': 'Invalid host'})
                return
            name, content_type = STATIC_FILES[path]
            source = (Path(__file__).parent / name).read_text(encoding='utf-8')
            data = source.replace('{{TOKEN}}', token).encode('utf-8')
            self.reply(200, data, content_type)

        def read_json(self, size):
            if not 0 <= size <= MAX_JSON_BODY:
                raise ValueError()
            if self.headers.get_content_type() != 'application/json':
                raise ValueError()
            raw = self.rfile.read(size)
            if len(raw) != size:
                raise ValueError()
            body = json.loads(raw)
            if type(body) is not dict:
                raise ValueError()
            return body

        def read_upload(self, path, size):
            # Ghi từng phần vào file tạm, không giữ cả file trong RAM.
            with path.open('xb') as output:
                remaining = size
                while remaining:
                    data = self.rfile.read(min(65536, remaining))
                    if not data:
                        raise ConnectionError('Incomplete upload')
                    output.write(data)
                    remaining -= len(data)

        def do_GET(self):
            try:
                url = urlsplit(self.path)
                if url.path in STATIC_FILES:
                    self.serve_static(url.path)
                elif url.path == '/api/download' and self.allowed():
                    transfer_id = parse_qs(url.query)['id'][0]
                    name, data = self.call(runtime.download(transfer_id))
                    self.reply(200, data, 'application/octet-stream', filename=name)
                elif url.path == '/api/events' and self.allowed():
                    after = int(parse_qs(url.query).get('after', ['0'])[0])
                    if after < 0:
                        raise ValueError()
                    self.reply(200, self.call(runtime.poll(after)))
                else:
                    self.reply(403, {'error': 'Access denied'})
            except OSError:
                self.reply(404, {'error': 'Completed file is unavailable'})
            except (ValueError, KeyError):
                self.reply(400, {'error': 'Invalid request'})

        def do_POST(self):
            temporary = None
            try:
                if not self.allowed():
                    self.reply(403, {'error': 'Invalid origin or token'})
                    return
                if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                    raise ValueError()
                size = int(self.headers['Content-Length'])
                url = urlsplit(self.path)
                if url.path == '/api/files':
                    if not 0 <= size <= MAX_UPLOAD:
                        raise ValueError('Maximum file size is 16 MiB')
                    query = parse_qs(url.query)
                    name, recipient = query['name'][0], query['recipient'][0]
                    if (
                        not name or Path(name).name != name or name in ('.', '..')
                        or '\\' in name or len(name.encode()) > 128
                    ):
                        raise ValueError('Invalid file name')
                    temporary = tempfile.TemporaryDirectory(prefix='btl-upload-')
                    path = Path(temporary.name) / name
                    self.read_upload(path, size)
                    result = self.call(runtime.upload(recipient, path, temporary))
                    temporary = None  # Tác vụ gửi file sẽ dọn thư mục này khi xong.
                else:
                    body = self.read_json(size)
                    result = self.call(runtime.command(url.path.removeprefix('/api/'), body))
                self.reply(200, result)
            except NotImplementedError:
                self.reply(503, {'error': 'This operation is not supported by the configured transport or server.'})
            except PermissionError:
                self.reply(401, {'error': 'Login was rejected'})
            except (TimeoutError, ConnectionError, OSError):
                self.reply(503, {'error': 'Cannot reach the server; check the address and connection'})
            except (ValueError, KeyError, TypeError, UnicodeError):
                self.reply(400, {'error': 'Invalid data or client is busy'})
            except Exception:
                self.reply(503, {'error': 'Operation failed; check the connection and client status'})
            finally:
                if temporary is not None:
                    temporary.cleanup()
    return Handler


def run_bridge(port=8000, directory=Path('data/received')):
    loop = asyncio.new_event_loop()
    runtime = ClientRuntime(directory)
    try:
        server = HTTPServer(('127.0.0.1', port), make_handler(runtime, loop, secrets.token_hex(32)))
    except BaseException:
        loop.close()
        raise
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    print(f'Open http://127.0.0.1:{server.server_port} — receive folder: {Path(directory).resolve()}')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        try:
            asyncio.run_coroutine_threadsafe(runtime.close(), loop).result(timeout=15)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join()
            loop.close()
