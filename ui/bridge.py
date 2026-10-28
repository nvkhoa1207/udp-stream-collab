"""HTTP localhost, stdlib, same-origin/token, request và upload hữu hạn."""
import asyncio
import json
import secrets
import tempfile
import threading
from concurrent.futures import TimeoutError as FutureTimeout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from ui.runtime import ClientRuntime

MAX_UPLOAD = 16 * 1024 * 1024


def make_handler(runtime, loop, token):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Không ghi password/session/token vào log.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def reply(self, status, value, kind='application/json'):
            data = json.dumps(value).encode() if kind == 'application/json' else value
            self.send_response(status)
            self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
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
                raise TimeoutError('Client quá hạn')

        def do_GET(self):
            try:
                url = urlsplit(self.path)
                if url.path in ('/', '/app.js', '/style.css'):
                    host = '127.0.0.1:' + str(self.server.server_port)
                    if self.headers.get('Host') != host:
                        self.reply(403, {'error': 'Host không hợp lệ'})
                        return
                    name = {'/': 'index.html', '/app.js': 'app.js', '/style.css': 'style.css'}[url.path]
                    data = (Path(__file__).parent / name).read_text().replace('{{TOKEN}}', token).encode()
                    self.reply(200, data, {'/': 'text/html; charset=utf-8', '/app.js': 'text/javascript', '/style.css': 'text/css'}[url.path])
                elif url.path == '/api/events' and self.allowed():
                    after = int(parse_qs(url.query).get('after', ['0'])[0])
                    if after < 0:
                        raise ValueError()
                    self.reply(200, self.call(runtime.poll(after)))
                else:
                    self.reply(403, {'error': 'Không được phép'})
            except (ValueError, KeyError):
                self.reply(400, {'error': 'Yêu cầu không hợp lệ'})

        def do_POST(self):
            temporary = None
            try:
                if not self.allowed():
                    self.reply(403, {'error': 'Origin/token không hợp lệ'})
                    return
                if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                    raise ValueError()
                size = int(self.headers['Content-Length'])
                url = urlsplit(self.path)
                if url.path == '/api/files':
                    if not 0 <= size <= MAX_UPLOAD:
                        raise ValueError('File tối đa 16 MiB')
                    query = parse_qs(url.query)
                    name, recipient = query['name'][0], query['recipient'][0]
                    if not name or Path(name).name != name or name in ('.', '..') or '\\' in name or len(name.encode()) > 128:
                        raise ValueError('Tên file không hợp lệ')
                    temporary = tempfile.TemporaryDirectory(prefix='btl-upload-')
                    path = Path(temporary.name) / name
                    with path.open('xb') as output:
                        remaining = size
                        while remaining:
                            data = self.rfile.read(min(65536, remaining))
                            if not data:
                                raise ConnectionError('Upload thiếu dữ liệu')
                            output.write(data)
                            remaining -= len(data)
                    result = self.call(runtime.upload(recipient, path, temporary))
                    temporary = None  # Background job sở hữu và dọn thư mục upload.
                else:
                    if not 0 <= size <= 8192 or self.headers.get_content_type() != 'application/json':
                        raise ValueError()
                    raw = self.rfile.read(size)
                    if len(raw) != size:
                        raise ValueError()
                    body = json.loads(raw)
                    if type(body) is not dict:
                        raise ValueError()
                    result = self.call(runtime.command(url.path.removeprefix('/api/'), body))
                self.reply(200, result)
            except NotImplementedError:
                self.reply(503, {'error': 'Transport/server của Hải chưa triển khai; chưa thể đăng nhập mạng thật.'})
            except PermissionError:
                self.reply(401, {'error': 'Đăng nhập bị từ chối'})
            except (ValueError, KeyError, TypeError, UnicodeError):
                self.reply(400, {'error': 'Dữ liệu không hợp lệ hoặc client đang bận'})
            except Exception:
                self.reply(503, {'error': 'Không hoàn tất thao tác; kiểm tra kết nối và trạng thái client'})
            finally:
                if temporary is not None:
                    temporary.cleanup()
    return Handler


def run_bridge(port=8000, directory=Path('data/received')):
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    runtime = ClientRuntime(directory)
    server = HTTPServer(('127.0.0.1', port), make_handler(runtime, loop, secrets.token_hex(32)))
    print(f'Mở http://127.0.0.1:{server.server_port} — file nhận: {Path(directory).resolve()}')
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
