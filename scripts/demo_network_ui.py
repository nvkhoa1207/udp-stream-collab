"""Mở server và hai client thật để thử giao diện trên localhost.

Tài khoản thử hai/khoa dùng mật khẩu mẫu 'test-password'.
Chỉ chạy trong môi trường thử; chạy bình thường bằng server.main.
"""
import argparse
import asyncio
import secrets
import tempfile
import threading
import time
from http.server import HTTPServer
from pathlib import Path

from server.app import ServerApp
from server.auth import AuthService
from ui.bridge import make_handler
from ui.runtime import ClientRuntime


async def run(seconds, slow_file_seconds=0, control_dir=None):
    auth = AuthService()
    for username in ('hai', 'khoa'):
        await asyncio.to_thread(auth.add_user, username, 'test-password')
    app = ServerApp(tcp_port=19000, udp_port=19001, auth=auth, enable_udp=True)
    await app.start()
    with tempfile.TemporaryDirectory(prefix='btl-browser-') as folder:
        loop = asyncio.get_running_loop()
        runtimes = [ClientRuntime(Path(folder) / name) for name in ('hai', 'khoa')]
        if slow_file_seconds or control_dir:
            from common.models import MessageType
            for runtime in runtimes:
                def factory(host, port, username, runtime=runtime):
                    client = runtime.make_client(host, port, username)
                    if control_dir:
                        finish = client.file_offers.receiver.finish
                        def slow_finish(transfer_id):
                            deadline = time.monotonic() + 20
                            while (control_dir / 'hold-verify').exists() and time.monotonic() < deadline:
                                time.sleep(0.1)
                            return finish(transfer_id)
                        client.file_offers.receiver.finish = slow_finish
                    send = client.transport.send
                    async def slow_send(peer, packet):
                        if packet.kind == MessageType.FILE_CHUNK:
                            await asyncio.sleep(slow_file_seconds)
                        if packet.kind == MessageType.FILE_COMPLETE and control_dir:
                            while (control_dir / 'hold-complete').exists():
                                await asyncio.sleep(0.1)
                        return await send(peer, packet)
                    client.transport.send = slow_send
                    return client
                runtime.factory = factory
        servers = [HTTPServer(('127.0.0.1', 18000+i), make_handler(runtime, loop, secrets.token_hex(32)))
                   for i, runtime in enumerate(runtimes)]
        threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
        for thread in threads:
            thread.start()
        print('Browser fixture ready: TCP 19000, UDP 19001, HTTP 18000/18001', flush=True)
        try:
            deadline = loop.time() + seconds
            running = True
            while loop.time() < deadline:
                if control_dir:
                    if (control_dir / 'stop-server').exists() and running:
                        await app.close()
                        running = False
                        print('Server stopped; browser clients remain running.', flush=True)
                    elif not (control_dir / 'stop-server').exists() and not running:
                        app = ServerApp(tcp_port=19000, udp_port=19001, auth=auth, enable_udp=True)
                        await app.start()
                        running = True
                        print('Server restarted.', flush=True)
                await asyncio.sleep(0.1)
        finally:
            for runtime in runtimes:
                await runtime.close()
            for server in servers:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
            for thread in threads:
                await asyncio.to_thread(thread.join, 2)
            await app.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local browser test: real server and two clients, using test-password.')
    parser.add_argument('--seconds', type=int, default=600)
    parser.add_argument('--slow-file-seconds', type=float, default=0)
    parser.add_argument('--control-dir', type=Path)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.seconds, args.slow_file_seconds, args.control_dir))
    except KeyboardInterrupt:
        pass
