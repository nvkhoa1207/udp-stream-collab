"""Bridge sở hữu một event loop và một phiên client, không chạy I/O HTTP trong loop."""
import asyncio
from collections import deque
from pathlib import Path
from client.service import ClientService
from client.files import FileReceiver
from transport.reliable_udp import ReliableUdpTransport
from client.file_cancel import FileTransferCancelled


class ClientRuntime:
    def __init__(self, directory, factory=None):
        self.directory = Path(directory)
        self.factory = factory or self.make_client
        self.client = None
        self.jobs = set()
        self.events = deque(maxlen=512)
        self.sequence = 0
        self.pump = None

    def make_client(self, host, port, username):
        return ClientService(ReliableUdpTransport(('0.0.0.0', 0)), (host, 9001), username,
                             tcp_peer=(host, port), file_receiver=FileReceiver(self.directory))

    def emit(self, event):
        self.sequence += 1
        self.events.append(dict(event, id=self.sequence))

    async def collect(self):
        while self.client is not None:
            client = self.client
            while not client.events.empty():
                self.emit(client.events.get_nowait())
            if hasattr(type(client.transport), 'notify_delivery'):
                client.transport.notify_delivery()
            for event in client.poll_file_updates():
                self.emit(event)
            await asyncio.sleep(0.1)
            if client.session_id is None:
                self.emit(dict(type='error', message='Phiên đã dừng; hãy đăng xuất rồi đăng nhập lại.'))
                return

    async def login(self, body):
        if self.client is not None:
            raise ValueError('Hãy đăng xuất phiên hiện tại trước')
        host, port, username = body['host'], body['port'], body['username']
        if type(host) is not str or not host or len(host) > 253 or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('Server không hợp lệ')
        # FileReceiver construction may create a directory on a slow filesystem.
        # Construct outside the core loop; asyncio primitives bind lazily at login.
        client = await asyncio.to_thread(self.factory, host, port, username)
        try:
            await client.login(body['password'])
        except BaseException:
            raise
        self.client = client
        self.emit(dict(type='connection', connected=True, username=username))
        self.pump = asyncio.create_task(self.collect())
        return dict(connected=True)

    def start_job(self, awaitable, path=None, owner=None):
        if len(self.jobs) >= 4:
            awaitable.close()
            if path is not None:
                path.unlink(missing_ok=True)
            raise ValueError('Client đang bận, hãy thử lại')
        async def run():
            try:
                await awaitable
            except asyncio.CancelledError:
                raise
            except FileTransferCancelled:
                pass  # The file snapshot already describes this requested cancellation.
            except Exception as error:
                self.emit(dict(type='error', message='Thao tác không hoàn tất: ' + type(error).__name__))
            finally:
                if path is not None:
                    await asyncio.to_thread(owner.cleanup) if owner is not None else await asyncio.to_thread(path.unlink, missing_ok=True)
        task = asyncio.create_task(run())
        self.jobs.add(task)
        def done(task):
            self.jobs.discard(task)
            if not task.cancelled():
                task.exception()
        task.add_done_callback(done)
        return dict(accepted=True)

    async def command(self, action, body):
        if action == 'login':
            return await self.login(body)
        if action == 'logout':
            await self.close()
            return dict(connected=False)
        if self.client is None or self.client.session_id is None:
            raise ValueError('Chưa kết nối')
        if action == 'messages':
            return self.start_job(self.client.send_chat(body['recipient'], body['text']))
        if action == 'cancel':
            method = self.client.cancel_receive_file if body['direction'] == 'receive' else self.client.cancel_file
            return self.start_job(method(body['transfer_id']))
        raise ValueError('Lệnh không hợp lệ')

    async def upload(self, recipient, path, owner=None):
        if self.client is None or self.client.session_id is None:
            raise ValueError('Chưa kết nối')
        return self.start_job(self.client.send_file(recipient, path), path, owner)

    async def poll(self, after):
        return dict(events=[e for e in self.events if e['id'] > after], cursor=self.sequence,
                    reset=bool(self.events and after < self.events[0]['id'] - 1),
                    connected=self.client is not None and self.client.session_id is not None)

    async def close(self):
        await asyncio.sleep(0)  # Cho job mới vào try/finally trước khi hủy.
        if self.pump is not None:
            self.pump.cancel()
            await asyncio.gather(self.pump, return_exceptions=True)
            self.pump = None
        jobs = list(self.jobs)
        for task in jobs:
            task.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        if self.client is not None:
            try:
                await self.client.logout()
            finally:
                for event in self.client.poll_file_updates():
                    self.emit(event)
                self.client = None
                self.emit(dict(type='connection', connected=False))
