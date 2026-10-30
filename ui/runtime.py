"""Nối các lệnh từ giao diện với một phiên client chạy bằng asyncio."""
import asyncio
import re
from collections import deque
from pathlib import Path
from uuid import uuid4
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
        self.errors = {}
        self.sending_file = False
        self.downloads = {}
        self.users = []
        self.account = None
        self.history_version = 0

    def make_client(self, host, port, username):
        transport = ReliableUdpTransport(('0.0.0.0', 0))
        receiver = FileReceiver(self.directory)
        return ClientService(
            transport, (host, 9001), username,
            tcp_peer=(host, port), file_receiver=receiver,
        )

    def emit(self, event):
        if event['type'] == 'error' and event.get('related_message_id'):
            if self.fail_message(event['related_message_id'], event['message']):
                return
        if event['type'] == 'file_error':
            previous = next((item for item in reversed(self.events) if item['type'] == 'file'
                and item['transfer_id'] == event['transfer_id']
                and item['direction'] == event['direction']), None)
            if previous is None:
                event = dict(type='error', scope='file-operation',
                    message='File ' + event['transfer_id'] + ': ' + event['message'])
            else:
                # File đã lưu xong vẫn giữ Completed; lỗi xác nhận nằm trên chính thẻ đó.
                status = 'completed' if previous['status'] == 'completed' else (
                    'cancelled' if event.get('code') == 'FILE_CANCELLED' else 'failed')
                event = dict(previous, status=status, message=event['message'])
        self.sequence += 1
        event = dict(event, id=self.sequence)
        self.events.append(event)
        if event['type'] == 'users':
            self.users = list(event['users'])
        if (event['type'] == 'file' and event.get('direction') == 'receive'
                and event.get('status') == 'completed'):
            transfer_id, name = event['transfer_id'], event.get('name', '')
            if (re.fullmatch('[0-9a-f]{32}', transfer_id) and name
                    and Path(name).name == name and '\\' not in name):
                self.downloads[transfer_id] = name
                while len(self.downloads) > 100:
                    del self.downloads[next(iter(self.downloads))]
        if event['type'] == 'error':
            self.errors[event.get('scope', 'operation')] = event

    def fail_message(self, message_id, reason):
        for event in self.events:
            if event['type'] == 'message' and event.get('outgoing') and event.get('message_id') == message_id:
                event.update(status='failed', reason=reason)
                self.emit(dict(type='message_status', message_id=message_id, status='failed', reason=reason))
                return True
        return False

    def send_message(self, body):
        client = self.client
        message_id = uuid4().hex
        recipient, text = body['recipient'], body['text']
        if type(recipient) is not str or type(text) is not str:
            raise ValueError('Invalid message')
        async def send():
            try:
                await client.send_chat(recipient, text, message_id=message_id)
            except asyncio.CancelledError:
                self.fail_message(message_id, 'Client stopped before sending finished')
                raise
            except Exception as error:
                if not self.fail_message(message_id, str(error) or type(error).__name__):
                    self.emit(dict(type='error', scope='chat-operation', message='Message failed: ' + str(error)))
        result = self.start_job(send())
        self.emit(dict(type='message', message_id=message_id, outgoing=True, status='requested',
                       sender=client.username, recipient=recipient, text=text))
        return result

    async def collect(self):
        while self.client is not None:
            client = self.client
            # Tạo thẻ trước khi xử lý lỗi tham chiếu tới lượt file vừa bắt đầu.
            for event in client.poll_file_updates():
                self.emit(event)
            while not client.events.empty():
                self.emit(client.events.get_nowait())
            if hasattr(type(client.transport), 'notify_delivery'):
                client.transport.notify_delivery()
            await asyncio.sleep(0.1)
            if client.session_id is None:
                self.emit(dict(type='error', scope='session', message='Session ended; log in again to reconnect.'))
                return

    async def login(self, body):
        # Phiên đã dừng vẫn còn socket/job cần dọn. Login chạy ngoài collector
        # nên close() có thể hủy và đợi collector an toàn.
        if self.client is not None and self.client.session_id is None:
            await self.close()
        if self.client is not None:
            raise ValueError('Log out of the current session first')
        host, port, username = body['host'], body['port'], body['username']
        if (
            type(host) is not str or not host or len(host) > 253
            or type(port) is not int or not 1 <= port <= 65535
        ):
            raise ValueError('Invalid server address or port')
        # Tạo thư mục nhận file trong thread để không chặn vòng xử lý mạng.
        client = await asyncio.to_thread(self.factory, host, port, username)
        await client.login(body['password'])
        if self.account is not None and self.account != username:
            # Đổi tài khoản thì bỏ lịch sử RAM và quyền tải cũ, không xóa file trên đĩa.
            self.events.clear()
            self.errors.clear()
            self.downloads.clear()
            self.history_version += 1
        self.account = username
        self.client = client
        self.users = []
        self.errors.pop('session', None)
        self.emit(dict(type='connection', connected=True, username=username))
        self.pump = asyncio.create_task(self.collect())
        return dict(connected=True)

    def start_job(self, awaitable, path=None, owner=None):
        file_count = getattr(self.client, '_sent_file_count', None)
        if len(self.jobs) >= 4:
            awaitable.close()
            if path is not None:
                path.unlink(missing_ok=True)
            raise ValueError('Client is busy; try again')
        async def run():
            try:
                await awaitable
            except asyncio.CancelledError:
                raise
            except FileTransferCancelled:
                pass  # Trạng thái file đã báo hủy, không cần thêm một lỗi chung.
            except Exception as error:
                if path is None:
                    self.emit(dict(type='error', message='Operation failed: ' + str(error)))
                elif getattr(self.client, '_sent_file_count', None) == file_count:
                    # Chưa tạo lượt truyền thì chưa có thẻ file để báo lỗi.
                    self.emit(dict(type='error', scope='upload',
                                   message='Could not start ' + path.name + ': ' + str(error)))
            finally:
                if path is not None:
                    self.sending_file = False
                    if owner is not None:
                        await asyncio.to_thread(owner.cleanup)
                    else:
                        await asyncio.to_thread(path.unlink, missing_ok=True)
        task = asyncio.create_task(run())
        self.jobs.add(task)
        def done(task):
            self.jobs.discard(task)
            if not task.cancelled():
                task.exception()
        task.add_done_callback(done)
        return dict(accepted=True)

    async def command(self, action, body):
        if action == 'dismiss-errors':
            ids = body['ids']
            if type(ids) is not list or any(type(value) is not int for value in ids):
                raise ValueError('Invalid error IDs')
            # Chỉ đóng các lỗi browser đã thấy, không che lỗi mới đến sau đó.
            self.errors = {key: event for key, event in self.errors.items() if event['id'] not in ids}
            return dict(accepted=True)
        if action == 'login':
            return await self.login(body)
        if action == 'logout':
            await self.close()
            return dict(connected=False)
        if self.client is None or self.client.session_id is None:
            raise ValueError('Not connected')
        if action == 'messages':
            return self.send_message(body)
        if action == 'cancel':
            if body['direction'] == 'receive':
                method = self.client.cancel_receive_file
            else:
                method = self.client.cancel_file
            return self.start_job(method(body['transfer_id']))
        raise ValueError('Invalid command')

    async def upload(self, recipient, path, owner=None):
        if self.client is None or self.client.session_id is None:
            raise ValueError('Not connected')
        if self.sending_file:
            raise ValueError('Wait for the current file to finish or cancel it first')
        self.sending_file = True
        try:
            return self.start_job(self.client.send_file(recipient, path), path, owner)
        except BaseException:
            self.sending_file = False
            raise

    async def poll(self, after):
        new_events = [event for event in self.events if event['id'] > after]
        history_expired = bool(self.events and after < self.events[0]['id'] - 1)
        connected = self.client is not None and self.client.session_id is not None
        return {
            'events': new_events,
            'cursor': self.sequence,
            'reset': history_expired,
            'connected': connected,
            'errors': list(self.errors.values()),
            'sending_file': self.sending_file,
            'downloads': list(self.downloads),
            'username': self.client.username if connected else '',
            'users': self.users if connected else [],
            'history_version': self.history_version,
            'oldest_id': self.events[0]['id'] if self.events else self.sequence + 1,
        }

    async def download(self, transfer_id):
        # Chỉ tải file đã nhận xong trong runtime này, không nhận đường dẫn từ browser.
        name = self.downloads.get(transfer_id)
        if name is None:
            raise FileNotFoundError('Completed file is unavailable')
        path = self.directory / (transfer_id + '_' + name)
        def read():
            if path.is_symlink() or path.resolve().parent != self.directory.resolve():
                raise FileNotFoundError('Completed file is unavailable')
            with path.open('rb') as file:
                data = file.read(16 * 1024 * 1024 + 1)
            if len(data) > 16 * 1024 * 1024:
                raise ValueError('File exceeds download limit')
            return name, data
        return await asyncio.to_thread(read)

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
