"""Kiểm tra phiên rồi chuyển gói tin qua hàng đợi riêng cho từng người nhận."""
import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass, field, replace

from common.models import MessageType, Packet


@dataclass
class Transfer:
    sender: object
    receiver: object
    meta_id: str
    updated: float
    message_ids: set = field(default_factory=set)
    failed: bool = False


class Router:
    def __init__(self, transport, sessions):
        self.transport = transport
        self.sessions = sessions
        self.revision = 0
        self._online = ()
        self._queues = {}
        self._workers = {}
        self.transfers = {}
        self._references = {}
        self.failures = 0
        self.closed = False
        self._users_dirty = set()
        transport.delivery_ready = self.can_accept

    @staticmethod
    def _stream(packet):
        if packet.kind == MessageType.MSG:
            # Mỗi người gửi chỉ chiếm một luồng chat ở mỗi người nhận.
            return 'chat-' + hashlib.sha256(packet.sender.encode()).hexdigest()[:48]
        return packet.stream_id

    def can_accept(self, received):
        packet = received.packet
        if packet.kind in (MessageType.HEARTBEAT, MessageType.LOGOUT):
            return True
        if packet.kind == MessageType.MSG and packet.recipient == '*':
            # Vẫn gửi tin chung cho người khác nếu có một hàng đợi đầy.
            # handle() sẽ báo RELAY_BUSY cho trường hợp bị đầy.
            return True
        targets = [s for s in self.sessions.active() if
                   (packet.recipient == '*' and s.username != packet.sender)
                   or s.username == packet.recipient]
        for session in targets:
            queue = self._queues.get((session.peer, self._stream(packet)))
            if queue is not None and queue.full():
                return False
        return True

    def _enqueue(self, target, packet, origin=None):
        if self.closed:
            return False
        key = (target.peer, packet.stream_id)
        queue = self._queues.get(key)
        if queue is None:
            if len(self._queues) >= 256:
                return False
            queue = self._queues[key] = asyncio.Queue(maxsize=16)
        try:
            queue.put_nowait((target, packet, origin))
        except asyncio.QueueFull:
            return False
        if key not in self._workers:
            self._workers[key] = asyncio.create_task(self._worker(key, queue))
        return True

    async def _worker(self, key, queue):
        pending = set()
        try:
            while not queue.empty() or pending:
                while not queue.empty() and len(pending) < 8:
                    item = queue.get_nowait()
                    queue.task_done()
                    pending.add(asyncio.create_task(self._forward(*item)))
                    self.transport.notify_delivery()
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        await task
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self._queues.pop(key, None)
            self._workers.pop(key, None)
            self.transport.notify_delivery()

    async def _forward(self, target, packet, origin):
        try:
            current = self.sessions.get(target.session_id)
            if current.peer != target.peer:
                raise ConnectionError('Destination offline')
            await asyncio.wait_for(self.transport.send(target.peer, packet), 45)
            if packet.kind == MessageType.FILE_COMPLETE:
                self._finish(packet.stream_id, success=True)
            elif packet.kind == MessageType.FILE_CANCEL:
                self._finish(json.loads(packet.payload)['transfer_id'], success=False)
            elif packet.kind == MessageType.ERROR:
                transfer_id = self._references.get(json.loads(packet.payload).get('related_message_id'))
                if transfer_id:
                    self._finish(transfer_id, success=False)
        except (OSError, ValueError, TimeoutError):
            self.failures += 1
            if origin is not None:
                self._error(origin, packet, 'RELAY_FAILED', 'Destination unavailable or relay timed out')
                self._abort_transfer(packet.stream_id, 'File relay failed')

    def _error(self, target, packet, code, message):
        reply = Packet(kind=MessageType.ERROR, sender='server', recipient=target.username,
                       stream_id='control', payload=json.dumps(dict(code=code,
                       message=message, related_message_id=packet.message_id)).encode())
        self._enqueue(target, reply)

    def _finish(self, transfer_id, *, success):
        transfer = self.transfers.pop(transfer_id, None)
        if transfer is not None:
            for mid in transfer.message_ids:
                self._references.pop(mid, None)
            for session in (transfer.sender, transfer.receiver):
                self.transport.finish_stream(session.peer, transfer_id, success=success)

    def _abort_transfer(self, transfer_id, reason):
        transfer = self.transfers.get(transfer_id)
        if transfer is not None:
            for target, other in ((transfer.sender, transfer.receiver),
                                  (transfer.receiver, transfer.sender)):
                try:
                    self.sessions.get(target.session_id)
                except ValueError:
                    continue
                self._enqueue(target, Packet(kind=MessageType.FILE_CANCEL, sender=other.username,
                    recipient=target.username, stream_id='control', payload=json.dumps(dict(
                    transfer_id=transfer_id, reason=reason)).encode()))
            self._finish(transfer_id, success=False)

    def publish_users(self):
        active = self.sessions.active()
        names = tuple(sorted(s.username for s in active))
        if names == self._online and not self._users_dirty:
            return
        if names != self._online:
            self._online = names
            self.revision += 1
            self._users_dirty = {s.session_id for s in active}
        # Mỗi trang tối đa 8 tên, mỗi tên tối đa 32 byte để không vượt cỡ gói tin.
        pages = [list(names[i:i+8]) for i in range(0, len(names), 8)] or [[]]
        for session in active:
            if session.session_id not in self._users_dirty:
                continue
            queue = self._queues.get((session.peer, 'control'))
            if queue is not None:
                keep = []
                while not queue.empty():
                    item = queue.get_nowait()
                    queue.task_done()
                    if item[1].kind != MessageType.USER_LIST:
                        keep.append(item)
                for item in keep:
                    queue.put_nowait(item)
                if queue.maxsize - queue.qsize() < len(pages):
                    continue
            all_sent = True
            for page, users in enumerate(pages):
                packet = Packet(kind=MessageType.USER_LIST, sender='server', recipient=session.username,
                    stream_id='control', payload=json.dumps(dict(revision=self.revision,
                    page=page, pages=len(pages), users=users), separators=(',', ':')).encode())
                if not self._enqueue(session, packet):
                    all_sent = False
            if all_sent:
                self._users_dirty.discard(session.session_id)
        self._users_dirty.intersection_update(s.session_id for s in active)

    async def handle(self, received):
        packet = received.packet
        try:
            source = self.sessions.get(packet.session_id)
            if source.peer != received.peer or source.username != packet.sender:
                return
        except ValueError:
            return
        if packet.kind == MessageType.HEARTBEAT:
            self.transport._write(source.peer, Packet(kind=MessageType.HEARTBEAT, sender='server',
                recipient=source.username, stream_id='control', session_id=source.session_id))
            return
        if packet.kind == MessageType.LOGOUT:
            self.sessions.remove(source.session_id)
            self.peer_removed(source)
            self.publish_users()
            return
        if packet.kind in (MessageType.MSG, MessageType.FILE_META) and packet.recipient == source.username:
            self._error(source, packet, 'SELF_SEND_DISABLED', 'Choose another recipient')
            return
        if packet.kind == MessageType.MSG:
            try:
                if not packet.payload.decode('utf-8').strip():
                    raise ValueError()
            except (UnicodeError, ValueError):
                self._error(source, packet, 'BAD_MESSAGE', 'Invalid UTF-8 or empty text')
                return
            targets = [s for s in self.sessions.active() if
                       (packet.recipient == '*' and s.username != source.username)
                       or s.username == packet.recipient]
            if not targets and packet.recipient != '*':
                self._error(source, packet, 'USER_OFFLINE', 'Recipient is offline')
            for target in targets:
                if not self._enqueue(target, replace(packet, stream_id=self._stream(packet)), source):
                    self._error(source, packet, 'RELAY_BUSY', 'Relay queue quota reached')
            return
        if packet.recipient == '*' or packet.kind not in (
                MessageType.FILE_META, MessageType.FILE_ACCEPT, MessageType.FILE_CHUNK, MessageType.FILE_END, MessageType.FILE_COMPLETE, MessageType.FILE_CANCEL, MessageType.ERROR):
            self._error(source, packet, 'BAD_REQUEST', 'Unsupported operation')
            return
        target = next((s for s in self.sessions.active() if s.username == packet.recipient), None)
        if target is None:
            self._error(source, packet, 'USER_OFFLINE', 'Recipient is offline')
            self._abort_transfer(packet.stream_id, 'Transfer peer is offline')
            self.transport.finish_stream(source.peer, packet.stream_id, success=False)
            return
        try:
            transfer_id = packet.stream_id
            if packet.kind == MessageType.FILE_CANCEL:
                body = json.loads(packet.payload)
                if (set(body) != {'transfer_id', 'reason'} or type(body['reason']) is not str
                        or not 1 <= len(body['reason']) <= 128 or packet.stream_id != 'control'):
                    raise ValueError()
                transfer_id = body['transfer_id']
            elif packet.kind == MessageType.ERROR:
                body = json.loads(packet.payload)
                if (set(body) != {'code', 'message', 'related_message_id'}
                        or any(type(v) is not str for v in body.values()) or packet.stream_id != 'control'):
                    raise ValueError()
                transfer_id = self._references.get(body['related_message_id'])
                if transfer_id is None:
                    # Lượt truyền có thể đã được dọn sau CANCEL hoặc COMPLETE.
                    # Bỏ lỗi đến muộn để hai bên không gửi ERROR qua lại mãi.
                    return
            if type(transfer_id) is not str or re.fullmatch('[0-9a-f]{32}', transfer_id) is None:
                raise ValueError()
            transfer = self.transfers.get(transfer_id)
            if packet.kind == MessageType.FILE_META:
                if transfer is not None or len(self.transfers) >= 64:
                    raise ValueError()
                if sum(source.session_id in (t.sender.session_id, t.receiver.session_id)
                       for t in self.transfers.values()) >= 4:
                    raise ValueError()
                transfer = self.transfers[transfer_id] = Transfer(source, target,
                    packet.message_id, time.monotonic())
            if transfer is not None:
                pair = (source.session_id, target.session_id)
                forward = (transfer.sender.session_id, transfer.receiver.session_id)
                reverse = forward[::-1]
                if pair not in (forward, reverse):
                    raise ValueError()
                if packet.kind in (MessageType.FILE_CHUNK, MessageType.FILE_END) and pair != forward:
                    raise ValueError()
                if packet.kind in (MessageType.FILE_ACCEPT, MessageType.FILE_COMPLETE) and pair != reverse:
                    raise ValueError()
                if len(transfer.message_ids) >= 65540:
                    raise ValueError()
                transfer.updated = time.monotonic()
                transfer.message_ids.add(packet.message_id)
                self._references[packet.message_id] = transfer_id
            elif packet.kind != MessageType.FILE_CANCEL:
                raise ValueError()
            if not self._enqueue(target, packet, source):
                self._error(source, packet, 'RELAY_BUSY', 'Relay queue quota reached')
                self._abort_transfer(transfer_id, 'Relay queue quota reached')
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            self._error(source, packet, 'BAD_FILE_STATE', 'Invalid file operation or quota reached')
            self.transport.finish_stream(source.peer, packet.stream_id, success=False)

    def peer_removed(self, session):
        self.transport.remove_peer(session.peer)
        for key, task in list(self._workers.items()):
            if key[0] == session.peer:
                task.cancel()
        for transfer_id, transfer in list(self.transfers.items()):
            if session.session_id in (transfer.sender.session_id, transfer.receiver.session_id):
                other = transfer.receiver if session.session_id == transfer.sender.session_id else transfer.sender
                fake = Packet(kind=MessageType.FILE_META, sender=session.username, recipient=other.username,
                              stream_id=transfer_id, message_id=transfer.meta_id)
                self._error(other, fake, 'USER_OFFLINE', 'Transfer peer disconnected')
                self._enqueue(other, Packet(kind=MessageType.FILE_CANCEL, sender=session.username,
                    recipient=other.username, stream_id='control', payload=json.dumps(dict(
                    transfer_id=transfer_id, reason='Transfer peer disconnected')).encode()))
                self._finish(transfer_id, success=False)

    def expire(self):
        for transfer_id, transfer in list(self.transfers.items()):
            if time.monotonic() - transfer.updated >= 60:
                fake = Packet(kind=MessageType.FILE_META, sender='server', recipient=transfer.sender.username,
                    stream_id=transfer_id, message_id=transfer.meta_id)
                self._error(transfer.sender, fake, 'RELAY_TIMEOUT', 'File relay expired')
                self._abort_transfer(transfer_id, 'File relay expired')

    async def close(self):
        self.closed = True
        tasks = tuple(self._workers.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._workers.clear()
        self._queues.clear()
        self.transfers.clear()
        self._references.clear()
        self._users_dirty.clear()
