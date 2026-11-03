"""Kiểm tra phiên rồi chuyển gói tin qua hàng đợi riêng cho từng người nhận."""
import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field, replace

from common.models import MessageType, Packet
from common.errors import StreamClosedError

logger = logging.getLogger(__name__)


@dataclass
class Transfer:
    # 1. Thông tin hai đầu gửi - nhận
    sender: object       # Session của người gửi file
    receiver: object     # Session của người nhận file
    meta_id: str         # ID của gói FILE_META ban đầu (để đối chiếu khi báo lỗi)
    updated: float       # Mốc thời gian (timestamp) có gói tin mới nhất đi qua

    # 2. Theo dõi gói tin và cờ trạng thái
    message_ids: set = field(default_factory=set)  # Tập UUID các gói tin thuộc đợt truyền này
    failed: bool = False                           # Đánh dấu đợt truyền đã thất bại
    finished: bool = False                         # Đánh dấu đợt truyền đã kết thúc


class Router:
    def __init__(self, transport, sessions):
        # 1. Nhận và lưu các dịch vụ hạ tầng
        self.transport = transport      # Tầng vận chuyển Reliable UDP để gửi gói
        self.sessions = sessions        # Sổ đăng ký Session để tra cứu user online
        self.revision = 0               # Số phiên bản của danh sách online (tăng mỗi khi có đổi)
        self._online = ()               # Bộ nhớ đệm danh sách user đang online hiện tại

        # 2. Quản lý hàng đợi và tác vụ chạy ngầm
        self._queues = {}               # Từ điển chứa hàng đợi: (peer, stream_id) -> asyncio.Queue
        self._workers = {}              # Từ điển chứa task công nhân: (peer, stream_id) -> Task
        self.transfers = {}             # transfer_id -> đối tượng Transfer
        self._references = {}           # message_id -> transfer_id (để tìm ra file khi nhận gói ERROR)
        self.failures = 0               # Đếm tổng số lần chuyển tiếp thất bại
        self.closed = False             # Cờ đánh dấu Router đã tắt chưa
        self._users_dirty = set()       # Tập hợp các session_id cần nhận danh sách user mới

        # 3. Cài đặt cơ chế hãm áp (Backpressure) bảo vệ RAM server
        transport.delivery_ready = self.can_accept

    @staticmethod
    def _stream(packet):
        # 1. Tin nhắn chat thường: tách luồng riêng theo người gửi (chống Head-of-Line blocking)
        if packet.kind == MessageType.MSG:
            # Mỗi người gửi chỉ chiếm một luồng chat ở mỗi người nhận.
            return 'chat-' + hashlib.sha256(packet.sender.encode()).hexdigest()[:48]
        # 2. Giữ nguyên stream_id gốc đối với file hoặc control
        return packet.stream_id

    # Kiểm tra hàng đợi đích có còn chỗ không trước khi nhận tiếp gói tin (Backpressure)
    def can_accept(self, received):
        packet = received.packet
        # 1. Các gói ưu tiên cao luôn được chấp nhận
        if packet.kind in (MessageType.HEARTBEAT, MessageType.LOGOUT):
            return True
        # 2. Chat chung (*) không bị chặn bởi một người nhận chậm
        if packet.kind == MessageType.MSG and packet.recipient == '*':
            # Vẫn gửi tin chung cho người khác nếu có một hàng đợi đầy.
            # handle() sẽ báo RELAY_BUSY cho trường hợp bị đầy.
            return True
        # 3. Tìm danh sách người sẽ nhận gói tin này
        targets = [s for s in self.sessions.active() if
                   (packet.recipient == '*' and s.username != packet.sender)
                   or s.username == packet.recipient]
        # 4. Kiểm tra hàng đợi của người nhận có bị đầy không (Backpressure)
        for session in targets:
            queue = self._queues.get((session.peer, self._stream(packet)))
            if queue is not None and queue.full():
                return False
        return True

    # Đưa gói tin vào hàng đợi của người nhận và đảm bảo có worker xử lý
    def _enqueue(self, target, packet, origin=None):
        # 1. Kiểm tra router còn mở không
        if self.closed:
            return False
        # 2. Tìm hoặc tạo mới hàng đợi cho stream này (tối đa 16 gói)
        key = (target.peer, packet.stream_id)
        queue = self._queues.get(key)
        if queue is None:
            # Giới hạn tối đa 256 hàng đợi trên toàn server để chống cạn RAM
            if len(self._queues) >= 256:
                return False
            queue = self._queues[key] = asyncio.Queue(maxsize=16)
        # 3. Đưa gói tin vào hàng đợi
        try:
            queue.put_nowait((target, packet, origin, self.transfers.get(packet.stream_id)))
        except asyncio.QueueFull:
            return False
        # 4. Đảm bảo có công nhân (_worker) đứng kéo hàng đợi này
        if key not in self._workers:
            self._workers[key] = asyncio.create_task(self._worker(key, queue))
        return True

    # Task ngầm rút gói tin từ hàng đợi và chuyển tiếp ra mạng
    async def _worker(self, key, queue):
        pending = set()  # Tập hợp các task đang trong quá trình bay qua mạng (_forward)
        try:
            # 1. Lấy tối đa 8 gói cùng lúc ra gửi (Pipelining) để mạng đạt tốc độ tối đa
            while not queue.empty() or pending:
                while not queue.empty() and len(pending) < 8:
                    item = queue.get_nowait()
                    queue.task_done()
                    pending.add(asyncio.create_task(self._forward(*item)))
                    self.transport.notify_delivery()  # Báo transport biết queue vừa trống chỗ
                # Đợi cho ít nhất 1 gói gửi xong rồi mới quay lại lấy tiếp
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        await task
        # 2. Dọn dẹp khi hàng đợi rỗng và worker kết thúc
        finally:
            for task in pending:
                task.cancel()  # Hủy các task còn sót
            await asyncio.gather(*pending, return_exceptions=True)
            self._queues.pop(key, None)
            self._workers.pop(key, None)
            self.transport.notify_delivery()

    # Chuyển tiếp 1 gói tin sang máy đích và cập nhật trạng thái truyền
    async def _forward(self, target, packet, origin, transfer=None):
        # 1. Kiểm tra lượt truyền đã xong chưa
        if transfer is not None and transfer.finished:
            return  # Gói còn trong hàng đợi của lượt đã đóng không cần gửi nữa.
        try:
            # 2. Kiểm tra người nhận còn online không
            current = self.sessions.get(target.session_id)
            if current.peer != target.peer:
                raise ConnectionError('Destination offline')
            # 3. Gửi tin cậy qua UDP (chờ tối đa 45 giây)
            await asyncio.wait_for(self.transport.send(target.peer, packet), 45)
            # 4. Cập nhật trạng thái nếu là các gói kết thúc file
            if packet.kind == MessageType.FILE_COMPLETE:
                self._finish(packet.stream_id, success=True)
            elif packet.kind == MessageType.FILE_CANCEL:
                self._finish(json.loads(packet.payload)['transfer_id'], success=False)
            elif packet.kind == MessageType.ERROR:
                transfer_id = self._references.get(json.loads(packet.payload).get('related_message_id'))
                if transfer_id:
                    self._finish(transfer_id, success=False)
        # 5. Xử lý sự cố mạng khi chuyển tiếp thất bại
        except (OSError, ValueError, TimeoutError) as error:
            if isinstance(error, StreamClosedError) and transfer is not None and transfer.finished:
                return  # Đóng lượt chủ động đã đánh thức pending send, không phải lỗi mạng.
            if transfer is not None and transfer.failed:
                return  # Lỗi gốc của lượt này đã được báo trước khi dọn các gói còn lại.
            self.failures += 1
            if origin is not None:
                # Gửi gói báo lỗi ngược lại cho người gửi ban đầu
                self._error(origin, packet, 'RELAY_FAILED', 'Destination unavailable or relay timed out')
                self._abort_transfer(packet.stream_id, 'File relay failed')

    # Tạo gói tin ERROR gửi cho target
    def _error(self, target, packet, code, message):
        reply = Packet(kind=MessageType.ERROR, sender='server', recipient=target.username,
                       stream_id='control', payload=json.dumps(dict(code=code,
                       message=message, related_message_id=packet.message_id)).encode())
        self._enqueue(target, reply)

    # Đóng hoàn tất một đợt truyền file (thành công hoặc thất bại)
    def _finish(self, transfer_id, *, success):
        transfer = self.transfers.pop(transfer_id, None)
        if transfer is not None:
            transfer.finished = True
            for mid in transfer.message_ids:
                self._references.pop(mid, None)
            for session in (transfer.sender, transfer.receiver):
                self.transport.finish_stream(session.peer, transfer_id, success=success)

    # Hủy khẩn cấp một đợt truyền file (gửi FILE_CANCEL cho cả 2 bên)
    def _abort_transfer(self, transfer_id, reason):
        transfer = self.transfers.get(transfer_id)
        if transfer is not None:
            transfer.failed = True
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

    # Phát sóng danh sách user online cho các client theo từng trang nhỏ
    def publish_users(self):
        # 1. Kiểm tra danh sách online có thay đổi không
        active = self.sessions.active()
        names = tuple(sorted(s.username for s in active))
        if names == self._online and not self._users_dirty:
            return
        if names != self._online:
            self._online = names
            self.revision += 1
            self._users_dirty = {s.session_id for s in active}
        # 2. Cắt nhỏ danh sách thành các trang 8 người (tránh vượt MTU 1200B)
        # Mỗi trang tối đa 8 tên, mỗi tên tối đa 32 byte để không vượt cỡ gói tin.
        pages = [list(names[i:i+8]) for i in range(0, len(names), 8)] or [[]]
        # 3. Gửi từng trang cho các user đang cần cập nhật
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

    # Cửa ngõ chính nhận và điều phối gói tin UDP từ tầng transport
    async def handle(self, received):
        packet = received.packet
        # A. Xác thực bảo mật danh tính gói tin đến
        try:
            source = self.sessions.get(packet.session_id)
            if source.peer != received.peer or source.username != packet.sender:
                return
        except ValueError:
            return
        # B. Xử lý gói Heartbeat và Đăng xuất (Logout)
        if packet.kind == MessageType.HEARTBEAT:
            self.transport._write(source.peer, Packet(kind=MessageType.HEARTBEAT, sender='server',
                recipient=source.username, stream_id='control', session_id=source.session_id))
            return
        if packet.kind == MessageType.LOGOUT:
            self.sessions.remove(source.session_id)
            self.peer_removed(source)
            logger.info("User %s logged out (UDP %s:%s)", source.username, source.peer[0], source.peer[1])
            self.publish_users()
            return
        # C. Chặn việc tự gửi tin hoặc file cho chính mình
        if packet.kind in (MessageType.MSG, MessageType.FILE_META) and packet.recipient == source.username:
            self._error(source, packet, 'SELF_SEND_DISABLED', 'Choose another recipient')
            return
        # D. Định tuyến tin nhắn Chat (MSG)
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
        # E. Định tuyến và kiểm soát hạn ngạch truyền File
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
            # E.1 Kiểm tra hạn ngạch khi bắt đầu file mới (FILE_META)
            if packet.kind == MessageType.FILE_META:
                if transfer is not None or len(self.transfers) >= 64:
                    raise ValueError()
                if sum(source.session_id in (t.sender.session_id, t.receiver.session_id)
                       for t in self.transfers.values()) >= 4:
                    raise ValueError()
                transfer = self.transfers[transfer_id] = Transfer(source, target,
                    packet.message_id, time.monotonic())
            # E.2 Kiểm tra tính hợp lệ và đúng chiều của gói tin file
            if transfer is not None:
                pair = (source.session_id, target.session_id)
                forward = (transfer.sender.session_id, transfer.receiver.session_id)
                reverse = forward[::-1]
                if pair not in (forward, reverse):
                    raise ValueError()
                # CHỐT BẢO MẬT: CHUNK và END bắt buộc phải đi từ Người Gửi -> Người Nhận
                if packet.kind in (MessageType.FILE_CHUNK, MessageType.FILE_END) and pair != forward:
                    raise ValueError()
                # CHỐT BẢO MẬT: ACCEPT và COMPLETE bắt buộc phải đi từ Người Nhận -> Người Gửi
                if packet.kind in (MessageType.FILE_ACCEPT, MessageType.FILE_COMPLETE) and pair != reverse:
                    raise ValueError()
                if len(transfer.message_ids) >= 65540:
                    raise ValueError()
                transfer.updated = time.monotonic()
                transfer.message_ids.add(packet.message_id)
                self._references[packet.message_id] = transfer_id
            elif packet.kind != MessageType.FILE_CANCEL:
                raise ValueError()
            # E.3 Đưa gói tin file vào hàng đợi chuyển tiếp sang người nhận
            if not self._enqueue(target, packet, source):
                self._error(source, packet, 'RELAY_BUSY', 'Relay queue quota reached')
                self._abort_transfer(transfer_id, 'Relay queue quota reached')
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            self._error(source, packet, 'BAD_FILE_STATE', 'Invalid file operation or quota reached')
            self.transport.finish_stream(source.peer, packet.stream_id, success=False)

    # Dọn dẹp tài nguyên và hủy các file transfer khi một client ngắt kết nối
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

    # Quét định kỳ hủy các file transfer bị treo quá 60s
    def expire(self):
        for transfer_id, transfer in list(self.transfers.items()):
            if time.monotonic() - transfer.updated >= 60:
                fake = Packet(kind=MessageType.FILE_META, sender='server', recipient=transfer.sender.username,
                    stream_id=transfer_id, message_id=transfer.meta_id)
                self._error(transfer.sender, fake, 'RELAY_TIMEOUT', 'File relay expired')
                self._abort_transfer(transfer_id, 'File relay expired')

    # Đóng toàn bộ router khi tắt Server
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
