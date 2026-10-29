"""Xử lý đăng nhập, chat và truyền file ở phía client."""

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from common.config import MAX_PAYLOAD_BYTES
from common.contracts import Transport
from common.models import MessageType, Packet, Peer
from client.file_offers import FileOfferReceiver
from client.file_sender import FileSender
from client.file_updates import FileUpdates
from client.auth import authenticate, bind_session
from client.files import FileReceiver
from client.user_list import UserListAssembler


FILE_OFFER_QUEUE_SIZE = 8
FILE_REPLY_TIMEOUT_SECONDS = 10.0
LOCAL_CANCEL_TIMEOUT_SECONDS = 3.0


@dataclass
class ReceiveCancel:
    transfer_id: str
    result: asyncio.Future


class ClientService:
    def __init__(
        self, transport: Transport, server_peer: Peer, username: str, *,
        file_receiver: FileReceiver | None = None, tcp_peer: Peer | None = None,
    ) -> None:
        self.tcp_peer = tcp_peer
        self.session_id = None
        self._session_tasks = []
        self._login_attempted = False
        self.transport = transport
        self.server_peer = server_peer
        self.username = username
        self.events = asyncio.Queue(maxsize=100)
        self.file_updates = FileUpdates()
        self.user_list = UserListAssembler()
        self.file_offers = FileOfferReceiver(username, file_receiver, updates=self.file_updates)
        self._file_queue = asyncio.Queue(maxsize=FILE_OFFER_QUEUE_SIZE)
        self._receive_started = False
        self._receiving = False
        self._file_worker_running = False
        self._file_sender = None
        self._sent_file_count = 0
        if hasattr(type(transport), 'notify_delivery'):
            transport.identity = username
            transport.delivery_ready = self._can_receive

    def _can_receive(self, received):
        packet = received.packet
        if packet.kind in (MessageType.FILE_META, MessageType.FILE_CHUNK,
                           MessageType.FILE_END, MessageType.FILE_CANCEL):
            return not self._file_queue.full()
        if packet.kind in (MessageType.MSG, MessageType.USER_LIST):
            return not self.events.full()
        return True

    def _finish_stream(self, transfer_id, success):
        method = getattr(type(self.transport), 'finish_stream', None)
        if method is not None:
            method(self.transport, self.server_peer, transfer_id, success=success)

    async def login(self, password: str) -> None:
        if self.tcp_peer is None or self._login_attempted or self._receive_started:
            raise RuntimeError("Login requires a new client with tcp_peer configured")
        self._login_attempted = True
        try:
            await self.transport.start()
            self.server_peer, session = await authenticate(self.tcp_peer, self.username, password)
            self.transport.register_peer(self.server_peer, session)
            await bind_session(self.transport, self.server_peer, self.username, session)
            self.session_id = session
            self._session_tasks = [asyncio.create_task(self.receive_events()),
                                   asyncio.create_task(self._heartbeat())]
            for task in self._session_tasks:
                task.add_done_callback(self._session_task_done)
            await asyncio.sleep(0)
        except BaseException:
            try:
                await self.transport.close()
            except Exception:
                pass
            raise

    def _session_task_done(self, task):
        if not task.cancelled():
            task.exception()  # Lỗi được phản ánh bằng connected=False, không log bí mật.
        self.session_id = None
        for other in self._session_tasks:
            if other is not task and not other.done():
                other.cancel()

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(3)
            if (hasattr(type(self.transport), 'notify_delivery')
                    and asyncio.get_running_loop().time() - self.transport.last_received > 10):
                raise TimeoutError('Server heartbeat timed out')
            if hasattr(type(self.transport), 'notify_delivery'):
                self.transport.notify_delivery()
            await asyncio.wait_for(self.transport.send(self.server_peer, Packet(
                kind=MessageType.HEARTBEAT, sender=self.username, recipient="server",
                stream_id="control", session_id=self.session_id)), timeout=3)

    async def logout(self):
        if self._file_sender is not None:
            self._file_sender.request_cancel()
            await self._file_sender.finished.wait()
        try:
            if self.session_id is not None:
                await asyncio.wait_for(self.transport.send(self.server_peer, Packet(
                    kind=MessageType.LOGOUT, sender=self.username, recipient="server",
                    stream_id="control", session_id=self.session_id)), timeout=2)
        finally:
            self.session_id = None
            for task in self._session_tasks:
                task.cancel()
            await asyncio.gather(*self._session_tasks, return_exceptions=True)
            await self.transport.close()

    def _check_session(self):
        if self.tcp_peer is not None and self.session_id is None:
            raise ConnectionError("Client is not logged in and bound, or the session has ended")

    async def send_chat(self, recipient: str, text: str) -> None:
        self._check_session()
        recipient = recipient.strip()

        if not recipient or len(recipient) > 32:
            raise ValueError(
                "Recipient name must contain 1 to 32 characters"
            )

        if not text.strip():
            raise ValueError("Message must not be empty")

        payload = text.encode("utf-8")

        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"Message limit: {MAX_PAYLOAD_BYTES} byte"
            )

        packet = Packet(
            kind=MessageType.MSG,
            sender=self.username,
            recipient=recipient,
            stream_id=f"chat-{recipient}",
            payload=payload,
        )

        await self.transport.send(self.server_peer, packet)

    async def send_file(self, recipient: str, path: Path) -> None:
        """Đợi COMPLETE đúng kích thước và hash; báo lỗi nếu thất bại hoặc bị hủy."""
        self._check_session()
        if type(recipient) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,32}", recipient) is None:
            raise ValueError("Files must be sent to one valid username")
        if not self._receiving:
            raise RuntimeError("Start receive_events before sending a file")
        if self._file_sender is not None:
            raise RuntimeError("Each client can send one file at a time")
        if self._sent_file_count >= 4096:
            raise RuntimeError("Send quota reached; reconnect to continue")
        sender = FileSender(self.transport, self.server_peer, self.username, recipient,
                            self._file_io, self.file_updates)
        self.file_updates.begin(sender.transfer_id, recipient)
        self._file_sender = sender
        self._sent_file_count += 1
        try:
            await sender.run(Path(path))
        finally:
            self._finish_stream(sender.transfer_id, sender.completed.is_set())
            self._file_sender = None

    def poll_file_updates(self) -> list[dict]:
        """Lấy các trạng thái file mới nhất trên vòng xử lý của client."""
        return self.file_updates.poll()

    async def cancel_file(self, transfer_id: str) -> bool:
        sender = self._file_sender
        if sender is None or sender.transfer_id != transfer_id or sender.finished.is_set():
            return False
        sender.request_cancel()
        await sender.finished.wait()
        return True

    async def receive_events(self) -> None:
        """Chạy một lần mỗi ClientService/phiên; hủy task này để dọn file đang nhận."""
        if self._receive_started:
            raise RuntimeError("Only one receive_events loop is allowed per session")
        self._receive_started = True
        self._receiving = True
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._receive_file_offers())
                await self._receive_loop()
        finally:
            self._receiving = False
            if self._file_sender is not None:
                self._file_sender.fail(ConnectionError("Receive loop stopped during file transfer"))

    async def cancel_receive_file(self, transfer_id: str) -> bool:
        """Trả True khi đã xử lý yêu cầu; trạng thái file cho biết kết quả dọn và báo hủy."""
        if type(transfer_id) is not str or re.fullmatch(r"[0-9a-f]{32}", transfer_id) is None:
            raise ValueError("Invalid transfer ID")
        if not self._receiving or not self._file_worker_running:
            raise RuntimeError("Receive loop has not started or has stopped")
        result = asyncio.get_running_loop().create_future()
        try:
            self._file_queue.put_nowait(ReceiveCancel(transfer_id, result))
        except asyncio.QueueFull:
            raise RuntimeError("Receive worker is busy; retry cancellation") from None
        return await result

    async def _cancel_receive(self, request: ReceiveCancel):
        if request.result.cancelled():
            return  # Yêu cầu đã bị hủy trước khi được xử lý.
        reply = await self._file_io(self.file_offers.cancel_local, request.transfer_id)
        if reply is None:
            if not request.result.done():
                request.result.set_result(False)
            return
        delivery = "failed"
        try:
            await asyncio.wait_for(self.transport.send(self.server_peer, reply),
                                   timeout=LOCAL_CANCEL_TIMEOUT_SECONDS)
            delivery = "sent_to_server"
        except Exception:
            pass  # Trạng thái file sẽ báo lỗi gửi hủy. Các lượt nhận khác vẫn tiếp tục.
        finally:
            await self._file_io(self.file_offers.note_local_cancel, request.transfer_id, delivery)
            self._finish_stream(request.transfer_id, False)
        if not request.result.done():
            request.result.set_result(True)

    async def _file_io(self, function, *args):
        # Hủy tác vụ không tự dừng thread đang ghi đĩa. Đợi nó xong trước
        # khi dọn file, tránh tạo thêm .part sau khi đã dọn.
        work = asyncio.create_task(asyncio.to_thread(function, *args))
        try:
            return await asyncio.shield(work)
        except asyncio.CancelledError:
            await work
            raise

    async def _receive_file_offers(self) -> None:
        self._file_worker_running = True
        try:
            while True:
                await self._file_io(self.file_offers.expire)
                await self._send_file_notifications()
                try:
                    packet = await asyncio.wait_for(self._file_queue.get(), timeout=1)
                    if hasattr(type(self.transport), 'notify_delivery'):
                        self.transport.notify_delivery()
                except TimeoutError:
                    continue
                try:
                    if isinstance(packet, ReceiveCancel):
                        try:
                            await self._cancel_receive(packet)
                        finally:
                            if not packet.result.done():
                                packet.result.set_exception(ConnectionError("Worker stopped while processing cancellation"))
                        continue
                    reply = await self._file_io(self.file_offers.handle, packet)
                    await self._send_file_notifications()
                    if reply is not None:
                        delivered = False
                        try:
                            await asyncio.wait_for(
                                self.transport.send(self.server_peer, reply),
                                timeout=FILE_REPLY_TIMEOUT_SECONDS,
                            )
                            delivered = True
                        finally:
                            await self._file_io(self.file_offers.note_reply, reply, delivered)
                        if reply.kind == MessageType.FILE_COMPLETE:
                            self._finish_stream(reply.stream_id, delivered)
                        elif reply.kind == MessageType.ERROR:
                            transfer_id = packet.stream_id
                            if packet.kind == MessageType.FILE_CANCEL:
                                from client.file_cancel import cancelled_transfer_id
                                transfer_id = cancelled_transfer_id(packet)
                            if transfer_id:
                                self._finish_stream(transfer_id, False)
                finally:
                    self._file_queue.task_done()
        finally:
            self._file_worker_running = False
            # Báo lại lỗi gửi phản hồi hoặc dọn file cho nơi gọi.
            try:
                await self._file_io(self.file_offers.close)
            finally:
                while not self._file_queue.empty():
                    pending = self._file_queue.get_nowait()
                    if isinstance(pending, ReceiveCancel) and not pending.result.done():
                        pending.result.set_exception(ConnectionError("Worker stopped before processing cancellation"))
                    self._file_queue.task_done()

    async def _send_file_notifications(self) -> None:
        for reply in await self._file_io(self.file_offers.take_notifications):
            await asyncio.wait_for(self.transport.send(self.server_peer, reply),
                                   timeout=FILE_REPLY_TIMEOUT_SECONDS)
        for transfer_id, success in await self._file_io(self.file_offers.take_terminal_streams):
            self._finish_stream(transfer_id, success)

    async def _receive_loop(self) -> None:
        while True:
            received = await self.transport.receive()
            packet = received.packet

            # Client nhận dữ liệu qua server đã cấu hình.
            if received.peer != self.server_peer:
                continue

            # Chỉ xử lý tin gửi cho mình hoặc tin chung.
            if packet.recipient not in (self.username, "*"):
                continue

            if packet.kind == MessageType.FILE_CANCEL:
                if self._file_sender is not None and self._file_sender.handle(packet):
                    continue
                if packet.recipient == self.username:
                    await self._file_queue.put(packet)

            elif packet.kind in (MessageType.FILE_ACCEPT, MessageType.FILE_COMPLETE, MessageType.ERROR):
                if self._file_sender is not None:
                    self._file_sender.handle(packet)
                if packet.kind == MessageType.ERROR:
                    try:
                        import json
                        message = json.loads(packet.payload)['message']
                        if type(message) is str:
                            self.events.put_nowait({'type': 'error', 'message': message[:480]})
                    except (ValueError, KeyError, TypeError, UnicodeError, asyncio.QueueFull):
                        pass

            elif packet.kind in (MessageType.FILE_META, MessageType.FILE_CHUNK, MessageType.FILE_END):
                if packet.recipient == self.username:
                    await self._file_queue.put(packet)

            elif packet.kind == MessageType.USER_LIST:
                if packet.sender != "server":
                    continue
                try:
                    event = self.user_list.add(packet.payload)
                except ValueError as exc:
                    await self.events.put({"type": "error", "message": str(exc)})
                    continue
                if event is not None:
                    await self.events.put(event)

            elif packet.kind == MessageType.MSG:
                try:
                    text = packet.payload.decode("utf-8")

                except UnicodeDecodeError:
                    await self.events.put({
                        "type": "error",
                        "message": "Received message is not valid UTF-8",
                    })
                    continue

                await self.events.put({
                    "type": "message",
                    "sender": packet.sender,
                    "recipient": packet.recipient,
                    "text": text,
                })
