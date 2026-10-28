"""Owner Khoa. Tách nghiệp vụ khỏi UI và socket."""

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

    async def login(self, password: str) -> None:
        if self.tcp_peer is None or self._login_attempted or self._receive_started:
            raise RuntimeError("Cần client mới với tcp_peer để đăng nhập")
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
            raise ConnectionError("Client chưa đăng nhập/BIND hoặc phiên đã dừng")

    async def send_chat(self, recipient: str, text: str) -> None:
        self._check_session()
        recipient = recipient.strip()

        if not recipient or len(recipient) > 32:
            raise ValueError(
                "Tên người nhận phải có từ 1 đến 32 ký tự"
            )

        if not text.strip():
            raise ValueError("Tin nhắn không được để trống")

        payload = text.encode("utf-8")

        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"Tin nhắn tối đa {MAX_PAYLOAD_BYTES} byte"
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
        """Chờ tới COMPLETE đúng size/hash; lỗi/timeout/hủy được trả về caller."""
        self._check_session()
        if type(recipient) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,32}", recipient) is None:
            raise ValueError("File phải gửi riêng tới một username hợp lệ")
        if not self._receiving:
            raise RuntimeError("Cần chạy receive_events trước khi gửi file")
        if self._file_sender is not None:
            raise RuntimeError("Mỗi client chỉ gửi một file tại một thời điểm")
        if self._sent_file_count >= 4096:
            raise RuntimeError("Đã hết quota lượt gửi; cần tạo phiên mới")
        sender = FileSender(self.transport, self.server_peer, self.username, recipient,
                            self._file_io, self.file_updates)
        self.file_updates.begin(sender.transfer_id, recipient)
        self._file_sender = sender
        self._sent_file_count += 1
        try:
            await sender.run(Path(path))
        finally:
            self._file_sender = None

    def poll_file_updates(self) -> list[dict]:
        """Lấy snapshot mới nhất; gọi trên event loop của client, như bridge sau này."""
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
            raise RuntimeError("Mỗi phiên chỉ chạy một vòng receive_events")
        self._receive_started = True
        self._receiving = True
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._receive_file_offers())
                await self._receive_loop()
        finally:
            self._receiving = False
            if self._file_sender is not None:
                self._file_sender.fail(ConnectionError("Vòng nhận đã dừng khi đang gửi file"))

    async def cancel_receive_file(self, transfer_id: str) -> bool:
        """True: worker đã xử lý yêu cầu; xem snapshot để biết cleanup/gửi CANCEL."""
        if type(transfer_id) is not str or re.fullmatch(r"[0-9a-f]{32}", transfer_id) is None:
            raise ValueError("Transfer ID không hợp lệ")
        if not self._receiving or not self._file_worker_running:
            raise RuntimeError("Vòng nhận chưa chạy hoặc đã dừng")
        result = asyncio.get_running_loop().create_future()
        try:
            self._file_queue.put_nowait(ReceiveCancel(transfer_id, result))
        except asyncio.QueueFull:
            raise RuntimeError("Worker nhận đang bận; hãy thử hủy lại") from None
        return await result

    async def _cancel_receive(self, request: ReceiveCancel):
        if request.result.cancelled():
            return  # Caller bỏ yêu cầu trước khi worker bắt đầu.
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
            pass  # Snapshot báo gửi hủy lỗi; không làm dừng các lượt nhận khác.
        finally:
            await self._file_io(self.file_offers.note_local_cancel, request.transfer_id, delivery)
        if not request.result.done():
            request.result.set_result(True)

    async def _file_io(self, function, *args):
        # Hủy coroutine không tự dừng thread. Chờ thao tác đĩa kết thúc trước
        # khi worker chuyển sang close(), tránh begin() tạo .part sau cleanup.
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
                except TimeoutError:
                    continue
                try:
                    if isinstance(packet, ReceiveCancel):
                        try:
                            await self._cancel_receive(packet)
                        finally:
                            if not packet.result.done():
                                packet.result.set_exception(ConnectionError("Worker dừng khi đang xử lý hủy"))
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
                finally:
                    self._file_queue.task_done()
        finally:
            self._file_worker_running = False
            # Lỗi gửi phản hồi/cleanup được truyền về caller, không báo thành công.
            try:
                await self._file_io(self.file_offers.close)
            finally:
                while not self._file_queue.empty():
                    pending = self._file_queue.get_nowait()
                    if isinstance(pending, ReceiveCancel) and not pending.result.done():
                        pending.result.set_exception(ConnectionError("Worker đã dừng trước khi xử lý hủy"))
                    self._file_queue.task_done()

    async def _send_file_notifications(self) -> None:
        for reply in await self._file_io(self.file_offers.take_notifications):
            await asyncio.wait_for(self.transport.send(self.server_peer, reply),
                                   timeout=FILE_REPLY_TIMEOUT_SECONDS)

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
                        "message": "Tin nhắn nhận được không đúng UTF-8",
                    })
                    continue

                await self.events.put({
                    "type": "message",
                    "sender": packet.sender,
                    "recipient": packet.recipient,
                    "text": text,
                })
