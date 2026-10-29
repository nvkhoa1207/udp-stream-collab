"""Nhận META/CHUNK/END qua một worker, dùng FileReceiver để ghi và kiểm tra file."""

import json
import re
import time
from dataclasses import replace
from typing import Callable

from client.files import FileReceiver
from client.file_cancel import cancelled_transfer_id
from common.config import MAX_PAYLOAD_BYTES
from common.models import MessageType, Packet


MAX_FILE_OFFERS_PER_SESSION = 4096
FILE_RESULT_CACHE_SECONDS = 120.0


class FileOfferReceiver:
    """Một worker sở hữu receiver và gọi các phương thức tuần tự trong một phiên."""

    def __init__(
        self, username: str, receiver: FileReceiver | None, *,
        clock: Callable[[], float] = time.monotonic,
        updates=None,
    ) -> None:
        self.username = username
        self.receiver = receiver
        self.seen_ids: set[str] = set()
        self.owners: dict[str, str] = {}
        # transfer_id -> (người gửi, phản hồi cuối, thời điểm hết cache).
        self.results: dict[str, tuple[str, Packet, float]] = {}
        self._clock = clock
        self.updates = updates
        self._watching: set[str] = set()
        self._cleanup_pending: set[str] = set()
        self._confirming: set[str] = set()
        self._local_cancelling: set[str] = set()
        self._offers: dict[str, Packet] = {}
        self._notifications: dict[str, Packet] = {}
        self._terminal_streams: dict[str, bool] = {}

    def _update(self, transfer_id, **changes):
        if self.updates is not None:
            self.updates.update(transfer_id, direction="receive", **changes)

    def _settle(self, transfer_id, status, **changes):
        self._offers.pop(transfer_id, None)
        pending = self.receiver is not None and transfer_id in self.receiver.transfers
        if status != 'completed':
            self._terminal_streams[transfer_id] = False
        self._update(transfer_id, status=status, cleanup_pending=pending, **changes)
        if pending:
            self._watching.add(transfer_id)
            self._cleanup_pending.add(transfer_id)
        else:
            self._watching.discard(transfer_id)
            self._cleanup_pending.discard(transfer_id)

    def _sync_expired(self):
        # begin() cũng gọi expire(); đồng bộ cả những lượt được dọn khi mở file mới.
        for transfer_id in list(self._watching):
            transfer = self.receiver.transfers.get(transfer_id)
            if transfer_id in self._cleanup_pending:
                if transfer is None:
                    self._update(transfer_id, cleanup_pending=False)
                    self._watching.discard(transfer_id)
                    self._cleanup_pending.discard(transfer_id)
            elif transfer is None:
                offer = self._offers.get(transfer_id)
                if offer is not None:
                    reply = self._error(offer, "FILE_EXPIRED", "Incoming transfer expired")
                    self._notifications[transfer_id] = self._remember_result(offer, reply)
                else:
                    self._settle(transfer_id, "expired", code="FILE_EXPIRED", message="Incoming transfer expired")
            elif transfer["failed"]:
                self._settle(transfer_id, "failed", code="FILE_STORAGE_ERROR", message="Temporary file cleanup failed")

    def _reject(self, packet, code, message):
        self._settle(packet.stream_id, "failed", code=code, message=message)
        return self._error(packet, code, message)

    def take_notifications(self) -> list[Packet]:
        """Worker lấy thông báo một lần; retry truyền gói thuộc transport."""
        replies = list(self._notifications.values())
        self._notifications.clear()
        return replies

    def take_terminal_streams(self):
        terminal = list(self._terminal_streams.items())
        self._terminal_streams.clear()
        return terminal

    def _valid_address(self, packet: Packet) -> bool:
        return (packet.recipient == self.username
                and type(packet.sender) is str
                and re.fullmatch(r"[A-Za-z0-9_-]{1,32}", packet.sender) is not None
                and type(packet.message_id) is str
                and re.fullmatch(r"[0-9a-f]{32}", packet.message_id) is not None)

    def _error(self, packet: Packet, code: str, message: str) -> Packet:
        payload = json.dumps({
            "code": code,
            "message": message,
            "related_message_id": packet.message_id,
        }, ensure_ascii=False).encode("utf-8")
        return Packet(
            kind=MessageType.ERROR, sender=self.username, recipient=packet.sender,
            stream_id="control", payload=payload,
        )

    def prepare(self, packet: Packet) -> Packet | None:
        # File chỉ gửi riêng. Header không đủ để trả lời an toàn thì bỏ gói.
        if packet.kind != MessageType.FILE_META or not self._valid_address(packet):
            return None
        if (type(packet.stream_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", packet.stream_id) is None):
            return self._error(packet, "INVALID_FILE_META", "Invalid file transfer ID")
        if self.receiver is None:
            return self._error(packet, "FILE_RECEIVE_DISABLED", "No receive folder is configured")
        transfer_id = packet.stream_id
        if transfer_id in self.seen_ids:
            return self._error(packet, "FILE_ID_REUSED", "Transfer IDs cannot be reused in a session")
        if len(self.seen_ids) >= MAX_FILE_OFFERS_PER_SESSION:
            return self._error(packet, "FILE_SESSION_LIMIT", "Session transfer quota reached; reconnect to continue")

        # Giữ dấu kể cả sau lỗi/expire, tránh META đến muộn tạo lại file cũ.
        self.seen_ids.add(transfer_id)
        if self.updates is not None:
            try:
                self.updates.begin(transfer_id, packet.sender, direction="receive")
            except RuntimeError:
                return self._error(packet, "FILE_STATUS_LIMIT", "Read pending updates before receiving another file")
        try:
            if not isinstance(packet.payload, bytes) or len(packet.payload) > MAX_PAYLOAD_BYTES:
                raise ValueError("Payload is too large or has an invalid type")
            metadata = json.loads(packet.payload.decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError):
            return self._reject(packet, "INVALID_FILE_META", "Metadata is not valid UTF-8 JSON")

        try:
            # begin() kiểm tra schema, hash, tên, size/quota và tạo .part.
            self.receiver.begin(transfer_id, metadata)
        except ValueError:
            return self._reject(packet, "FILE_REJECTED", "Invalid metadata or file receive limit exceeded")
        except (OSError, ExceptionGroup):
            # Không đưa đường dẫn máy hoặc thông tin lỗi hệ thống vào gói mạng.
            return self._reject(packet, "FILE_STORAGE_ERROR", "Could not prepare the destination file")
        finally:
            self._sync_expired()

        self.owners[transfer_id] = packet.sender
        self._offers[transfer_id] = packet
        self._watching.add(transfer_id)
        saved = self.receiver.transfers[transfer_id]["metadata"]
        self._update(transfer_id, status="receiving", total_bytes=saved["size"], name=saved["name"])
        return Packet(
            kind=MessageType.FILE_ACCEPT, sender=self.username,
            recipient=packet.sender, stream_id=transfer_id,
        )

    def _prune(self) -> None:
        now = self._clock()
        for transfer_id, (_, _, deadline) in list(self.results.items()):
            if now >= deadline:
                del self.results[transfer_id]
        if self.receiver is not None:
            for transfer_id in list(self.owners):
                transfer = self.receiver.transfers.get(transfer_id)
                if transfer is None or transfer["failed"]:
                    del self.owners[transfer_id]

    def _remember_result(self, packet: Packet, reply: Packet) -> Packet:
        self._offers.pop(packet.stream_id, None)
        self.owners.pop(packet.stream_id, None)
        self.results[packet.stream_id] = (
            packet.sender, reply, self._clock() + FILE_RESULT_CACHE_SECONDS,
        )
        if reply.kind == MessageType.FILE_COMPLETE:
            self._confirming.add(packet.stream_id)
            self._settle(packet.stream_id, "completed", confirmation_delivery="pending")
        else:
            body = json.loads(reply.payload)
            status = {"FILE_CANCELLED": "cancelled", "FILE_EXPIRED": "expired"}.get(body["code"], "failed")
            self._settle(packet.stream_id, status, code=body["code"], message=body["message"])
        return reply

    def note_reply(self, reply: Packet, delivered: bool):
        if reply.kind == MessageType.FILE_COMPLETE:
            self._terminal_streams[reply.stream_id] = delivered
            self._update(reply.stream_id, confirmation_delivery="sent_to_server" if delivered else "failed")
            self._confirming.discard(reply.stream_id)

    def _fail(self, packet: Packet, code: str, message: str) -> Packet:
        # Chỉ gọi sau khi đã xác nhận đúng chủ của lượt truyền.
        try:
            self.receiver.cancel(packet.stream_id)
        except OSError:
            code, message = "FILE_STORAGE_ERROR", "Temporary file cleanup failed; will retry"
        return self._remember_result(packet, self._error(packet, code, message))

    def handle(self, packet: Packet) -> Packet | None:
        """CHUNK thường không có reply nghiệp vụ; END thành công trả COMPLETE."""
        if packet.kind == MessageType.FILE_CANCEL:
            return self.cancel(packet)
        if packet.kind == MessageType.FILE_META:
            return self.prepare(packet)
        if packet.kind not in (MessageType.FILE_CHUNK, MessageType.FILE_END):
            return None
        if not self._valid_address(packet):
            return None
        transfer_id = packet.stream_id
        if (type(transfer_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", transfer_id) is None):
            return self._error(packet, "FILE_DATA_ERROR", "Invalid file transfer ID")
        if self.receiver is None:
            return self._error(packet, "FILE_RECEIVE_DISABLED", "No receive folder is configured")

        self._prune()
        cached = self.results.get(transfer_id)
        owner = cached[0] if cached is not None else self.owners.get(transfer_id)
        if owner is None:
            return self._error(packet, "FILE_NOT_ACTIVE", "Incoming transfer has not started or has already ended")
        if owner != packet.sender:
            return self._error(packet, "FILE_SENDER_MISMATCH", "Sender does not match the transfer")

        if cached is not None:
            # Không gia hạn cache khi có gói lặp, không gọi finish() lần hai.
            if packet.kind == MessageType.FILE_END:
                if not isinstance(packet.payload, bytes) or packet.payload != b"" or packet.chunk_index is not None:
                    return self._error(packet, "FILE_DATA_ERROR", "FILE_END must have an empty payload")
                return cached[1]
            return cached[1] if cached[1].kind == MessageType.ERROR else None

        try:
            if packet.kind == MessageType.FILE_CHUNK:
                previous = self.receiver.transfers[transfer_id]["received_bytes"]
                self.receiver.accept_chunk(transfer_id, packet.chunk_index, packet.payload)
                received_bytes = self.receiver.transfers[transfer_id]["received_bytes"]
                if received_bytes != previous:
                    self._update(transfer_id, received_bytes=received_bytes)
                return None

            if not isinstance(packet.payload, bytes) or packet.payload != b"" or packet.chunk_index is not None:
                raise ValueError("Invalid FILE_END format")
            metadata = dict(self.receiver.transfers[transfer_id]["metadata"])
            self._update(transfer_id, status="verifying")
            # finish() chỉ thành công sau khi đủ chunk, đúng size/hash và lưu xong.
            self.receiver.finish(transfer_id)
        except TimeoutError:
            return self._fail(packet, "FILE_EXPIRED", "Incoming file transfer expired")
        except ValueError:
            return self._fail(packet, "FILE_DATA_ERROR", "Missing chunks or invalid data, size or hash")
        except OSError:
            return self._fail(packet, "FILE_STORAGE_ERROR", "Could not write or finalize the file")

        reply = Packet(
            kind=MessageType.FILE_COMPLETE, sender=self.username,
            recipient=packet.sender, stream_id=transfer_id,
            payload=json.dumps({
                "size": metadata["size"], "sha256": metadata["sha256"],
            }).encode("utf-8"),
        )
        return self._remember_result(packet, reply)

    def cancel(self, packet: Packet) -> Packet | None:
        transfer_id = cancelled_transfer_id(packet)
        if transfer_id is None or not self._valid_address(packet):
            return None
        if self.receiver is None:
            return None
        self._prune()
        cached = self.results.get(transfer_id)
        owner = cached[0] if cached is not None else self.owners.get(transfer_id)
        if owner is not None and owner != packet.sender:
            return self._error(packet, "FILE_SENDER_MISMATCH", "Sender does not match the transfer")
        if cached is not None:
            # COMPLETE đã có thì không xóa file; CANCEL lặp không kéo dài cache.
            # COMPLETE đang gửi đã có transport lo retry. Không gửi DATA mới
            # vì luồng file có thể đã đóng khi CANCEL tới trên control.
            if cached[1].kind == MessageType.FILE_COMPLETE:
                return None
            return cached[1]
        if transfer_id not in self.seen_ids:
            if len(self.seen_ids) >= MAX_FILE_OFFERS_PER_SESSION:
                return self._error(packet, "FILE_SESSION_LIMIT", "Session transfer quota reached")
            # CANCEL đi control có thể vượt META đi stream file.
            self.seen_ids.add(transfer_id)
        target = replace(packet, stream_id=transfer_id)
        return self._fail(target, "FILE_CANCELLED", "File transfer was cancelled")

    def expire(self) -> list[str]:
        try:
            return self.receiver.expire() if self.receiver is not None else []
        finally:
            self._sync_expired()
            self._prune()

    def cancel_local(self, transfer_id: str) -> Packet | None:
        """Chỉ gọi trong worker; không hủy file đã kết thúc hoặc chưa được nhận."""
        offer = self._offers.get(transfer_id)
        if offer is None or transfer_id not in self.receiver.transfers:
            return None
        self._update(transfer_id, cancel_delivery="pending")
        self._local_cancelling.add(transfer_id)
        self._fail(offer, "FILE_CANCELLED", "The receiver cancelled the transfer")
        return Packet(
            kind=MessageType.FILE_CANCEL, sender=self.username, recipient=offer.sender,
            stream_id="control", payload=json.dumps({
                "transfer_id": transfer_id, "reason": "Receiver cancelled the transfer",
            }).encode(),
        )

    def note_local_cancel(self, transfer_id: str, delivery: str):
        self._update(transfer_id, cancel_delivery=delivery)
        self._local_cancelling.discard(transfer_id)

    def close(self) -> None:
        try:
            if self.receiver is not None:
                self.receiver.close()
        finally:
            # finish() có thể xong trong thread ngay khi worker bị hủy, trước send COMPLETE.
            for transfer_id in self._local_cancelling:
                self._update(transfer_id, cancel_delivery="failed")
            self._local_cancelling.clear()
            for transfer_id in self._confirming:
                self._update(transfer_id, confirmation_delivery="failed")
            self._confirming.clear()
            for transfer_id in list(self._watching):
                if transfer_id in self.receiver.transfers:
                    self._settle(transfer_id, "failed", code="FILE_STORAGE_ERROR", message="Could not remove temporary files when closing the client")
                elif transfer_id in self._cleanup_pending:
                    self._update(transfer_id, cleanup_pending=False)
                    self._watching.discard(transfer_id)
                    self._cleanup_pending.discard(transfer_id)
                else:
                    self._settle(transfer_id, "cancelled", code="CLIENT_CLOSED", message="Client stopped receiving files")
            self.owners.clear()
            self.results.clear()
            self._offers.clear()
            self._notifications.clear()
            self._terminal_streams.clear()
