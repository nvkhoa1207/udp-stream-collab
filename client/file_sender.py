"""Một lượt gửi file; vòng nhận của ClientService chuyển phản hồi vào handle()."""

import asyncio
import hashlib
import json
import re
from itertools import islice
from pathlib import Path
from uuid import uuid4

from client.files import iter_chunks
from client.file_cancel import FileTransferCancelled, cancelled_transfer_id
from common.config import MAX_PAYLOAD_BYTES
from common.models import MessageType, Packet
from common.errors import FILE_FAILURE_REASONS


FILE_SEND_CONCURRENCY = 8
MAX_SEND_FILE_BYTES = 16 * 1024 * 1024
FILE_SEND_TIMEOUT_SECONDS = 45.0
FILE_RESPONSE_TIMEOUT_SECONDS = 30.0
FILE_CANCEL_TIMEOUT_SECONDS = 3.0


def prepare_metadata(path: Path) -> dict:
    """Đọc từng chunk, chặn cả trường hợp file lớn lên trong lúc tính hash."""
    if not path.is_file():
        raise ValueError("Only regular files can be sent")
    if path.stat().st_size > MAX_SEND_FILE_BYTES:
        raise ValueError("File exceeds the 16 MiB send limit")
    digest = hashlib.sha256()
    size = chunks = 0
    source = iter_chunks(path)
    try:
        for chunk in source:
            size += len(chunk)
            if size > MAX_SEND_FILE_BYTES:
                raise ValueError("File exceeds the 16 MiB send limit")
            chunks += 1
            digest.update(chunk)
    finally:
        source.close()
    return dict(name=path.name, size=size, sha256=digest.hexdigest(),
                chunk_size=MAX_PAYLOAD_BYTES, total_chunks=chunks)


class FileSender:
    def __init__(self, transport, server_peer, username, recipient, file_io, updates, remember=None):
        self.transport = transport
        self.server_peer = server_peer
        self.username = username
        self.recipient = recipient
        self.file_io = file_io
        self.updates = updates
        self.remember = remember
        self.transfer_id = uuid4().hex
        self.phase = "preparing"
        self.metadata = None
        self.message_ids = set()
        self.accepted = asyncio.Event()
        self.completed = asyncio.Event()
        self.finished = asyncio.Event()
        self.meta_started = False
        self.remote_cancel = False
        self.acknowledged_bytes = 0
        # Lưu lỗi bằng set_result để tránh cảnh báo lỗi Future chưa được đọc.
        self.failure = asyncio.get_running_loop().create_future()

    def fail(self, error: Exception) -> None:
        if not self.failure.done():
            self.failure.set_result(error)

    def handle(self, packet: Packet) -> bool | None:
        if packet.recipient != self.username:
            return
        if packet.kind == MessageType.FILE_CANCEL:
            if (packet.sender == self.recipient
                    and cancelled_transfer_id(packet) == self.transfer_id):
                self.remote_cancel = True
                reason = json.loads(packet.payload)['reason']
                if reason in FILE_FAILURE_REASONS:
                    self.fail(RuntimeError(reason))
                else:
                    self.fail(FileTransferCancelled("The receiver cancelled the transfer"))
                return True
            return False
        if packet.kind == MessageType.ERROR:
            if packet.sender not in (self.recipient, "server") or packet.stream_id != "control":
                return
            try:
                body = self._json(packet.payload)
                if (set(body) != {"code", "message", "related_message_id"}
                        or any(type(value) is not str for value in body.values())
                        or body["related_message_id"] not in self.message_ids):
                    return
            except ValueError:
                return
            if body['code'] == 'FILE_CANCELLED':
                self.remote_cancel = True
                self.fail(FileTransferCancelled(body['message']))
            else:
                self.fail(RuntimeError(f"{body['code']}: {body['message']}"))
            return True

        if packet.sender != self.recipient or packet.stream_id != self.transfer_id:
            return
        try:
            if packet.kind == MessageType.FILE_ACCEPT and self.phase == "accept":
                if packet.payload != b"" or packet.chunk_index is not None:
                    raise ValueError("Invalid FILE_ACCEPT")
                self.accepted.set()
            elif packet.kind == MessageType.FILE_COMPLETE and self.phase == "complete":
                body = self._json(packet.payload)
                if (set(body) != {"size", "sha256"}
                        or type(body["size"]) is not int
                        or body["size"] != self.metadata["size"]
                        or type(body["sha256"]) is not str
                        or re.fullmatch(r"[0-9a-fA-F]{64}", body["sha256"]) is None
                        or body["sha256"].lower() != self.metadata["sha256"]
                        or packet.chunk_index is not None):
                    raise ValueError("FILE_COMPLETE size or SHA-256 does not match")
                self.completed.set()
        except ValueError as exc:
            self.fail(exc)

    @staticmethod
    def _json(payload: bytes) -> dict:
        if not isinstance(payload, bytes) or len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("File response is too large or has an invalid type")
        try:
            body = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise ValueError("File response is not valid JSON") from exc
        if type(body) is not dict:
            raise ValueError("File response must be an object")
        return body

    async def _send(self, kind, payload=b"", index=None):
        packet = Packet(kind=kind, sender=self.username, recipient=self.recipient,
                        stream_id=self.transfer_id, payload=payload, chunk_index=index)
        # Ghi nhận trước khi gửi vì bên nhận có thể trả lời ngay.
        self.message_ids.add(packet.message_id)
        if self.remember is not None:
            self.remember(packet, self.transfer_id, 'send')
        if kind == MessageType.FILE_META:
            self.meta_started = True
        await asyncio.wait_for(self.transport.send(self.server_peer, packet),
                               timeout=FILE_SEND_TIMEOUT_SECONDS)
        if kind == MessageType.FILE_CHUNK:
            # Mỗi phần file gọi send một lần. Transport tự lo gửi lại và ACK trùng.
            self.acknowledged_bytes += len(payload)
            self.updates.update(self.transfer_id, acknowledged_bytes=self.acknowledged_bytes)

    def request_cancel(self):
        self.fail(FileTransferCancelled("File cancellation requested"))

    async def _notify_cancel(self):
        packet = Packet(
            kind=MessageType.FILE_CANCEL, sender=self.username, recipient=self.recipient,
            stream_id="control", payload=json.dumps({
                "transfer_id": self.transfer_id, "reason": "Sender stopped the transfer",
            }).encode(),
        )
        try:
            await asyncio.wait_for(self.transport.send(self.server_peer, packet),
                                   timeout=FILE_CANCEL_TIMEOUT_SECONDS)
        except Exception:
            return "failed"
        # ACK này chỉ từ server, chưa cho biết bên nhận đã dọn file hay chưa.
        return "sent_to_server"

    async def run(self, path: Path) -> None:
        work = asyncio.create_task(self._transfer(path))
        status = "failed"
        try:
            await asyncio.wait((work, self.failure), return_when=asyncio.FIRST_COMPLETED)
            if self.failure.done():
                raise self.failure.result()
            await work
            status = "completed"
        except (asyncio.CancelledError, FileTransferCancelled) as error:
            status = "cancelled"
            self.updates.update(self.transfer_id, message=str(error) or 'Client stopped the transfer')
            raise
        except Exception as error:
            message = str(error) or ('Timed out during ' + self.phase)
            self.updates.update(self.transfer_id, message=message)
            raise
        finally:
            # Vẫn dọn file nếu tác vụ bị hủy lần nữa trong lúc báo bên nhận.
            cleanup = asyncio.create_task(self._finish(work, status))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    async def _finish(self, work, status):
        cancel_delivery = "not_needed"
        try:
            if not work.done():
                work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            if status != "completed" and self.meta_started and not self.remote_cancel:
                cancel_delivery = await self._notify_cancel()
        finally:
            self.failure.cancel()
            self.message_ids.clear()
            self.phase = "closed"
            self.updates.update(self.transfer_id, status=status, cancel_delivery=cancel_delivery)
            self.finished.set()

    async def _transfer(self, path: Path) -> None:
        self.metadata = await self.file_io(prepare_metadata, path)
        payload = json.dumps(self.metadata, ensure_ascii=False).encode("utf-8")
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("Metadata exceeds the 480-byte limit")

        self.phase = "accept"
        self.updates.update(
            self.transfer_id,
            status="waiting_accept",
            total_bytes=self.metadata["size"],
            name=self.metadata["name"],
        )
        await self._send(MessageType.FILE_META, payload)
        await asyncio.wait_for(self.accepted.wait(), timeout=FILE_RESPONSE_TIMEOUT_SECONDS)

        self.phase = "chunks"
        self.updates.update(self.transfer_id, status="sending")
        source = iter_chunks(path)
        digest = hashlib.sha256()
        size = index = 0
        try:
            while True:
                batch = await self.file_io(lambda: list(islice(source, FILE_SEND_CONCURRENCY)))
                if not batch:
                    break
                size += sum(map(len, batch))
                if size > self.metadata["size"]:
                    raise ValueError("Source file changed during transfer")
                # TaskGroup hủy và đợi các send còn lại nếu một send bị lỗi.
                async with asyncio.TaskGroup() as tasks:
                    for chunk in batch:
                        digest.update(chunk)
                        tasks.create_task(self._send(MessageType.FILE_CHUNK, chunk, index))
                        index += 1
        finally:
            await self.file_io(source.close)

        if (size != self.metadata["size"] or index != self.metadata["total_chunks"]
                or digest.hexdigest() != self.metadata["sha256"]):
            raise ValueError("Source file changed during transfer")
        self.phase = "complete"
        self.updates.update(self.transfer_id, status="verifying")
        await self._send(MessageType.FILE_END)
        await asyncio.wait_for(self.completed.wait(), timeout=FILE_RESPONSE_TIMEOUT_SECONDS)
