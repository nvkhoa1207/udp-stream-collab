"""Owner Khoa. Tách nghiệp vụ khỏi UI và socket."""

import asyncio
from pathlib import Path

from common.config import MAX_PAYLOAD_BYTES
from common.contracts import Transport
from common.models import MessageType, Packet, Peer


class ClientService:
    def __init__(self, transport: Transport, server_peer: Peer, username: str) -> None:
        self.transport = transport
        self.server_peer = server_peer
        self.username = username
        self.events = asyncio.Queue(maxsize=100)

    async def login(self, password: str) -> None:
        # TODO[K1]: handshake TCP + auth; AUTH_OK trả session_id, không trả khóa.
        # Đăng ký peer/session rồi BIND/BIND_OK theo docs/02_API.md.
        # Cần truyền cấu hình TCP riêng; server_peer là endpoint UDP.
        # Nhận cấu hình phiên, A đăng ký peer/key; BIND trước dữ liệu nghiệp vụ.
        raise NotImplementedError("TODO[K1]: ClientService.login")

    async def send_chat(self, recipient: str, text: str) -> None:
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
        # TODO[K3/K4]: meta/accept/chunks/end/complete; bounded concurrency; progress.
        raise NotImplementedError("TODO[K3/K4]: ClientService.send_file")

    async def receive_events(self) -> None:
        while True:
            received = await self.transport.receive()
            packet = received.packet

            # Client nhận dữ liệu qua server đã cấu hình.
            if received.peer != self.server_peer:
                continue

            # Chỉ xử lý tin gửi cho mình hoặc tin chung.
            if packet.recipient not in (self.username, "*"):
                continue

            if packet.kind == MessageType.MSG:
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
