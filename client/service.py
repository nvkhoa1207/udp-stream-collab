"""Owner Khoa. Tách nghiệp vụ khỏi UI và socket."""

from pathlib import Path

from common.contracts import Transport
from common.models import Peer


class ClientService:
    def __init__(self, transport: Transport, server_peer: Peer, username: str) -> None:
        self.transport = transport
        self.server_peer = server_peer
        self.username = username

    async def login(self, password: str) -> None:
        # TODO[K1]: handshake TCP + auth, kiểm tra chứng chỉ nếu dùng TLS.
        # Cần truyền cấu hình TCP riêng; server_peer là endpoint UDP.
        # Nhận cấu hình phiên, A đăng ký peer/key; BIND trước dữ liệu nghiệp vụ.
        raise NotImplementedError("TODO[K1]: ClientService.login")

    async def send_chat(self, recipient: str, text: str) -> None:
        # TODO[K2]: kiểm tra kích thước UTF-8, tạo Packet MSG, gửi qua transport.
        raise NotImplementedError("TODO[K2]: ClientService.send_chat")

    async def send_file(self, recipient: str, path: Path) -> None:
        # TODO[K3/K4]: meta/accept/chunks/end/complete; bounded concurrency; progress.
        raise NotImplementedError("TODO[K3/K4]: ClientService.send_file")

    async def receive_events(self) -> None:
        # TODO[K2/K4]: một consumer transport.receive(), dispatch MSG/user/file/error.
        # Đẩy event ra queue/callback cho UI; không print trực tiếp trong core.
        raise NotImplementedError("TODO[K2/K4]: ClientService.receive_events")
