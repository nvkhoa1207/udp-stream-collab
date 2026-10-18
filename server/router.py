"""Owner A. Định tuyến nghiệp vụ, không tự làm lại ACK/sequence ở đây."""

from common.contracts import Transport
from common.models import ReceivedPacket
from server.sessions import SessionRegistry


class Router:
    def __init__(self, transport: Transport, sessions: SessionRegistry) -> None:
        self.transport = transport
        self.sessions = sessions

    async def handle(self, received: ReceivedPacket) -> None:
        # TODO[A5]: suy ra sender từ session; validate recipient/kind/quyền.
        # MSG '*': fan-out theo danh sách. Private/file: lookup đúng endpoint.
        # Giữ message_id khi relay, transport gán session/seq mới cho chặng kế.
        # Một peer chậm không block fan-out/vòng nhận; lỗi forward phải báo người gửi.
        raise NotImplementedError("TODO[A5]: Router.handle")
