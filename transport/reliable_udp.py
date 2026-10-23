"""Owner A. Dùng chung ở client/server; chưa mở socket hay đảm bảo tin cậy."""

from common.models import Packet, Peer, ReceivedPacket


class ReliableUdpTransport:
    def __init__(self, local_address: Peer) -> None:
        self.local_address = local_address

    def register_peer(self, peer: Peer, session_id: str) -> None:
        """Cấu hình peer/phiên sau AUTH; không yêu cầu khóa trong bản chưa mã hóa.

        Client đăng ký server sau AUTH_OK; server đăng ký endpoint sau BIND hợp lệ.
        Xem docs/02_API.md: đăng ký không tự mở socket hoặc đánh dấu online.
        """
        # TODO[A3/A6]: lưu mapping peer/session, kiểm tra đầu vào và xung đột phiên.
        raise NotImplementedError("TODO[A3/A6]: register_peer")

    async def start(self) -> None:
        # TODO[A5]: socket UDP + receive loop async; cấu hình buffer hữu hạn.
        raise NotImplementedError("TODO[A5]: ReliableUdpTransport.start")

    async def send(self, peer: Peer, packet: Packet) -> None:
        # TODO[A6]: seq theo stream; encode JSON; đợi ACK với retry hữu hạn.
        # ACK không cần ACK tiếp; loại heartbeat có chính sách hết hạn riêng.
        # TODO[A6]: recv_limit (packet); WINDOW_PROBE/UPDATE và stall timeout.
        # TODO[A7]: đồng thời tuân ngân sách CUBIC (byte) dùng chung mỗi peer/hướng.
        raise NotImplementedError("TODO[A6]: ReliableUdpTransport.send")

    async def receive(self) -> ReceivedPacket:
        # TODO[A6]: decode JSON, kiểm tra session/endpoint, ACK, dedup/order.
        # ACK/WINDOW_UPDATE/WINDOW_PROBE xử lý nội bộ; không giao sang nghiệp vụ.
        # receive() nhả slot; tăng recv_limit sau giao gói đúng thứ tự cho caller.
        raise NotImplementedError("TODO[A6]: ReliableUdpTransport.receive")

    async def close(self) -> None:
        # TODO[A7]: đóng socket; hủy task/timer; resolve/fail các send đang đợi.
        raise NotImplementedError("TODO[A7]: ReliableUdpTransport.close")
