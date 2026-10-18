"""Owner A. Dùng chung ở client/server; chưa mở socket hay đảm bảo tin cậy."""

from common.models import Packet, Peer, ReceivedPacket


class ReliableUdpTransport:
    def __init__(self, local_address: Peer) -> None:
        self.local_address = local_address

    def register_peer(
        self, peer: Peer, session_id: str, tx_key: bytes, rx_key: bytes
    ) -> None:
        # TODO[A3/A6]: cấu hình sau auth, tách khóa/nonce theo hướng; phối hợp K5.
        raise NotImplementedError("TODO[A3/A6]: register_peer")

    async def start(self) -> None:
        # TODO[A5]: socket UDP + receive loop async; cấu hình buffer hữu hạn.
        raise NotImplementedError("TODO[A5]: ReliableUdpTransport.start")

    async def send(self, peer: Peer, packet: Packet) -> None:
        # TODO[A6]: seq theo stream; cửa sổ; encode + cipher; đợi ACK với retry.
        # ACK không cần ACK tiếp; loại heartbeat có chính sách hết hạn riêng.
        # TODO[A7]: gate theo receiver window + congestion budget; timeout hữu hạn.
        raise NotImplementedError("TODO[A6]: ReliableUdpTransport.send")

    async def receive(self) -> ReceivedPacket:
        # TODO[A6]: decode envelope, xác thực, ACK, dedup, reorder mỗi stream.
        # ACK được transport xử lý nội bộ, không giao ACK sang router/client.
        raise NotImplementedError("TODO[A6]: ReliableUdpTransport.receive")

    async def close(self) -> None:
        # TODO[A7]: đóng socket; hủy task/timer; resolve/fail các send đang đợi.
        raise NotImplementedError("TODO[A7]: ReliableUdpTransport.close")
