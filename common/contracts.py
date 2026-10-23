"""Khoa và A cùng dựa vào API này để phát triển độc lập."""

from typing import Protocol

from common.models import Packet, Peer, ReceivedPacket


class Transport(Protocol):
    async def start(self) -> None:
        """Mở tài nguyên nhận/gửi."""
        ...

    async def send(self, peer: Peer, packet: Packet) -> None:
        """Real: đợi ACK chặng kế tiếp hoặc raise lỗi; mock: chỉ đưa vào queue."""
        ...

    async def receive(self) -> ReceivedPacket:
        """Real: kiểm tra session/endpoint, dedup/order; trả gói và nhả slot nhận.

        Caller giữ backpressure bằng queue/task hữu hạn; không có release().
        Kiểm tra phiên không đồng nghĩa với xác thực packet bằng mật mã.
        """
        ...

    async def close(self) -> None:
        """Đóng transport; caller hủy task receive đang chờ khi shutdown."""
        ...
