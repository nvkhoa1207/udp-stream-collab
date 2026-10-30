"""Các hàm gửi nhận mà transport thật và mock đều cần có."""

from typing import Protocol

from common.models import Packet, Peer, ReceivedPacket


class Transport(Protocol):
    async def start(self) -> None:
        """Mở tài nguyên nhận/gửi."""
        ...

    async def send(self, peer: Peer, packet: Packet) -> None:
        """Bản thật đợi ACK hoặc báo lỗi; bản mock chỉ đưa gói vào hàng đợi."""
        ...

    async def receive(self) -> ReceivedPacket:
        """Kiểm tra phiên, bỏ gói trùng và trả gói đúng thứ tự cho ứng dụng.

        Khi lấy gói ra thì bộ đệm nhận có thêm chỗ. Phía gọi cũng cần giới hạn
        hàng đợi của mình. Việc kiểm tra phiên không thay cho xác thực bằng mật mã.
        """
        ...

    async def close(self) -> None:
        """Đóng kết nối; phía gọi hủy tác vụ receive đang chờ khi dừng chương trình."""
        ...
