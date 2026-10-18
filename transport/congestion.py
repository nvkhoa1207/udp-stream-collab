"""Owner A, Khoa hỗ trợ đo. KHÔNG phải một bản BBR/CUBIC đã triển khai."""


class CongestionController:
    def budget_bytes(self) -> int:
        # TODO[A7]: ngân sách gửi còn lại theo thuật toán được xác nhận.
        raise NotImplementedError("TODO[A7]: budget_bytes")

    def on_ack(self, acknowledged_bytes: int, rtt_seconds: float) -> None:
        # TODO[A7]: cập nhật cwnd/RTT/rate theo đúng thuật toán, tránh ACK đếm đôi.
        raise NotImplementedError("TODO[A7]: on_ack")

    def on_loss(self, lost_bytes: int) -> None:
        # TODO[A7]: phản ứng loss; fixed window không được gọi là BBR/CUBIC.
        raise NotImplementedError("TODO[A7]: on_loss")
