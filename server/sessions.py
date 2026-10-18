"""Owner A. Dùng monotonic clock cho heartbeat và timeout."""

from common.models import Peer


class SessionRegistry:
    def create(self, username: str) -> str:
        # TODO[A3]: session ID khó đoán; chính sách trùng username; state ban đầu.
        raise NotImplementedError("TODO[A3]: SessionRegistry.create")

    def bind_peer(self, session_id: str, peer: Peer) -> None:
        # TODO[A3]: chỉ gọi sau khi BIND đã được xác thực bằng khóa của phiên.
        raise NotImplementedError("TODO[A3]: SessionRegistry.bind_peer")

    def touch(self, session_id: str) -> None:
        # TODO[A4]: cập nhật last_seen với traffic hợp lệ; không tin heartbeat giả.
        raise NotImplementedError("TODO[A4]: SessionRegistry.touch")

    def expire(self, now: float) -> list[str]:
        # TODO[A4]: loại session timeout, trả danh sách username cần thông báo offline.
        raise NotImplementedError("TODO[A4]: SessionRegistry.expire")
