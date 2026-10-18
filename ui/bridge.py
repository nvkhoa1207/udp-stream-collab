"""Owner Khoa, chỉ triển khai nếu giảng viên chấp thuận phương án HTTP bridge."""


def run_bridge() -> None:
    # TODO[K6]: localhost HTTP, cùng origin; nối event queue với ClientService.
    # Không block event loop UDP. Xem ui/README.md; không Flask/WebSockets.
    raise NotImplementedError("TODO[K6]: HTTP bridge, chờ chốt yêu cầu UI")
