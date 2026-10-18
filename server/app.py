"""Owner A — socket server, vòng đời async và ghép các module."""


class ServerApp:
    async def run(self) -> None:
        # TODO[A2]: TCP listener, handshake/framing; asyncio hoặc thread.
        # TODO[A3]: auth/TLS; tạo session, cấp khóa bằng kênh được bảo vệ.
        # TODO[A4]: timer heartbeat, USER_LIST chia trang.
        # TODO[A5]: một vòng nhận UDP dispatch sang router; xử lý nhiều peer.
        # TODO[A7]: giới hạn task/buffer, try/finally, timeout và cleanup.
        raise NotImplementedError("TODO[A2-A7]: ServerApp.run")
