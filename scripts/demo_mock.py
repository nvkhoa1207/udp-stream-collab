"""Chạy: python -m scripts.demo_mock. Demo hợp đồng, KHÔNG thử mạng thật."""

import asyncio

from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main() -> None:
    print("DEMO GIẢ: queue trong bộ nhớ; không có socket, auth hay mã hóa.")
    hub = MemoryHub()
    server = MemoryTransport(hub, ("mock-server", 9001))
    khoa = MemoryTransport(hub, ("mock-khoa", 10001))
    friend = MemoryTransport(hub, ("mock-friend", 10002))
    endpoints = (server, khoa, friend)
    for endpoint in endpoints:
        await endpoint.start()
    try:
        packet = Packet(
            kind=MessageType.MSG,
            sender="khoa",
            recipient="friend",
            stream_id="chat-khoa-friend",
            payload="Chào bạn, đây là dữ liệu giả để ghép module.".encode("utf-8"),
        )
        await khoa.send(server.address, packet)
        at_server = await asyncio.wait_for(server.receive(), timeout=1)
        print(f"1. Server giả nhận {at_server.packet.kind.value} từ {at_server.peer}")
        # Chuyển tiếp cố định chỉ để minh họa. Router thật do A tự triển khai.
        await server.send(friend.address, at_server.packet)
        at_friend = await asyncio.wait_for(friend.receive(), timeout=1)
        assert at_friend.packet == packet
        assert at_friend.peer == server.address
        print("2. Client bạn nhận:", at_friend.packet.payload.decode("utf-8"))
        print("3. message_id giữ nguyên; chặng gửi trực tiếp là server giả.")
        print("OK hợp đồng mock. Tiếp theo: TODO_BAN_A.md / TODO_KHOA.md.")
    finally:
        for endpoint in reversed(endpoints):
            await endpoint.close()


if __name__ == "__main__":
    asyncio.run(main())
