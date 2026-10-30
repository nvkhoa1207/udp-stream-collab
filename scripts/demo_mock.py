"""Chạy: python -m scripts.demo_mock. Demo hợp đồng, KHÔNG thử mạng thật."""

import asyncio

from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main() -> None:
    print("MOCK DEMO: in-memory queues; no sockets, authentication or encryption.")
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
            payload="Hello, this is sample data for module integration.".encode("utf-8"),
        )
        await khoa.send(server.address, packet)
        at_server = await asyncio.wait_for(server.receive(), timeout=1)
        print(f"1. Mock server received {at_server.packet.kind.value} from {at_server.peer}")
        # Chuyển tiếp cố định cho bài thử; ứng dụng thật dùng server.router.
        await server.send(friend.address, at_server.packet)
        at_friend = await asyncio.wait_for(friend.receive(), timeout=1)
        assert at_friend.packet == packet
        assert at_friend.peer == server.address
        print("2. Peer client received:", at_friend.packet.payload.decode("utf-8"))
        print("3. message_id is preserved; the immediate sender is the mock server.")
        print("Mock contract OK. Next: TODO_BAN_A.md / TODO_KHOA.md.")
    finally:
        for endpoint in reversed(endpoints):
            await endpoint.close()


if __name__ == "__main__":
    asyncio.run(main())
