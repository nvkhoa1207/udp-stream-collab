"""Demo phần nhận USER_LIST của Khoa qua mock, không mở TCP/UDP."""

import asyncio
import json

from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    hub = MemoryHub()
    server = MemoryTransport(hub, ("mock-server", 9001))
    local = MemoryTransport(hub, ("mock-khoa", 10001))
    await server.start()
    await local.start()
    client = ClientService(local, server.address, "khoa")
    receive_task = asyncio.create_task(client.receive_events())

    async def send_page(page, users):
        payload = json.dumps({
            "revision": 7, "page": page, "pages": 2, "users": users,
        }).encode("utf-8")
        await server.send(local.address, Packet(
            kind=MessageType.USER_LIST, sender="server", recipient="khoa",
            stream_id="control", payload=payload,
        ))

    try:
        await send_page(1, ["hai"])
        # Tin nhắn đến sau trang 1: vòng nhận phải tiếp tục, không chờ trang 0.
        await server.send(local.address, Packet(
            kind=MessageType.MSG, sender="hai", recipient="khoa",
            stream_id="chat-khoa", payload="Chào Khoa!".encode("utf-8"),
        ))
        event = await asyncio.wait_for(client.events.get(), timeout=1)
        assert event["type"] == "message"
        print("Có trang số 1, còn thiếu trang số 0: chưa phát danh sách online.")
        print("Vẫn nhận chat:", event["text"])

        await send_page(0, ["khoa"])
        event = await asyncio.wait_for(client.events.get(), timeout=1)
        assert event == {"type": "users", "revision": 7, "users": ["khoa", "hai"]}
        print("Đủ hai trang, revision:", event["revision"])
        print("Danh sách online:", ", ".join(event["users"]))
    finally:
        receive_task.cancel()
        try:
            await receive_task
        except asyncio.CancelledError:
            pass
        await local.close()
        await server.close()


if __name__ == "__main__":
    asyncio.run(main())
