import asyncio

from client.service import ClientService
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    hub = MemoryHub()

    server = MemoryTransport(
        hub, ("mock-server", 9001)
    )

    khoa_transport = MemoryTransport(
        hub, ("mock-khoa", 10001)
    )

    friend_transport = MemoryTransport(
        hub, ("mock-friend", 10002)
    )

    transports = [
        server,
        khoa_transport,
        friend_transport,
    ]

    for transport in transports:
        await transport.start()

    khoa = ClientService(
        khoa_transport, server.address, "khoa"
    )

    friend = ClientService(
        friend_transport, server.address, "friend"
    )

    # Cho client friend chờ nhận tin ở nền.
    receive_task = asyncio.create_task(
        friend.receive_events()
    )

    try:
        # Khoa gửi tin đến server.
        await khoa.send_chat(
            "friend", "Hello, I am Khoa."
        )

        # Server giả nhận tin.
        received = await asyncio.wait_for(
            server.receive(), timeout=2
        )

        packet = received.packet

        print(
            f"Mock server received a message from {packet.sender}, "
            f"sent to {packet.recipient}."
        )

        # Chuyển tiếp tới friend trong bài thử này.
        await server.send(
            friend_transport.address, packet
        )

        # Lấy sự kiện mà client friend đã nhận.
        event = await asyncio.wait_for(
            friend.events.get(), timeout=2
        )
        assert event == {
            "type": "message",
            "sender": "khoa",
            "recipient": "friend",
            "text": "Hello, I am Khoa.",
        }

        print(
            f"friend received from {event['sender']}: "
            f"{event['text']}"
        )

    finally:
        # Dừng tác vụ nhận tin khi kết thúc demo.
        receive_task.cancel()

        try:
            await receive_task
        except asyncio.CancelledError:
            pass

        for transport in transports:
            await transport.close()


if __name__ == "__main__":
    asyncio.run(main())
