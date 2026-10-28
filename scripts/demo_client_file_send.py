"""Hai ClientService truyền file qua relay bộ nhớ; không có socket hay ACK thật."""

import asyncio
import tempfile
from pathlib import Path

from client.files import FileReceiver, build_metadata
from client.service import ClientService
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "hello.bin"
        source.write_bytes(bytes(range(256)) * 20)
        receiver = FileReceiver(root / "received")
        hub = MemoryHub()
        server = MemoryTransport(hub, ("server", 9001))
        khoa_transport = MemoryTransport(hub, ("khoa", 10001))
        hai_transport = MemoryTransport(hub, ("hai", 10002))
        transports = [server, khoa_transport, hai_transport]
        for transport in transports:
            await transport.start()
        khoa = ClientService(khoa_transport, server.address, "khoa")
        hai = ClientService(hai_transport, server.address, "hai", file_receiver=receiver)
        addresses = {"khoa": khoa_transport.address, "hai": hai_transport.address}

        async def relay():
            while True:
                packet = (await server.receive()).packet
                if packet.kind.value != "FILE_CHUNK":
                    print(f"{packet.sender} → {packet.recipient}: {packet.kind.value}")
                await server.send(addresses[packet.recipient], packet)

        tasks = [asyncio.create_task(khoa.receive_events()),
                 asyncio.create_task(hai.receive_events()), asyncio.create_task(relay())]
        try:
            await asyncio.sleep(0)  # Cho hai vòng nhận khởi động.
            await asyncio.wait_for(khoa.send_file("hai", source), timeout=5)
            files = list(receiver.output_directory.iterdir())
            assert len(files) == 1
            assert files[0].read_bytes() == source.read_bytes()
            assert build_metadata(files[0])["sha256"] == build_metadata(source)["sha256"]
            assert receiver.reserved_bytes == 0
            print("send_file() hoàn tất: 5120 byte, nội dung và SHA-256 khớp.")
            print("Demo dùng mock; không chứng minh ACK/retry hay truyền UDP thật.")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for transport in transports:
                await transport.close()


if __name__ == "__main__":
    asyncio.run(main())
