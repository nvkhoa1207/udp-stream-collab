"""Minh họa 100% vẫn chờ COMPLETE, và hủy file đang gửi bằng mock."""

import asyncio
import json
import tempfile
from pathlib import Path

from client.file_cancel import FileTransferCancelled
from client.files import FileReceiver
from client.service import ClientService
from common.models import MessageType
from mocks.memory_transport import MemoryHub, MemoryTransport


class DemoTransport(MemoryTransport):
    pause_after_four = False

    async def send(self, peer, packet):
        if self.pause_after_four and packet.kind == MessageType.FILE_CHUNK and packet.chunk_index >= 4:
            await asyncio.Event().wait()  # Cố ý chặn để minh họa nút hủy.
        await super().send(peer, packet)


def show(update):
    print(f"{update['status']}: {update['acknowledged_bytes']}/{update['total_bytes']} byte")


async def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "example.bin"
        source.write_bytes(bytes(range(256)) * 20)
        hub = MemoryHub()
        server = MemoryTransport(hub, ("server", 9001))
        local = DemoTransport(hub, ("khoa", 10001))
        remote = MemoryTransport(hub, ("hai", 10002))
        for transport in (server, local, remote):
            await transport.start()
        receiver = FileReceiver(root / "received")
        khoa = ClientService(local, server.address, "khoa")
        hai = ClientService(remote, server.address, "hai", file_receiver=receiver)
        complete_waiting, release_complete, cancelled = (asyncio.Event() for _ in range(3))

        async def relay():
            while True:
                packet = (await server.receive()).packet
                if packet.kind == MessageType.FILE_COMPLETE:
                    complete_waiting.set()
                    await release_complete.wait()
                if packet.kind == MessageType.ERROR and json.loads(packet.payload)["code"] == "FILE_CANCELLED":
                    cancelled.set()
                address = remote.address if packet.recipient == "hai" else local.address
                await server.send(address, packet)

        tasks = [asyncio.create_task(khoa.receive_events()),
                 asyncio.create_task(hai.receive_events()), asyncio.create_task(relay())]
        try:
            await asyncio.sleep(0)
            sending = asyncio.create_task(khoa.send_file("hai", source))
            tasks.append(sending)
            await asyncio.wait_for(complete_waiting.wait(), timeout=3)
            update = khoa.poll_file_updates()[0]
            assert update["status"] == "verifying" and update["acknowledged_bytes"] == 5120
            assert not sending.done()
            show(update)
            print("Đã chuyển 100% tới server giả; vẫn chờ COMPLETE.")
            release_complete.set()
            await sending
            show(khoa.poll_file_updates()[0])

            local.pause_after_four = True
            sending = asyncio.create_task(khoa.send_file("hai", source))
            tasks.append(sending)
            async with asyncio.timeout(3):
                while True:
                    updates = khoa.poll_file_updates()
                    if updates and updates[0]["acknowledged_bytes"] == 1920:
                        update = updates[0]
                        break
                    await asyncio.sleep(0.01)
            show(update)
            await khoa.cancel_file(update["transfer_id"])
            try:
                await sending
            except FileTransferCancelled:
                pass
            else:
                raise AssertionError("Lượt hủy không được báo thành công")
            await asyncio.wait_for(cancelled.wait(), timeout=3)
            show(khoa.poll_file_updates()[0])
            assert not list(receiver.output_directory.glob("*.part"))
            assert receiver.reserved_bytes == 0
            files = list(receiver.output_directory.iterdir())
            assert len(files) == 1 and files[0].read_bytes() == source.read_bytes()
            print("Bên nhận đã dọn .part và trả quota; file hoàn tất trước đó vẫn còn.")
            print("Demo dùng mock, không có ACK/UDP thật. Thư mục thử được tự dọn.")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for transport in (local, remote, server):
                await transport.close()


if __name__ == "__main__":
    asyncio.run(main())
