"""Tiến độ nhận, hết hạn và sai hash qua mock; không có socket thật."""

import asyncio
import hashlib
import json
import tempfile
from pathlib import Path
from uuid import uuid4

from client.files import FileReceiver
from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    with tempfile.TemporaryDirectory() as directory:
        now = 0.0
        receiver = FileReceiver(Path(directory), clock=lambda: now)
        hub = MemoryHub()
        server = MemoryTransport(hub, ("server", 9001))
        local = MemoryTransport(hub, ("khoa", 10001))
        await server.start()
        await local.start()
        client = ClientService(local, server.address, "khoa", file_receiver=receiver)
        task = asyncio.create_task(client.receive_events())
        content = b"A" * 480 + b"B" * 480 + b"C" * 40
        metadata = dict(name="example.bin", size=1000, sha256=hashlib.sha256(content).hexdigest(),
                        chunk_size=480, total_chunks=3)

        async def send(transfer_id, kind, payload=b"", index=None):
            await server.send(local.address, Packet(
                kind=kind, sender="hai", recipient="khoa", stream_id=transfer_id,
                payload=payload, chunk_index=index,
            ))

        async def reply():
            return (await asyncio.wait_for(server.receive(), timeout=2)).packet

        async def wait_update(transfer_id, predicate):
            async with asyncio.timeout(3):
                while True:
                    for update in client.poll_file_updates():
                        if update["transfer_id"] == transfer_id and predicate(update):
                            print(f"{update['status']}: {update['received_bytes']}/{update['total_bytes']} byte"
                                  + (f" — {update['code']}" if "code" in update else ""))
                            return update
                    await asyncio.sleep(0.01)

        async def start(transfer_id, meta):
            await send(transfer_id, MessageType.FILE_META, json.dumps(meta).encode())
            assert (await reply()).kind == MessageType.FILE_ACCEPT

        async def chunk(transfer_id, index):
            await send(transfer_id, MessageType.FILE_CHUNK, content[index * 480:(index + 1) * 480], index)

        try:
            transfer_id = uuid4().hex
            await start(transfer_id, metadata)
            await wait_update(transfer_id, lambda u: u["status"] == "receiving")
            for index, count in ((2, 40), (0, 520)):
                await chunk(transfer_id, index)
                await wait_update(transfer_id, lambda u: u["received_bytes"] == count)
            await chunk(transfer_id, 0)  # Lặp lại: không được cộng thêm 480 byte.
            await chunk(transfer_id, 1)
            await wait_update(transfer_id, lambda u: u["received_bytes"] == 1000)
            await send(transfer_id, MessageType.FILE_END)
            assert (await reply()).kind == MessageType.FILE_COMPLETE
            await wait_update(transfer_id, lambda u: u["status"] == "completed"
                              and u["confirmation_delivery"] == "sent_to_server")

            transfer_id = uuid4().hex
            await start(transfer_id, metadata)
            now = 46.0  # Đồng hồ giả: không phải đợi 45 giây trong demo.
            await wait_update(transfer_id, lambda u: u["status"] == "expired")
            timeout_reply = await reply()
            assert json.loads(timeout_reply.payload)["code"] == "FILE_EXPIRED"
            print("Sender received ERROR FILE_EXPIRED on the control stream.")

            transfer_id = uuid4().hex
            await start(transfer_id, dict(metadata, sha256="0" * 64))
            for index in range(3):
                await chunk(transfer_id, index)
            await send(transfer_id, MessageType.FILE_END)
            assert (await reply()).kind == MessageType.ERROR
            await wait_update(transfer_id, lambda u: u["status"] == "failed")
            assert receiver.reserved_bytes == 0
            assert not list(receiver.output_directory.glob("*.part"))
            files = list(receiver.output_directory.iterdir())
            assert len(files) == 1 and files[0].read_bytes() == content
            print("Only files with a matching hash remain; no .part files or reserved quota.")
            print("Demo uses mocks and a fake clock; real UDP is not tested here.")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await local.close()
            await server.close()


if __name__ == "__main__":
    asyncio.run(main())
