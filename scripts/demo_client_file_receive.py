"""Client Khoa nhận META/CHUNK/END qua mock, lưu file rồi trả COMPLETE."""

import asyncio
import json
import tempfile
from pathlib import Path
from uuid import uuid4

from client.files import FileReceiver, build_metadata, iter_chunks
from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    with tempfile.TemporaryDirectory(prefix="btl-file-receive-") as folder:
        root = Path(folder)
        source = root / "sample.bin"
        source.write_bytes(b"A" * 480 + b"B" * 480 + b"C" * 40)
        metadata = build_metadata(source)
        chunks = list(iter_chunks(source))  # Chỉ dùng list cho file demo nhỏ.
        receiver = FileReceiver(root / "received")
        hub = MemoryHub()
        server = MemoryTransport(hub, ("mock-server", 9001))
        local = MemoryTransport(hub, ("mock-khoa", 10001))
        await server.start()
        await local.start()
        client = ClientService(local, server.address, "khoa", file_receiver=receiver)
        receive_task = asyncio.create_task(client.receive_events())
        transfer_id = uuid4().hex

        async def send(kind, payload=b"", index=None):
            await server.send(local.address, Packet(
                kind=kind, sender="hai", recipient="khoa", stream_id=transfer_id,
                payload=payload, chunk_index=index,
            ))

        async def reply():
            return (await asyncio.wait_for(server.receive(), timeout=2)).packet

        try:
            await send(MessageType.FILE_META, json.dumps(metadata).encode())
            assert (await reply()).kind == MessageType.FILE_ACCEPT
            print("META hợp lệ -> FILE_ACCEPT")

            for index in [2, 0, 0, 1]:
                await send(MessageType.FILE_CHUNK, chunks[index], index)
                print("Đưa vào client chunk:", index)
            await send(MessageType.FILE_END)
            complete = await reply()
            assert complete.kind == MessageType.FILE_COMPLETE
            assert json.loads(complete.payload) == {
                "size": metadata["size"], "sha256": metadata["sha256"],
            }
            target = receiver.output_directory / f"{transfer_id}_{metadata['name']}"
            assert target.read_bytes() == source.read_bytes()
            assert receiver.reserved_bytes == 0
            assert not list(receiver.output_directory.glob("*.part"))
            print("FILE_COMPLETE: đủ 1000 byte, hash đúng, nội dung khớp.")

            await send(MessageType.FILE_END)
            assert await reply() == complete
            assert len(list(receiver.output_directory.iterdir())) == 1
            print("END lặp: trả lại COMPLETE, không tạo thêm file.")
        finally:
            receive_task.cancel()
            try:
                await receive_task
            except asyncio.CancelledError:
                pass
            await local.close()
            await server.close()
        print("Demo qua mock; thư mục tạm sẽ được dọn khi kết thúc.")


if __name__ == "__main__":
    asyncio.run(main())
