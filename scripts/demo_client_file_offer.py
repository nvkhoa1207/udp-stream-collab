"""META -> chuẩn bị .part -> ACCEPT/ERROR, qua mock và thư mục tạm."""

import asyncio
import json
import tempfile
from pathlib import Path
from uuid import uuid4

from client.files import FileReceiver, build_metadata
from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


async def main():
    with tempfile.TemporaryDirectory(prefix="btl-file-offer-") as folder:
        root = Path(folder)
        original = root / "hello.txt"
        original.write_bytes(b"Hello Khoa!")
        metadata = build_metadata(original)
        receiver = FileReceiver(root / "received")

        hub = MemoryHub()
        server = MemoryTransport(hub, ("mock-server", 9001))
        local = MemoryTransport(hub, ("mock-khoa", 10001))
        await server.start()
        await local.start()
        client = ClientService(local, server.address, "khoa", file_receiver=receiver)
        receive_task = asyncio.create_task(client.receive_events())
        transfer_id = uuid4().hex

        async def send_meta(meta, stream_id):
            await server.send(local.address, Packet(
                kind=MessageType.FILE_META, sender="hai", recipient="khoa",
                stream_id=stream_id, payload=json.dumps(meta).encode("utf-8"),
            ))
            reply = await asyncio.wait_for(server.receive(), timeout=2)
            return reply.packet

        try:
            print("Mock server forwards FILE_META from Hai to Khoa.")
            reply = await send_meta(metadata, transfer_id)
            assert reply.kind == MessageType.FILE_ACCEPT
            assert reply.recipient == "hai" and reply.stream_id == transfer_id
            assert (receiver.output_directory / f"{transfer_id}.part").exists()
            print("Khoa created a .part file and sent FILE_ACCEPT to Hai through the server.")
            print("Reserved bytes:", receiver.reserved_bytes, "byte")

            reply = await send_meta(dict(metadata, name="../bad.txt"), uuid4().hex)
            assert reply.kind == MessageType.ERROR
            print("Invalid file name ->", json.loads(reply.payload)["code"])
            assert list(receiver.transfers) == [transfer_id]
            print("The earlier valid incoming transfer is preserved.")
        finally:
            receive_task.cancel()
            try:
                await receive_task
            except asyncio.CancelledError:
                pass
            await local.close()
            await server.close()

        assert receiver.reserved_bytes == 0
        assert not list(receiver.output_directory.glob("*.part"))
        print("Client stopped: .part removed and quota released. CHUNK/END were not sent.")


if __name__ == "__main__":
    asyncio.run(main())
