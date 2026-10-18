"""Transport giả trong MỘT process. Không socket, ACK, loss, auth hoặc mã hóa."""

import asyncio

from common.models import Packet, Peer, ReceivedPacket


class MemoryHub:
    def __init__(self) -> None:
        self.queues: dict[Peer, asyncio.Queue[ReceivedPacket]] = {}


class MemoryTransport:
    def __init__(self, hub: MemoryHub, address: Peer) -> None:
        self.hub = hub
        self.address = address
        self._queue: asyncio.Queue[ReceivedPacket] | None = None

    async def start(self) -> None:
        if self._queue is not None:
            raise RuntimeError("Transport đã start")
        if self.address in self.hub.queues:
            raise ValueError("Địa chỉ mock đã được sử dụng")
        self._queue = asyncio.Queue()
        self.hub.queues[self.address] = self._queue

    async def send(self, peer: Peer, packet: Packet) -> None:
        if self._queue is None:
            raise RuntimeError("Cần start trước send")
        target = self.hub.queues.get(peer)
        if target is None:
            raise ConnectionError(f"Mock peer chưa start: {peer}")
        await target.put(ReceivedPacket(peer=self.address, packet=packet))

    async def receive(self) -> ReceivedPacket:
        if self._queue is None:
            raise RuntimeError("Cần start trước receive")
        return await self._queue.get()

    async def close(self) -> None:
        if self._queue is not None:
            self.hub.queues.pop(self.address, None)
            self._queue = None
