"""Giả lập truyền gói bằng hàng đợi trong RAM, không dùng mạng thật."""

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
            raise RuntimeError("Transport has already started")
        if self.address in self.hub.queues:
            raise ValueError("Mock address is already in use")
        self._queue = asyncio.Queue()
        self.hub.queues[self.address] = self._queue

    async def send(self, peer: Peer, packet: Packet) -> None:
        if self._queue is None:
            raise RuntimeError("Start transport before sending")
        target = self.hub.queues.get(peer)
        if target is None:
            raise ConnectionError(f"Mock peer has not started: {peer}")
        await target.put(ReceivedPacket(peer=self.address, packet=packet))

    async def receive(self) -> ReceivedPacket:
        if self._queue is None:
            raise RuntimeError("Start transport before receiving")
        return await self._queue.get()

    async def close(self) -> None:
        if self._queue is not None:
            self.hub.queues.pop(self.address, None)
            self._queue = None
