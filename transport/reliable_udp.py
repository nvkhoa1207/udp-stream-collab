"""Bounded UDP endpoint shared by the client and server (no encryption)."""
import asyncio
import re
from dataclasses import dataclass, field, replace
from ipaddress import ip_address

from common.codec import decode_packet, encode_packet
from common.config import MAX_DATAGRAM_BYTES
from common.models import MessageType as M, Packet, ReceivedPacket


@dataclass
class _ReceiveStream:
    consumed: int = 0
    queued: int = 0
    packets: dict = field(default_factory=dict)


@dataclass
class _SendStream:
    sequence: int = 0
    error: Exception | None = None


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, owner):
        self.owner = owner

    def datagram_received(self, data, addr):
        try:
            if len(data) > MAX_DATAGRAM_BYTES:
                return
            self.owner._received(addr[:2], decode_packet(data))
        except (ValueError, KeyError, asyncio.QueueFull):
            self.owner.rejected += 1

    def error_received(self, exc):
        self.owner.last_socket_error = exc

    def connection_lost(self, exc):
        self.owner._lost.set()


class ReliableUdpTransport:
    def __init__(self, local_address, *, bootstrap=None, authorize=None,
                 retry_seconds=0.5, max_retries=8):
        self.local_address = local_address
        self.bootstrap = bootstrap
        self.authorize = authorize
        self._socket = None
        self._lost = asyncio.Event()
        self._closed = False
        self._peers = {}
        self._incoming = asyncio.Queue(maxsize=1024)
        self.rejected = 0
        self.last_socket_error = None
        self.retry_seconds = retry_seconds
        self.max_retries = max_retries
        self._tx = {}
        self._rx = {}
        self._pending = {}

    def register_peer(self, peer, session_id):
        host, port = peer
        if str(ip_address(host)) != host or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('Invalid peer endpoint')
        if type(session_id) is not str or re.fullmatch('[0-9a-f]{32}', session_id) is None:
            raise ValueError('Invalid session')
        if peer in self._peers and self._peers[peer] != session_id:
            raise ValueError('Endpoint already registered')
        if peer not in self._peers and len(self._peers) >= 100:
            raise ValueError('Peer quota reached')
        self._peers[peer] = session_id

    def remove_peer(self, peer):
        self._peers.pop(peer, None)
        for key in list(self._tx):
            if key[0] == peer:
                self._fail_stream(key, ConnectionError('Peer removed'))
                del self._tx[key]
        for key in list(self._rx):
            if key[0] == peer:
                del self._rx[key]

    def _fail_stream(self, key, error):
        if key in self._tx:
            self._tx[key].error = error
        for pending_key, future in list(self._pending.items()):
            if pending_key[:2] == key and not future.done():
                future.set_result(error)

    def _control(self, peer, packet, kind, **kwargs):
        self._write(peer, Packet(kind=kind, sender=packet.recipient if packet.recipient != '*' else 'server',
            recipient=packet.sender, stream_id=packet.stream_id,
            session_id=packet.session_id, **kwargs))

    async def start(self):
        if self._socket is not None or self._closed:
            raise RuntimeError('Transport already started or closed')
        loop = asyncio.get_running_loop()
        self._socket, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self), local_addr=self.local_address)
        self.local_address = self._socket.get_extra_info('sockname')[:2]

    def _write(self, peer, packet):
        if self._socket is None or self._closed:
            raise ConnectionError('Transport closed')
        self._socket.sendto(encode_packet(packet), peer)

    def _received(self, peer, packet):
        if self._closed:
            return
        if packet.kind == M.BIND and self.bootstrap is not None:
            self.bootstrap(peer, packet)
            return
        if self._peers.get(peer) != packet.session_id:
            raise ValueError('Unknown endpoint/session')
        if self.authorize is not None and not self.authorize(peer, packet):
            raise ValueError('Packet rejected by session policy')
        if packet.kind in (M.BIND_OK, M.HEARTBEAT):
            self._incoming.put_nowait(ReceivedPacket(peer, packet))
            return
        key = (peer, packet.stream_id)
        if packet.kind == M.ACK:
            future = self._pending.get((*key, packet.ack))
            if future is not None and not future.done():
                future.set_result(None)
            return
        if packet.kind in (M.WINDOW_UPDATE, M.WINDOW_PROBE):
            return
        state = self._rx.get(key)
        if state is None:
            if len(self._rx) >= 128:
                return
            state = self._rx[key] = _ReceiveStream()
        if packet.seq <= state.consumed or packet.seq in state.packets:
            self._control(peer, packet, M.ACK, ack=packet.seq, recv_limit=state.consumed + 8)
            return
        if packet.seq > state.consumed + 8:
            return
        state.packets[packet.seq] = packet
        self._queue_ready(key, state)
        self._control(peer, packet, M.ACK, ack=packet.seq, recv_limit=state.consumed + 8)

    def _queue_ready(self, key, state):
        while state.queued + 1 in state.packets and not self._incoming.full():
            state.queued += 1
            self._incoming.put_nowait(ReceivedPacket(key[0], state.packets[state.queued]))

    async def send(self, peer, packet):
        if peer not in self._peers:
            raise ValueError('Peer not registered')
        if self._closed:
            raise ConnectionError('Transport closed')
        packet = replace(packet, session_id=self._peers[peer])
        if packet.kind in (M.BIND, M.BIND_OK, M.HEARTBEAT):
            self._write(peer, packet)
            return
        if packet.kind in (M.ACK, M.WINDOW_PROBE, M.WINDOW_UPDATE):
            raise ValueError('Transport owns ACK/window packets')
        key = (peer, packet.stream_id)
        if key not in self._tx and len(self._tx) >= 512:
            raise ValueError('Send stream quota reached')
        state = self._tx.setdefault(key, _SendStream())
        if state.error is not None:
            raise state.error
        if len(self._pending) >= 1024:
            raise ValueError('Pending send quota reached')
        # Validate before reserving a sequence: malformed application packets cannot leave a hole.
        encode_packet(replace(packet, seq=state.sequence + 1))
        state.sequence += 1
        packet = replace(packet, seq=state.sequence)
        pending_key = (*key, packet.seq)
        future = asyncio.get_running_loop().create_future()
        self._pending[pending_key] = future
        try:
            for attempt in range(self.max_retries + 1):
                self._write(peer, packet)
                try:
                    result = await asyncio.wait_for(asyncio.shield(future),
                        min(4.0, self.retry_seconds * 2 ** attempt))
                    if result is not None:
                        raise result
                    return
                except TimeoutError:
                    if attempt == self.max_retries:
                        raise TimeoutError('UDP retry limit reached')
        except BaseException:
            self._fail_stream(key, ConnectionError('Stream send failed or cancelled'))
            raise
        finally:
            self._pending.pop(pending_key, None)

    async def receive(self):
        received = await self._incoming.get()
        if received is None:
            self._incoming.put_nowait(None)
            raise ConnectionError('Transport closed')
        key = (received.peer, received.packet.stream_id)
        state = self._rx.get(key)
        if received.packet.seq and state is not None:
            state.consumed = received.packet.seq
            state.packets.pop(state.consumed, None)
            self._queue_ready(key, state)
        return received

    async def close(self):
        if self._closed:
            return
        self._closed = True
        for key in self._tx:
            self._fail_stream(key, ConnectionError('Transport closed'))
        if self._socket is not None:
            self._socket.close()
            await self._lost.wait()
            self._socket = None
        self._peers.clear()
        self._rx.clear()
        self._tx.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
        self._incoming.put_nowait(None)
