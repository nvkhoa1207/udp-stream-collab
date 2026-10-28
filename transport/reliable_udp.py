"""Bounded UDP endpoint shared by the client and server (no encryption)."""
import asyncio
import re
from dataclasses import dataclass, field, replace
from ipaddress import ip_address

from common.codec import decode_packet, encode_packet
from common.config import MAX_DATAGRAM_BYTES
from common.config import RECEIVE_WINDOW_PACKETS, MAX_RECEIVE_STREAMS
from common.models import MessageType as M, Packet, ReceivedPacket
from transport.congestion import CongestionController


@dataclass
class _ReceiveStream:
    consumed: int = 0
    queued: int = 0
    packets: dict = field(default_factory=dict)


@dataclass
class _SendStream:
    sequence: int = 0
    error: Exception | None = None
    limit: int = 0
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    probe_at: float = 0.0


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
                 retry_seconds=0.5, max_retries=8, probe_seconds=1.0,
                 stall_seconds=10.0, max_streams=MAX_RECEIVE_STREAMS):
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
        self._tombstones = {}
        self._draining = {}
        self.probe_seconds = probe_seconds
        self.stall_seconds = stall_seconds
        self.max_streams = max_streams
        self._congestion = {}
        self._congestion_changed = {}
        self._ready_blocked = set()
        self.delivery_ready = None
        self._delivery_changed = asyncio.Event()
        self.identity = None
        self.last_received = 0.0

    def notify_delivery(self):
        self._delivery_changed.set()

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
        if peer not in self._peers:
            if len(self._rx) >= self.max_streams:
                raise ValueError('No reserved control buffer available')
            self._rx[(peer, 'control')] = _ReceiveStream()
        self._peers[peer] = session_id
        self._congestion.setdefault(peer, CongestionController())
        self._congestion_changed.setdefault(peer, asyncio.Event())

    def remove_peer(self, peer):
        self._peers.pop(peer, None)
        for key in list(self._tx):
            if key[0] == peer:
                self._fail_stream(key, ConnectionError('Peer removed'))
                del self._tx[key]
        for key in list(self._rx):
            if key[0] == peer:
                del self._rx[key]
        for key in list(self._tombstones):
            if key[0] == peer:
                del self._tombstones[key]
        for key in list(self._draining):
            if key[0] == peer:
                del self._draining[key]
        self._congestion.pop(peer, None)
        self._ready_blocked = {k for k in self._ready_blocked if k[0] != peer}
        wake = self._congestion_changed.pop(peer, None)
        if wake is not None:
            wake.set()

    def _fail_stream(self, key, error):
        if key in self._tx:
            self._tx[key].error = error
            self._tx[key].changed.set()
        for pending_key, future in list(self._pending.items()):
            if pending_key[:2] == key and not future.done():
                future.set_result(error)

    def finish_stream(self, peer, stream_id, *, success=True):
        """Close both directions after application completion; keep a small tombstone.

        File result payload caches live in FileOfferReceiver (120 seconds).
        Late delivered DATA can still be ACKed, never delivered a second time.
        """
        if stream_id == 'control' or stream_id.startswith('chat-'):
            return
        key = (peer, stream_id)
        if key in self._tombstones or (key not in self._tx and key not in self._rx):
            return
        if success:
            self._draining[key] = success
            if any(k[:2] == key for k in self._pending) or (
                    key in self._rx and self._rx[key].packets):
                return
        self._draining.pop(key, None)
        rx = self._rx.pop(key, None)
        self._fail_stream(key, ConnectionError('Stream closed'))
        self._tx.pop(key, None)
        self._tombstones[key] = (rx.consumed if rx else 0, success)

    def _can_reserve(self, peer, stream_id):
        history = {k[1] for k in (*self._tombstones, *self._rx, *self._tx) if k[0] == peer}
        return stream_id in history or len(history) < 4096

    def _control(self, peer, packet, kind, **kwargs):
        self._write(peer, Packet(kind=kind, sender=self.identity or (packet.recipient if packet.recipient != '*' else 'server'),
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
        self.last_received = asyncio.get_running_loop().time()
        if packet.kind in (M.BIND_OK, M.HEARTBEAT):
            self._incoming.put_nowait(ReceivedPacket(peer, packet))
            self.notify_delivery()
            return
        key = (peer, packet.stream_id)
        if packet.kind == M.ACK:
            future = self._pending.get((*key, packet.ack))
            if future is not None and not future.done():
                state = self._tx[key]
                state.limit = max(state.limit, packet.recv_limit)
                state.changed.set()
                future.set_result(None)
            return
        if packet.kind == M.WINDOW_UPDATE:
            state = self._tx.get(key)
            if state is not None and packet.recv_limit > state.limit:
                state.limit = packet.recv_limit
                state.changed.set()
            return
        if key in self._tombstones:
            consumed, _ = self._tombstones[key]
            if packet.seq and packet.seq <= consumed:
                self._control(peer, packet, M.ACK, ack=packet.seq, recv_limit=consumed)
            elif packet.kind == M.WINDOW_PROBE:
                self._control(peer, packet, M.WINDOW_UPDATE, recv_limit=0)
            return
        state = self._rx.get(key)
        if packet.kind == M.WINDOW_PROBE:
            if state is None and len(self._rx) < self.max_streams and self._can_reserve(*key):
                state = self._rx[key] = _ReceiveStream()
            self._control(peer, packet, M.WINDOW_UPDATE,
                recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS if state else 0)
            return
        if state is None:
            return
        if packet.seq <= state.consumed or packet.seq in state.packets:
            self._control(peer, packet, M.ACK, ack=packet.seq, recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)
            return
        if key in self._draining:
            return
        if packet.seq > state.consumed + RECEIVE_WINDOW_PACKETS:
            return
        state.packets[packet.seq] = packet
        self._queue_ready(key, state)
        self._control(peer, packet, M.ACK, ack=packet.seq, recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)

    def _queue_ready(self, key, state):
        while state.queued + 1 in state.packets and not self._incoming.full():
            state.queued += 1
            self._incoming.put_nowait(ReceivedPacket(key[0], state.packets[state.queued]))
            self.notify_delivery()
        if state.queued + 1 in state.packets:
            self._ready_blocked.add(key)
        else:
            self._ready_blocked.discard(key)

    def _refill_ready(self):
        for key in tuple(self._ready_blocked):
            state = self._rx.get(key)
            if state is None:
                self._ready_blocked.discard(key)
            else:
                self._queue_ready(key, state)

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
        if key in self._tombstones:
            raise ConnectionError('Stream already closed')
        if key in self._draining:
            raise ConnectionError('Stream draining')
        if not self._can_reserve(*key):
            raise ValueError('Session stream history quota reached; reconnect')
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
        cc = self._congestion[peer]
        wake = self._congestion_changed[peer]
        size = len(encode_packet(packet))
        reserved = False
        try:
            loop = asyncio.get_running_loop()
            progress_at, old_limit = loop.time(), state.limit
            while packet.seq > state.limit:
                if state.error is not None:
                    raise state.error
                if state.limit > old_limit:
                    progress_at, old_limit = loop.time(), state.limit
                if loop.time() - progress_at >= self.stall_seconds:
                    raise TimeoutError('Receive window stalled')
                state.changed.clear()
                if loop.time() >= state.probe_at:
                    state.probe_at = loop.time() + self.probe_seconds
                    self._write(peer, replace(packet, kind=M.WINDOW_PROBE, seq=0, payload=b'', chunk_index=None))
                try:
                    await asyncio.wait_for(state.changed.wait(), max(0.001, state.probe_at - loop.time()))
                except TimeoutError:
                    pass
            congestion_deadline = loop.time() + self.stall_seconds
            while cc.budget_bytes() < size or loop.time() < cc.next_send:
                if state.error is not None:
                    raise state.error
                if loop.time() >= congestion_deadline:
                    raise TimeoutError('Congestion budget stalled')
                wake.clear()
                delay = max(0.001, cc.next_send - loop.time()) if cc.budget_bytes() >= size else 0.1
                try:
                    await asyncio.wait_for(wake.wait(), delay)
                except TimeoutError:
                    pass
            ticket = cc.on_sent(size)
            reserved = True
            sent_at = loop.time()
            for attempt in range(self.max_retries + 1):
                if attempt:
                    delay = max(0, cc.next_send - loop.time())
                    if delay:
                        await asyncio.sleep(delay)
                    cc.next_send = loop.time() + max(0.0001, cc.rtt * size / cc.cwnd)
                self._write(peer, packet)
                try:
                    result = await asyncio.wait_for(asyncio.shield(future),
                        min(4.0, self.retry_seconds * 2 ** attempt))
                    if result is not None:
                        raise result
                    cc.on_ack(size, loop.time() - sent_at if attempt == 0 else 0)
                    reserved = False
                    wake.set()
                    return
                except TimeoutError:
                    cc.on_loss(size, ticket=ticket)
                    if attempt == self.max_retries:
                        raise TimeoutError('UDP retry limit reached')
        except BaseException:
            self._fail_stream(key, ConnectionError('Stream send failed or cancelled'))
            self.finish_stream(peer, packet.stream_id, success=False)
            raise
        finally:
            if reserved:
                cc.discard(size)
                wake.set()
            self._pending.pop(pending_key, None)
            if key in self._draining:
                self.finish_stream(*key, success=self._draining[key])

    async def receive(self):
        while True:
            for _ in range(self._incoming.qsize()):
                received = self._incoming.get_nowait()
                if received is None:
                    self._incoming.put_nowait(None)
                    raise ConnectionError('Transport closed')
                key = (received.peer, received.packet.stream_id)
                if self._peers.get(received.peer) != received.packet.session_id:
                    self._refill_ready()
                    continue
                state = self._rx.get(key)
                if received.packet.seq and state is None:
                    self._refill_ready()
                    continue
                if ((received.packet.seq and received.packet.seq != state.consumed + 1)
                        or (self.delivery_ready is not None and not self.delivery_ready(received))):
                    self._incoming.put_nowait(received)
                    continue
                if received.packet.seq:
                    state.consumed = received.packet.seq
                    state.packets.pop(state.consumed, None)
                    self._control(received.peer, received.packet, M.WINDOW_UPDATE,
                                  recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)
                    self._queue_ready(key, state)
                    if key in self._draining:
                        self.finish_stream(*key, success=self._draining[key])
                self._refill_ready()
                return received
            self._delivery_changed.clear()
            await self._delivery_changed.wait()

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
        self._tombstones.clear()
        self._draining.clear()
        self._ready_blocked.clear()
        self._congestion.clear()
        for wake in self._congestion_changed.values():
            wake.set()
        self._congestion_changed.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
        self._incoming.put_nowait(None)
        self.notify_delivery()
