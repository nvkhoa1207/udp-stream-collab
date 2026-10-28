"""Bounded UDP endpoint shared by the client and server (no encryption)."""
import asyncio
import re
from dataclasses import replace
from ipaddress import ip_address

from common.codec import decode_packet, encode_packet
from common.config import MAX_DATAGRAM_BYTES
from common.models import MessageType as M, Packet, ReceivedPacket


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
    def __init__(self, local_address, *, bootstrap=None, authorize=None):
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

    async def send(self, peer, packet):
        if peer not in self._peers:
            raise ValueError('Peer not registered')
        if packet.kind not in (M.BIND, M.BIND_OK, M.HEARTBEAT):
            raise ValueError('Reliable DATA requires sequence/ACK support')
        self._write(peer, replace(packet, session_id=self._peers[peer]))

    async def receive(self):
        received = await self._incoming.get()
        if received is None:
            self._incoming.put_nowait(None)
            raise ConnectionError('Transport closed')
        return received

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._socket is not None:
            self._socket.close()
            await self._lost.wait()
            self._socket = None
        self._peers.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
        self._incoming.put_nowait(None)
