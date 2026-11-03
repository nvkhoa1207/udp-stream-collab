"""Gửi UDP có ACK, gửi lại và giới hạn bộ đệm; envelope AES-GCM."""
import asyncio
from common.errors import StreamClosedError
import re
from dataclasses import dataclass, field, replace
from ipaddress import ip_address

from common.codec import decode_packet, encode_packet
from common.config import MAX_DATAGRAM_BYTES
from common.config import RECEIVE_WINDOW_PACKETS, MAX_RECEIVE_STREAMS
from common.models import MessageType, Packet, ReceivedPacket
from transport.congestion import CongestionController
from security.wire import OVERHEAD, open_record, seal_record


@dataclass
class _ReceiveStream:
    # 1. Quản lý con trỏ số thứ tự đã xử lý
    consumed: int = 0         # Số seq lớn nhất mà tầng ứng dụng đã lấy ra xử lý
    queued: int = 0           # Số seq lớn nhất đã được xếp vào hàng đợi chờ ứng dụng lấy

    # 2. Bộ đệm Selective Repeat lưu các gói đến lộn xộn
    packets: dict = field(default_factory=dict)  # Lưu gói tin theo dạng {seq: Packet}
    advertised: bool = False  # Đã thông báo cửa sổ nhận ban đầu cho bên gửi chưa


@dataclass
class _SendStream:
    # 1. Đánh số thứ tự và lưu vết lỗi
    sequence: int = 0                      # Số seq hiện tại của bên gửi (bắt đầu từ 1, tăng dần)
    error: Exception | None = None         # Lưu lỗi nếu luồng này bị đứt giữa chừng

    # 2. Quản lý cửa sổ nhận (Flow Control) và Thăm dò (Probe)
    limit: int = 0                         # Số seq tối đa được phép gửi (do bên nhận cấp qua recv_limit)
    changed: asyncio.Event = field(default_factory=asyncio.Event)  # Đánh thức luồng gửi khi limit tăng
    probe_at: float = 0.0                  # Thời điểm tiếp theo cần gửi gói WINDOW_PROBE nếu bị tắc


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, owner):
        self.owner = owner  # owner chính là đối tượng ReliableUdpTransport

    # 1. Card mạng nhận được 1 datagram UDP
    def datagram_received(self, data, addr):
        try:
            # Vứt bỏ ngay nếu gói tin vượt quá kích thước MTU 1200 byte
            if len(data) > MAX_DATAGRAM_BYTES:
                return
            # Mở phong bì giải mã AES-GCM (open_record) -> Giải mã JSON (decode_packet) -> Chuyển cho _received
            self.owner._received(addr[:2], decode_packet(open_record(data, b'UDP')))
        except (ValueError, KeyError, asyncio.QueueFull):
            self.owner.rejected += 1  # Tăng biến đếm các gói rác bị từ chối

    # 2. Ghi nhận lỗi socket hệ điều hành
    def error_received(self, exc):
        self.owner.last_socket_error = exc

    # 3. Báo hiệu khi socket bị đóng
    def connection_lost(self, exc):
        self.owner._lost.set()


class ReliableUdpTransport:
    def __init__(self, local_address, *, bootstrap=None, authorize=None,
                 retry_seconds=0.5, max_retries=8, probe_seconds=1.0,
                 stall_seconds=10.0, max_streams=MAX_RECEIVE_STREAMS):
        # 1. Địa chỉ và các hàm callback bảo mật
        self.local_address = local_address  # Địa chỉ IP/Port bind của socket UDP này
        self.bootstrap = bootstrap          # Hàm xử lý gói BIND ban đầu (ServerApp._bind)
        self.authorize = authorize          # Hàm kiểm tra tính hợp lệ từng gói (ServerApp._authorize)

        # 2. Quản lý socket và hàng đợi nhận
        self._socket = None                 # Đối tượng DatagramTransport của asyncio
        self._lost = asyncio.Event()        # Sự kiện socket bị đứt
        self._closed = False                # Cờ đánh dấu transport đã đóng
        self._close_task = None             # Task dọn dẹp đóng transport
        self._peers = {}                    # Lưu danh sách: {peer_address: session_id}
        self._incoming = asyncio.Queue(maxsize=1024)  # Hàng đợi gói tin sạch đã xếp đúng thứ tự
        self.rejected = 0                   # Đếm số gói bị loại bỏ
        self.last_socket_error = None

        # 3. Tham số cấu hình độ tin cậy
        self.retry_seconds = retry_seconds  # Thời gian chờ ACK cơ sở (0.5s)
        self.max_retries = max_retries      # Số lần thử gửi lại tối đa (8 lần)
        self.probe_seconds = probe_seconds  # Chu kỳ gửi gói thăm dò cửa sổ (1.0s)
        self.stall_seconds = stall_seconds  # Thời gian tối đa cho phép nghẽn trước khi timeout (10s)
        self.max_streams = max_streams      # Số luồng nhận tối đa cùng lúc (128)

        # 4. Bộ nhớ trạng thái gửi - nhận - chờ ACK
        self._tx = {}  # Trạng thái các luồng gửi, khóa là (địa chỉ, stream_id).
        self._rx = {}  # Trạng thái các luồng nhận, dùng cùng dạng khóa.
        self._pending = {}  # Các gói đang chờ ACK, thêm seq vào khóa.
        self._inflight = set()              # Tập hợp các khóa gói tin đang thực sự bay trên mạng
        self._send_completion = set()       # Tập hợp Future theo dõi việc hoàn tất gửi của các task
        self._tombstones = {}  # Dấu của luồng đã đóng để nhận ra gói đến muộn (Tombstone).
        self._draining = {}  # Luồng đang chờ xử lý hết gói trước khi đóng.

        # 5. Bộ điều khiển tắc nghẽn và luồng xử lý
        self._congestion = {}               # Mỗi peer sở hữu một CongestionController (CUBIC) riêng
        self._congestion_changed = {}       # Event đánh thức luồng gửi khi CUBIC mở thêm cửa sổ cwnd
        self._ready_blocked = set()         # Lưu các stream có gói sẵn sàng nhưng hàng đợi _incoming bị đầy
        self.delivery_ready = None          # Hook kiểm tra xem Router có sẵn sàng nhận tiếp không
        self._delivery_changed = asyncio.Event()  # Event đánh thức hàm receive() khi có gói mới
        self.identity = None                # Định danh người gửi (đặt bởi Server hoặc Client)
        self.last_received = 0.0            # Mốc thời gian nhận gói tin cuối cùng (tính heartbeat)

    # Đánh thức vòng lặp receive() khi có gói mới hoặc thay đổi trạng thái chuyển giao
    def notify_delivery(self):
        self._delivery_changed.set()

    def register_peer(self, peer, session_id):
        # 1. Kiểm tra hợp lệ đầu vào
        if self._closed:
            raise ConnectionError('Transport closed')
        host, port = peer
        if str(ip_address(host)) != host or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('Invalid peer endpoint')
        if type(session_id) is not str or re.fullmatch('[0-9a-f]{32}', session_id) is None:
            raise ValueError('Invalid session')
        if peer in self._peers and self._peers[peer] != session_id:
            raise ValueError('Endpoint already registered')
        if peer not in self._peers and len(self._peers) >= 100:
            raise ValueError('Peer quota reached')
        # 2. Cấp phát bộ đệm điều khiển và thuật toán CUBIC riêng cho peer này
        if peer not in self._peers:
            if len(self._rx) >= self.max_streams:
                raise ValueError('No reserved control buffer available')
            self._rx[(peer, 'control')] = _ReceiveStream()
        self._peers[peer] = session_id
        self._congestion.setdefault(peer, CongestionController())
        self._congestion_changed.setdefault(peer, asyncio.Event())

    def remove_peer(self, peer):
        # 1. Xóa peer và báo lỗi cho các luồng gửi đang dở dang
        self._peers.pop(peer, None)
        for key in list(self._tx):
            if key[0] == peer:
                self._fail_stream(key, ConnectionError('Peer removed'))
                del self._tx[key]
        # 2. Dọn sạch bộ nhớ đệm nhận, tombstone và CUBIC của peer này
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

    # Báo lỗi và hủy tất cả các gói đang chờ ACK thuộc về stream này
    def _fail_stream(self, key, error):
        if key in self._tx:
            self._tx[key].error = error
            self._tx[key].changed.set()
        for pending_key, future in list(self._pending.items()):
            if pending_key[:2] == key and not future.done():
                future.set_result(error)

    def finish_stream(self, peer, stream_id, *, success=True):
        # Đóng luồng khi xong và giữ số thứ tự cuối để nhận ra gói trùng.
        # Gói cũ đã xử lý vẫn được ACK nhưng không đưa lên ứng dụng lần nữa.
        # FileOfferReceiver giữ riêng phản hồi kết quả file trong 120 giây.
        if stream_id == 'control' or stream_id.startswith('chat-'):
            return
        key = (peer, stream_id)
        if key in self._tombstones or (key not in self._tx and key not in self._rx):
            return
        # 1. Nếu kết thúc thành công, chờ xả nốt các gói đang chờ ACK
        if success:
            self._draining[key] = success
            if any(k[:2] == key for k in self._pending) or (
                    key in self._rx and self._rx[key].packets):
                return
        # 2. Giải phóng bộ nhớ và cắm Tombstone lưu số seq cuối cùng
        self._draining.pop(key, None)
        rx = self._rx.pop(key, None)
        self._fail_stream(key, StreamClosedError('Stream closed'))
        self._tx.pop(key, None)
        self._tombstones[key] = (rx.consumed if rx else 0, success)

    # Kiểm tra hạn ngạch lịch sử stream của một peer (tối đa 4096 stream)
    def _can_reserve(self, peer, stream_id):
        if stream_id == 'control' or stream_id.startswith('chat-'):
            return True
        history = set()
        for address, name in (*self._tombstones, *self._rx, *self._tx):
            if address == peer and name != 'control' and not name.startswith('chat-'):
                history.add(name)
        return stream_id in history or len(history) < 4096

    # Bắn nhanh các gói tin điều khiển (ACK, WINDOW_UPDATE, WINDOW_PROBE)
    def _control(self, peer, packet, kind, **kwargs):
        sender = self.identity
        if not sender:
            sender = packet.recipient if packet.recipient != '*' else 'server'
        reply = Packet(
            kind=kind, sender=sender, recipient=packet.sender,
            stream_id=packet.stream_id, session_id=packet.session_id, **kwargs,
        )
        self._write(peer, reply)

    # Mở socket UDP bất đồng bộ trên cổng cục bộ
    async def start(self):
        if self._socket is not None or self._closed:
            raise RuntimeError('Transport already started or closed')
        loop = asyncio.get_running_loop()
        self._socket, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self), local_addr=self.local_address)
        self.local_address = self._socket.get_extra_info('sockname')[:2]

    # Đóng gói, mã hóa AES-GCM và đẩy thẳng xuống socket UDP
    def _write(self, peer, packet):
        if self._socket is None or self._closed:
            raise ConnectionError('Transport closed')
        data = seal_record(encode_packet(packet), b'UDP')
        if len(data) > MAX_DATAGRAM_BYTES:
            raise ValueError('Encrypted datagram exceeds size limit')
        self._socket.sendto(data, peer)

    def _received(self, peer, packet):
        if self._closed:
            return
        # 1. Xử lý gói bắt tay BIND ban đầu của Server
        if packet.kind == MessageType.BIND and self.bootstrap is not None:
            self.bootstrap(peer, packet)
            return
        # 2. Xác thực bảo mật IP/Port và Session ID
        if self._peers.get(peer) != packet.session_id:
            raise ValueError('Unknown endpoint/session')
        if self.authorize is not None and not self.authorize(peer, packet):
            raise ValueError('Packet rejected by session policy')
        self.last_received = asyncio.get_running_loop().time()
        # 3. Các gói điều khiển đơn giản không cần seq đưa thẳng lên hàng đợi
        if packet.kind in (MessageType.BIND_OK, MessageType.HEARTBEAT):
            self._incoming.put_nowait(ReceivedPacket(peer, packet))
            self.notify_delivery()
            return
        key = (peer, packet.stream_id)
        # 4. Xử lý khi nhận được gói ACK (Phía Gửi nhận ACK)
        if packet.kind == MessageType.ACK:
            ack_key = (*key, packet.ack)
            future = self._pending.get(ack_key) if ack_key in self._inflight else None
            if future is not None and not future.done():
                state = self._tx[key]
                state.limit = max(state.limit, packet.recv_limit)
                state.changed.set()
                future.set_result(None)
            return
        # 5. Xử lý khi nhận gói nới rộng cửa sổ WINDOW_UPDATE
        if packet.kind == MessageType.WINDOW_UPDATE:
            state = self._tx.get(key)
            if state is not None and packet.recv_limit > state.limit:
                state.limit = packet.recv_limit
                state.changed.set()
            return
        # 6. Xử lý gói tin bay lạc của luồng đã đóng (Tombstone)
        if key in self._tombstones:
            consumed, _ = self._tombstones[key]
            if packet.seq and packet.seq <= consumed:
                self._control(peer, packet, MessageType.ACK, ack=packet.seq, recv_limit=consumed)
            elif packet.kind == MessageType.WINDOW_PROBE:
                self._control(peer, packet, MessageType.WINDOW_UPDATE, recv_limit=0)
            return
        state = self._rx.get(key)
        # 7. Xử lý gói thăm dò cửa sổ WINDOW_PROBE
        if packet.kind == MessageType.WINDOW_PROBE:
            if state is None and len(self._rx) < self.max_streams and self._can_reserve(*key):
                state = self._rx[key] = _ReceiveStream()
            if state is not None:
                state.advertised = True
            self._control(peer, packet, MessageType.WINDOW_UPDATE,
                recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS if state else 0)
            return
        if state is None or not state.advertised:
            return
        # 8. Khử gói trùng lặp (Duplicate Packet)
        if packet.seq <= state.consumed or packet.seq in state.packets:
            self._control(peer, packet, MessageType.ACK, ack=packet.seq, recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)
            return
        if key in self._draining:
            return
        if packet.seq > state.consumed + RECEIVE_WINDOW_PACKETS:
            return
        # 9. Nhận và lưu vào bộ đệm Selective Repeat
        state.packets[packet.seq] = packet
        self._queue_ready(key, state)
        self._control(peer, packet, MessageType.ACK, ack=packet.seq, recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)

    def _queue_ready(self, key, state):
        # 1. Quét tuần tự: Chỉ đẩy vào _incoming khi gói tiếp theo (queued + 1) đã có mặt
        while state.queued + 1 in state.packets and not self._incoming.full():
            state.queued += 1
            self._incoming.put_nowait(ReceivedPacket(key[0], state.packets[state.queued]))
            self.notify_delivery()
        if state.queued + 1 in state.packets:
            self._ready_blocked.add(key)
        else:
            self._ready_blocked.discard(key)

    # Đẩy tiếp các gói bị nghẽn trước đó khi hàng đợi _incoming vừa có chỗ trống
    def _refill_ready(self):
        for key in tuple(self._ready_blocked):
            state = self._rx.get(key)
            if state is None:
                self._ready_blocked.discard(key)
            else:
                self._queue_ready(key, state)

    async def _wait_for_window(self, peer, packet, state):
        # 1. Chờ Flow Control: chừng nào seq vượt quá giới hạn bên nhận cho phép
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
            # Định kỳ mỗi 1.0s bắn gói WINDOW_PROBE để hỏi bên nhận xem đã có chỗ chưa
            if loop.time() >= state.probe_at:
                state.probe_at = loop.time() + self.probe_seconds
                probe = replace(
                    packet, kind=MessageType.WINDOW_PROBE,
                    seq=0, payload=b'', chunk_index=None,
                )
                self._write(peer, probe)
            try:
                wait_seconds = max(0.001, state.probe_at - loop.time())
                await asyncio.wait_for(state.changed.wait(), wait_seconds)
            except TimeoutError:
                pass

    async def _wait_for_budget(self, congestion, wake, size, state):
        # 1. Chờ Congestion Control: khi bytes_in_flight chạm trần cwnd hoặc chưa tới lịch Pacing
        loop = asyncio.get_running_loop()
        congestion_deadline = loop.time() + self.stall_seconds
        while congestion.budget_bytes() < size or loop.time() < congestion.next_send:
            if state.error is not None:
                raise state.error
            if loop.time() >= congestion_deadline:
                raise TimeoutError('Congestion budget stalled')
            wake.clear()
            if congestion.budget_bytes() >= size:
                delay = max(0.001, congestion.next_send - loop.time())
            else:
                delay = 0.1
            try:
                await asyncio.wait_for(wake.wait(), delay)
            except TimeoutError:
                pass

    # Động cơ gửi tin cậy Selective Repeat: kiểm soát flow/congestion và retry
    async def send(self, peer, packet):
        # A. Kiểm tra điều kiện ban đầu và gán số thứ tự seq
        if self._closed:
            raise ConnectionError('Transport closed')
        if peer not in self._peers:
            raise ValueError('Peer not registered')
        packet = replace(packet, session_id=self._peers[peer])
        if packet.kind in (MessageType.BIND, MessageType.BIND_OK, MessageType.HEARTBEAT):
            self._write(peer, packet)
            return
        if packet.kind in (MessageType.ACK, MessageType.WINDOW_PROBE, MessageType.WINDOW_UPDATE):
            raise ValueError('Transport owns ACK/window packets')
        key = (peer, packet.stream_id)
        if key in self._tombstones:
            raise StreamClosedError('Stream already closed')
        if key in self._draining:
            raise StreamClosedError('Stream draining')
        if not self._can_reserve(*key):
            raise ValueError('Session stream history quota reached; reconnect')
        if key not in self._tx and len(self._tx) >= 512:
            raise ValueError('Send stream quota reached')
        state = self._tx.get(key)
        if state is not None and state.error is not None:
            raise state.error
        if len(self._pending) >= 1024:
            raise ValueError('Pending send quota reached')
        # Kiểm tra trước khi tăng seq để gói lỗi không làm thiếu số thứ tự.
        encode_packet(replace(packet, seq=state.sequence + 1 if state else 1))
        if state is None:
            state = self._tx[key] = _SendStream()
        state.sequence += 1
        packet = replace(packet, seq=state.sequence)
        pending_key = (*key, packet.seq)
        future = asyncio.get_running_loop().create_future()
        completion = asyncio.get_running_loop().create_future()
        self._send_completion.add(completion)
        self._pending[pending_key] = future
        congestion = self._congestion[peer]
        wake = self._congestion_changed[peer]
        size = len(encode_packet(packet)) + OVERHEAD
        reserved = False
        try:
            loop = asyncio.get_running_loop()
            # B. Vượt qua 2 cổng kiểm soát (Flow Control & Congestion Control)
            await self._wait_for_window(peer, packet, state)
            await self._wait_for_budget(congestion, wake, size, state)
            ticket = congestion.on_sent(size)
            reserved = True
            sent_at = loop.time()
            if state.error is not None:
                raise state.error
            self._inflight.add(pending_key)
            # C. Vòng lặp retransmit (Exponential Backoff & Thuật toán Karn)
            for attempt in range(self.max_retries + 1):
                if attempt:
                    while loop.time() < congestion.next_send and not future.done():
                        try:
                            await asyncio.wait_for(asyncio.shield(future), congestion.next_send - loop.time())
                        except TimeoutError:
                            pass
                    if future.done():
                        result = future.result()
                        if result is not None:
                            raise result
                        congestion.on_ack(size, 0)
                        reserved = False
                        wake.set()
                        return
                    congestion.next_send = loop.time() + max(0.0001, congestion.rtt * size / congestion.cwnd)
                self._write(peer, packet)
                try:
                    result = await asyncio.wait_for(asyncio.shield(future),
                        min(4.0, self.retry_seconds * 2 ** attempt))
                    if result is not None:
                        raise result
                    # Thuật toán Karn: Chỉ tính mẫu RTT nếu gói này KHÔNG bị gửi lại (attempt == 0)
                    congestion.on_ack(size, loop.time() - sent_at if attempt == 0 else 0)
                    reserved = False
                    wake.set()
                    return
                except TimeoutError:
                    congestion.on_loss(size, ticket=ticket)
                    if attempt == self.max_retries:
                        raise TimeoutError('UDP retry limit reached')
        # D. Dọn dẹp tài nguyên bắt buộc khi xong hoặc lỗi
        except BaseException:
            self._fail_stream(key, ConnectionError('Stream send failed or cancelled'))
            self.finish_stream(peer, packet.stream_id, success=False)
            raise
        finally:
            if reserved:
                congestion.discard(size)
                wake.set()
            self._pending.pop(pending_key, None)
            self._inflight.discard(pending_key)
            if key in self._draining:
                self.finish_stream(*key, success=self._draining[key])
            if not completion.done():
                completion.set_result(None)
            self._send_completion.discard(completion)

    # Tầng ứng dụng lấy dữ liệu sạch, đúng thứ tự và nới rộng cửa sổ Flow Control
    async def receive(self):
        while True:
            # 1. Rút gói tin từ hàng đợi nội bộ _incoming
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
                # 2. Cập nhật tiêu thụ gói tin và nới rộng Flow Control
                if received.packet.seq:
                    state.consumed = received.packet.seq
                    state.packets.pop(state.consumed, None)
                    self._control(received.peer, received.packet, MessageType.WINDOW_UPDATE,
                                  recv_limit=state.consumed + RECEIVE_WINDOW_PACKETS)
                    self._queue_ready(key, state)
                    if key in self._draining:
                        self.finish_stream(*key, success=self._draining[key])
                self._refill_ready()
                return received
            self._delivery_changed.clear()
            await self._delivery_changed.wait()

    # Đóng transport an toàn (Graceful Shutdown)
    async def close(self):
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    # Dọn dẹp đóng socket và giải phóng toàn bộ hàng đợi trong RAM
    async def _close(self):
        # Đánh dấu lỗi tất cả các luồng gửi đang dở
        for key in self._tx:
            self._fail_stream(key, ConnectionError('Transport closed'))
        # Đóng socket hệ điều hành
        if self._socket is not None:
            self._socket.close()
            await self._lost.wait()
            self._socket = None
        # Đánh thức giải phóng tất cả các task đang ngủ chờ
        for wake in self._congestion_changed.values():
            wake.set()
        # Đợi tất cả các task gửi hoàn thành việc dọn dẹp
        if self._send_completion:
            await asyncio.gather(*(asyncio.shield(done) for done in tuple(self._send_completion)))
        # Xóa sạch toàn bộ từ điển trong RAM
        self._peers.clear()
        self._rx.clear()
        self._tx.clear()
        self._tombstones.clear()
        self._draining.clear()
        self._inflight.clear()
        self._ready_blocked.clear()
        self._congestion.clear()
        for wake in self._congestion_changed.values():
            wake.set()
        self._congestion_changed.clear()
        while not self._incoming.empty():
            self._incoming.get_nowait()
        self._incoming.put_nowait(None)
        self.notify_delivery()
