"""Kiểm tra server và client thật qua địa chỉ loopback.

File và tài khoản chỉ dùng cho bài thử, thư mục tạm được dọn sau khi chạy.
Lỗi mạng được chèn lúc gửi; gói không bị bỏ vẫn đi qua socket UDP thật.
"""
import argparse
import asyncio
import hashlib
import json
import random
import tempfile
import time
from pathlib import Path

from client.files import FileReceiver
from client.service import ClientService
from common.models import MessageType as M
from server.app import ServerApp
from server.auth import AuthService
from transport.reliable_udp import ReliableUdpTransport


class Faults:
    def __init__(self, seed, loss, delay):
        self.random = random.Random(seed)
        self.loss, self.delay = loss, delay
        self.handles = set()
        self.sent = self.dropped = self.duplicated = 0

    def install(self, transport):
        write = transport._write
        def send(peer, packet):
            self.sent += 1
            if packet.kind not in (M.HEARTBEAT, M.BIND, M.BIND_OK):
                if self.random.random() < self.loss:
                    self.dropped += 1
                    return
                if len(self.handles) >= 4096:
                    raise ValueError('Fault scheduling quota reached')
                if self.delay:
                    handle = None
                    duplicate = self.loss and self.sent % 37 == 0
                    if duplicate:
                        self.duplicated += 1
                    def deliver():
                        self.handles.discard(handle)
                        if not transport._closed:
                            write(peer, packet)
                            if duplicate:
                                write(peer, packet)
                    handle = asyncio.get_running_loop().call_later(
                        self.delay * self.random.uniform(0.3, 1.7), deliver)
                    self.handles.add(handle)
                    return
            write(peer, packet)
            if self.loss and self.sent % 37 == 0:
                self.duplicated += 1
                write(peer, packet)
        transport._write = send

    def close(self):
        for handle in self.handles:
            handle.cancel()
        self.handles.clear()


async def wait_until(predicate, timeout=15):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def run(args):
    start = time.monotonic()
    faults = Faults(args.seed, args.loss, args.delay)
    auth = AuthService()
    for i in range(args.clients):
        await asyncio.to_thread(auth.add_user, f'user{i:02}', 'test-password')
    app = ServerApp(tcp_port=0, udp_port=0, auth=auth, enable_udp=True)
    await app.start()
    app.transport.retry_seconds = 0.04
    app.transport.probe_seconds = 0.04
    faults.install(app.transport)
    clients, pumps, messages = [], [], [set() for _ in range(args.clients)]
    with tempfile.TemporaryDirectory(prefix='btl-network-') as directory:
        root = Path(directory)
        try:
            for i in range(args.clients):
                transport = ReliableUdpTransport(('127.0.0.1', 0), retry_seconds=0.04, probe_seconds=0.04)
                faults.install(transport)
                clients.append(ClientService(transport, app.transport.local_address, f'user{i:02}',
                    tcp_peer=app.address, file_receiver=FileReceiver(root / f'received{i}')))
            await asyncio.gather(*(c.login('test-password') for c in clients))
            async def pump(index, client):
                while True:
                    event = await client.events.get()
                    if event['type'] == 'message':
                        messages[index].add(event['text'])
                    client.transport.notify_delivery()
            pumps = [asyncio.create_task(pump(i, c)) for i, c in enumerate(clients)]
            await wait_until(lambda: all(c.user_list.complete and c.user_list.revision == args.clients for c in clients))
            await clients[0].send_chat('*', 'twenty-client broadcast')
            await wait_until(lambda: all('twenty-client broadcast' in m for m in messages[1:]))
            await asyncio.gather(*(c.send_chat('user01', f'concurrent-{i}')
                                  for i, c in enumerate(clients) if i != 1))
            await wait_until(lambda: len(messages[1]) >= args.clients)
            print(json.dumps(dict(stage='online/chat', clients=len(clients), seconds=round(time.monotonic()-start, 3))), flush=True)
            sender, receiver = clients[:2]

            async def transfer(size, name, verbose=True):
                path = root / name
                def create():
                    digest = hashlib.sha256()
                    with path.open('wb') as output:
                        remaining = size
                        while remaining:
                            block = bytes(range(256)) * min(256, (remaining+255)//256)
                            block = block[:remaining]
                            output.write(block)
                            digest.update(block)
                            remaining -= len(block)
                    return digest.hexdigest()
                expected = await asyncio.to_thread(create)
                before = time.monotonic()
                sending = asyncio.create_task(sender.send_file(receiver.username, path))
                chat_started = time.monotonic()
                await clients[-1].send_chat('user00', 'chat-during-' + name)
                await wait_until(lambda: 'chat-during-' + name in messages[0])
                chat_ms = (time.monotonic() - chat_started) * 1000
                await asyncio.wait_for(sending, 600)
                destinations = list((root / 'received1').glob('*_' + name))
                if len(destinations) != 1:
                    raise AssertionError('Expected exactly one received file')
                destination = destinations[0]
                def digest_file():
                    with destination.open('rb') as source:
                        return hashlib.file_digest(source, 'sha256').hexdigest()
                if await asyncio.to_thread(digest_file) != expected or destination.stat().st_size != size:
                    raise AssertionError('Received file hash/size mismatch')
                sender.poll_file_updates()
                receiver.poll_file_updates()
                await wait_until(lambda: not app.router.transfers)
                if list((root / 'received1').glob('*.part')):
                    raise AssertionError('Partial file leaked')
                seconds = time.monotonic() - before
                if verbose:
                    print(json.dumps(dict(stage='file', bytes=size, seconds=round(seconds, 3),
                        kib_per_second=round(size/1024/seconds, 1), chat_latency_ms=round(chat_ms, 2))), flush=True)

            for size in args.sizes:
                await transfer(size, f'file-{size}.bin')
            for i in range(args.streams):
                # File rỗng vẫn dùng META/ACCEPT/END/COMPLETE qua cả hai chặng UDP.
                await transfer(0, f'sequential-{i}.bin', verbose=False)
                if (i+1) % 128 == 0 or i+1 == args.streams:
                    print(json.dumps(dict(stage='sequential-files', completed=i+1)), flush=True)
            if len(args.sizes) + args.streams == 4096:
                try:
                    await sender.send_file(receiver.username, root / 'sequential-0.bin')
                except RuntimeError as error:
                    if 'quota' not in str(error):
                        raise
                else:
                    raise AssertionError('Sender file history quota was not enforced')
                from common.models import Packet
                from uuid import uuid4
                try:
                    await sender.transport.send(sender.server_peer, Packet(kind=M.MSG,
                        sender=sender.username, recipient=receiver.username,
                        stream_id=uuid4().hex, payload=b'quota'))
                except ValueError as error:
                    if 'quota' not in str(error):
                        raise
                else:
                    raise AssertionError('Transport stream history quota was not enforced')
                await asyncio.gather(*(c.logout() for c in clients))
                for i in range(len(clients)):
                    transport = ReliableUdpTransport(('127.0.0.1', 0), retry_seconds=.04, probe_seconds=.04)
                    faults.install(transport)
                    clients[i] = ClientService(transport, app.transport.local_address, f'user{i:02}',
                        tcp_peer=app.address, file_receiver=FileReceiver(root / f'received{i}'))
                await asyncio.gather(*(c.login('test-password') for c in clients))
                pumps.extend(asyncio.create_task(pump(i, c)) for i, c in enumerate(clients))
                sender, receiver = clients[:2]
                await transfer(0, 'after-quota-reconnect.bin')
                print(json.dumps(dict(stage='quota-4096-and-reconnect', result='PASS')), flush=True)
            print(json.dumps(dict(stage='resources', active_receive=len(app.transport._rx),
                tombstones=len(app.transport._tombstones), pending=len(app.transport._pending),
                relay_transfers=len(app.router.transfers), relay_failures=app.router.failures,
                congestion_loss_events=sum(cc.loss_events for cc in app.transport._congestion.values()),
                packets=faults.sent, injected_drops=faults.dropped, injected_duplicates=faults.duplicated,
                seed=args.seed, loss=args.loss, delay=args.delay)), flush=True)
        finally:
            await asyncio.gather(*(c.logout() for c in clients), return_exceptions=True)
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            faults.close()
            await app.close()
        if app.transport._pending or app.router._workers or app.transport._peers or app.sessions._sessions:
            raise AssertionError('Shutdown leaked resources')
    print(json.dumps(dict(stage='PASS', seconds=round(time.monotonic()-start, 3))), flush=True)


def main():
    parser = argparse.ArgumentParser(description='Test real clients on loopback with optional packet loss and delay.')
    parser.add_argument('--clients', type=int, default=20)
    parser.add_argument('--streams', type=int, default=0)
    parser.add_argument('--sizes', type=lambda s: [int(v) for v in s.split(',')], default=[1024, 1048576])
    parser.add_argument('--seed', type=int, default=7)
    parser.add_argument('--loss', type=float, default=0)
    parser.add_argument('--delay', type=float, default=0)
    args = parser.parse_args()
    if not 2 <= args.clients <= 20 or not 0 <= args.loss <= .2 or not 0 <= args.delay <= .1:
        parser.error('clients 2..20, loss 0..0.2, delay 0..0.1')
    if not 0 <= args.streams <= 4096-len(args.sizes) or any(not 0 <= size <= 16*1024*1024 for size in args.sizes):
        parser.error('total files <=4096, size 0..16 MiB')
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
