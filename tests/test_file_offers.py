import asyncio
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from client.file_offers import FileOfferReceiver
from client.files import FileReceiver
from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


def metadata():
    return {
        "name": "hello.txt", "size": 3, "sha256": hashlib.sha256(b"abc").hexdigest(),
        "chunk_size": 480, "total_chunks": 1,
    }


def offer(*, transfer_id=None, meta=None, **fields):
    return Packet(
        kind=MessageType.FILE_META, sender="hai", recipient="khoa",
        stream_id=transfer_id or uuid4().hex,
        payload=json.dumps(metadata() if meta is None else meta).encode("utf-8"),
        **fields,
    )


class FileOfferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receiver = FileReceiver(self.root)
        self.handler = FileOfferReceiver("khoa", self.receiver)
        self.addCleanup(self.handler.close)

    def test_accept_only_after_storage_prepared(self):
        packet = offer()
        reply = self.handler.prepare(packet)
        self.assertEqual(reply.kind, MessageType.FILE_ACCEPT)
        self.assertEqual((reply.sender, reply.recipient, reply.stream_id, reply.payload),
                         ("khoa", "hai", packet.stream_id, b""))
        self.assertTrue((self.root / f"{packet.stream_id}.part").is_file())
        self.assertEqual(self.receiver.reserved_bytes, 3)

    def test_bad_metadata_rejected_without_file(self):
        cases = [dict(metadata(), name="../escape"), dict(metadata(), sha256="bad"),
                 dict(metadata(), total_chunks=2), dict(metadata(), size=True)]
        for meta in cases:
            packet = offer(meta=meta)
            reply = self.handler.prepare(packet)
            body = json.loads(reply.payload)
            self.assertEqual(reply.kind, MessageType.ERROR)
            self.assertEqual(reply.stream_id, "control")
            self.assertEqual(body["related_message_id"], packet.message_id)
            self.assertLessEqual(len(reply.payload), 480)
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertEqual(self.receiver.reserved_bytes, 0)

    def test_invalid_json_and_stream_id(self):
        from dataclasses import replace
        for payload in (b"\xff", b"{", b" " * 481, b"[]"):
            reply = self.handler.prepare(replace(offer(), payload=payload))
            self.assertEqual(reply.kind, MessageType.ERROR)
        self.assertEqual(self.handler.prepare(offer(transfer_id="../bad")).kind,
                         MessageType.ERROR)
        self.assertEqual(self.receiver.reserved_bytes, 0)

    def test_duplicate_or_quota_error_preserves_existing_transfer(self):
        self.receiver.max_transfers = 1
        first = offer()
        self.handler.prepare(first)
        for packet in (first, offer()):
            self.assertEqual(self.handler.prepare(packet).kind, MessageType.ERROR)
        self.assertEqual(list(self.receiver.transfers), [first.stream_id])
        self.assertTrue((self.root / f"{first.stream_id}.part").exists())

    def test_storage_error_does_not_expose_local_path(self):
        with patch.object(self.receiver, "begin", side_effect=PermissionError("/secret/local/path")):
            reply = self.handler.prepare(offer())
        body = json.loads(reply.payload)
        self.assertEqual(body["code"], "FILE_STORAGE_ERROR")
        self.assertNotIn("/secret", reply.payload.decode())

    def test_disabled_and_session_limit(self):
        disabled = FileOfferReceiver("khoa", None).prepare(offer())
        self.assertEqual(json.loads(disabled.payload)["code"], "FILE_RECEIVE_DISABLED")
        with patch("client.file_offers.MAX_FILE_OFFERS_PER_SESSION", 1):
            self.handler.prepare(offer())
            reply = self.handler.prepare(offer())
        self.assertEqual(json.loads(reply.payload)["code"], "FILE_SESSION_LIMIT")

    def test_unreplyable_headers_are_ignored(self):
        from dataclasses import replace
        for fields in ({"sender": "../bad"}, {"message_id": "bad"}, {"recipient": "*"}):
            self.assertIsNone(self.handler.prepare(replace(offer(), **fields)))
        self.assertEqual(self.receiver.reserved_bytes, 0)


class ControlledTransport(MemoryTransport):
    def __init__(self, hub, address):
        super().__init__(hub, address)
        self.pause_reply = False
        self.reply_started = asyncio.Event()
        self.release_reply = asyncio.Event()

    async def send(self, peer, packet):
        if packet.kind == MessageType.FILE_ACCEPT and self.pause_reply:
            self.reply_started.set()
            await self.release_reply.wait()
        await super().send(peer, packet)


class ClientFileOfferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.now = 0.0
        self.receiver = FileReceiver(self.root, clock=lambda: self.now)
        hub = MemoryHub()
        self.server = MemoryTransport(hub, ("server", 9001))
        self.local = ControlledTransport(hub, ("khoa", 10001))
        self.other = MemoryTransport(hub, ("other", 10002))
        for transport in (self.server, self.local, self.other):
            await transport.start()
        self.client = ClientService(self.local, self.server.address, "khoa",
                                    file_receiver=self.receiver)
        self.task = asyncio.create_task(self.client.receive_events())

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        for transport in (self.server, self.local, self.other):
            await transport.close()
        self.temp.cleanup()

    async def receive_reply(self):
        received = await asyncio.wait_for(self.server.receive(), timeout=2)
        self.assertEqual(received.peer, self.local.address)
        return received.packet

    async def chat_marker(self):
        await self.server.send(self.local.address, Packet(
            kind=MessageType.MSG, sender="hai", recipient="khoa",
            stream_id="chat-khoa", payload=b"still responsive",
        ))
        event = await asyncio.wait_for(self.client.events.get(), timeout=1)
        self.assertEqual(event["text"], "still responsive")

    async def test_accept_and_shutdown_cleanup(self):
        packet = offer()
        await self.server.send(self.local.address, packet)
        reply = await self.receive_reply()
        self.assertEqual(reply.kind, MessageType.FILE_ACCEPT)
        self.assertEqual(reply.stream_id, packet.stream_id)
        self.assertEqual(self.receiver.reserved_bytes, 3)
        self.task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.task
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.glob("*.part")), [])

    async def test_rejection_then_next_offer_and_chat_work(self):
        await self.server.send(self.local.address, offer(meta=dict(metadata(), name="../bad")))
        self.assertEqual((await self.receive_reply()).kind, MessageType.ERROR)
        await self.chat_marker()
        await self.server.send(self.local.address, offer())
        self.assertEqual((await self.receive_reply()).kind, MessageType.FILE_ACCEPT)

    async def test_chat_receives_while_accept_send_is_waiting(self):
        self.local.pause_reply = True
        await self.server.send(self.local.address, offer())
        await asyncio.wait_for(self.local.reply_started.wait(), timeout=1)
        await self.chat_marker()
        self.local.release_reply.set()
        self.assertEqual((await self.receive_reply()).kind, MessageType.FILE_ACCEPT)

    async def test_waiting_offers_are_bounded(self):
        self.local.pause_reply = True
        await self.server.send(self.local.address, offer())
        await asyncio.wait_for(self.local.reply_started.wait(), timeout=1)
        for _ in range(20):
            await self.server.send(self.local.address, offer())

        async def wait_until_full():
            while not self.client._file_queue.full():
                await asyncio.sleep(0)
        await asyncio.wait_for(wait_until_full(), timeout=1)
        self.assertEqual(self.client._file_queue.qsize(), 8)
        self.assertEqual(len(self.receiver.transfers), 1)

    async def test_reply_timeout_fails_task_and_cleans_files(self):
        self.local.pause_reply = True
        with patch("client.service.FILE_REPLY_TIMEOUT_SECONDS", 0.02):
            await self.server.send(self.local.address, offer())
            with self.assertRaises(ExceptionGroup) as raised:
                await asyncio.wait_for(self.task, timeout=1)
        self.assertTrue(any(isinstance(error, TimeoutError) for error in raised.exception.exceptions))
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.glob("*.part")), [])

    async def test_cleanup_error_is_reported_and_pending_queue_is_discarded(self):
        self.local.pause_reply = True
        await self.server.send(self.local.address, offer())
        await asyncio.wait_for(self.local.reply_started.wait(), timeout=1)
        self.client._file_queue.put_nowait(offer())
        with patch.object(self.receiver, "close", side_effect=PermissionError("busy")):
            self.task.cancel()
            with self.assertRaises(ExceptionGroup):
                await self.task
        self.assertTrue(self.client._file_queue.empty())
        self.assertEqual(self.receiver.reserved_bytes, 3)
        await asyncio.to_thread(self.receiver.close)
        self.assertEqual(self.receiver.reserved_bytes, 0)

    async def test_expire_and_late_meta_cannot_recreate_transfer(self):
        packet = offer()
        await self.server.send(self.local.address, packet)
        await self.receive_reply()
        self.now = 46.0

        async def wait_for_expire():
            while self.receiver.reserved_bytes:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait_for_expire(), timeout=2)
        expired = await self.receive_reply()
        body = json.loads(expired.payload)
        self.assertEqual(body["code"], "FILE_EXPIRED")
        self.assertEqual(body["related_message_id"], packet.message_id)
        await self.server.send(self.local.address, packet)
        reply = await self.receive_reply()
        self.assertEqual(json.loads(reply.payload)["code"], "FILE_ID_REUSED")
        self.assertEqual(list(self.root.glob("*.part")), [])

    async def test_wrong_peer_and_broadcast_do_not_allocate(self):
        from dataclasses import replace
        await self.other.send(self.local.address, offer())
        await self.server.send(self.local.address, replace(offer(), recipient="*"))
        await self.chat_marker()
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertTrue(self.server._queue.empty())

    async def test_timeout_notification_send_failure_cleans_other_transfers(self):
        await self.server.send(self.local.address, offer())
        await self.receive_reply()
        original_send = self.local.send

        async def blocked_error(peer, packet):
            if packet.kind == MessageType.ERROR:
                await asyncio.Event().wait()
            await original_send(peer, packet)

        self.now = 46
        with patch.object(self.local, "send", blocked_error), patch("client.service.FILE_REPLY_TIMEOUT_SECONDS", 0.02):
            await self.server.send(self.local.address, offer())
            with self.assertRaises(ExceptionGroup):
                await asyncio.wait_for(self.task, timeout=2)
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.glob("*.part")), [])

    async def test_cancellation_waits_for_disk_before_cleanup(self):
        entered = threading.Event()
        release = threading.Event()
        order = []
        original_begin = self.receiver.begin
        original_close = self.receiver.close

        def slow_begin(*args):
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError("Test did not release disk operation")
            original_begin(*args)
            order.append("begin")

        def tracked_close():
            order.append("close")
            original_close()

        with patch.object(self.receiver, "begin", slow_begin), patch.object(self.receiver, "close", tracked_close):
            try:
                await self.server.send(self.local.address, offer())
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                await self.chat_marker()
                self.task.cancel()
                # Cho cancellation đi tới worker trong khi thao tác đĩa còn bị giữ.
                await asyncio.sleep(0.02)
                self.assertFalse(self.task.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await self.task
        self.assertEqual(order, ["begin", "close"])
        self.assertEqual(list(self.root.glob("*.part")), [])


if __name__ == "__main__":
    unittest.main()
