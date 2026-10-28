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


CONTENT = b"A" * 480 + b"B" * 480 + b"C" * 40


def meta_packet(transfer_id, content=CONTENT, sender="hai", **metadata_changes):
    metadata = {
        "name": "received.bin", "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "chunk_size": 480, "total_chunks": (len(content) + 479) // 480,
    }
    metadata.update(metadata_changes)
    return Packet(
        kind=MessageType.FILE_META, sender=sender, recipient="khoa", stream_id=transfer_id,
        payload=json.dumps(metadata).encode(),
    )


def chunk_packet(transfer_id, index, content=CONTENT, sender="hai"):
    return Packet(
        kind=MessageType.FILE_CHUNK, sender=sender, recipient="khoa", stream_id=transfer_id,
        chunk_index=index, payload=content[index * 480:(index + 1) * 480],
    )


def end_packet(transfer_id, sender="hai", payload=b""):
    return Packet(kind=MessageType.FILE_END, sender=sender, recipient="khoa",
                  stream_id=transfer_id, payload=payload)


class FileReceiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 0.0
        self.receiver = FileReceiver(self.root, clock=lambda: self.now)
        self.handler = FileOfferReceiver("khoa", self.receiver, clock=lambda: self.now)
        self.addCleanup(self.handler.close)
        self.id = uuid4().hex

    def start(self, **kwargs):
        self.assertEqual(self.handler.handle(meta_packet(self.id, **kwargs)).kind,
                         MessageType.FILE_ACCEPT)

    def chunks(self):
        for index in [2, 0, 0, 1]:
            self.assertIsNone(self.handler.handle(chunk_packet(self.id, index)))

    def assert_clean(self):
        self.assertFalse(self.receiver.transfers)
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.glob("*.part")), [])

    def test_reorder_duplicate_complete_and_late_packets(self):
        self.start()
        self.chunks()
        reply = self.handler.handle(end_packet(self.id))
        self.assertEqual(reply.kind, MessageType.FILE_COMPLETE)
        self.assertEqual((reply.recipient, reply.stream_id), ("hai", self.id))
        self.assertEqual(json.loads(reply.payload), {
            "size": 1000, "sha256": hashlib.sha256(CONTENT).hexdigest(),
        })
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)
        self.assert_clean()
        self.assertEqual(self.handler.handle(end_packet(self.id)), reply)
        self.assertIsNone(self.handler.handle(chunk_packet(self.id, 0)))
        self.assertEqual(self.handler.handle(meta_packet(self.id)).kind, MessageType.ERROR)
        self.assertEqual(len(list(self.root.iterdir())), 1)

    def test_empty_file_completes_without_chunks(self):
        self.start(content=b"")
        reply = self.handler.handle(end_packet(self.id))
        self.assertEqual(reply.kind, MessageType.FILE_COMPLETE)
        self.assertEqual(json.loads(reply.payload)["size"], 0)
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), b"")
        self.assert_clean()

    def test_missing_chunk_closes_transfer_and_caches_error(self):
        self.start()
        self.handler.handle(chunk_packet(self.id, 0))
        end = end_packet(self.id)
        reply = self.handler.handle(end)
        self.assertEqual(reply.kind, MessageType.ERROR)
        self.assertEqual(json.loads(reply.payload)["related_message_id"], end.message_id)
        self.assert_clean()
        self.assertEqual(self.handler.handle(chunk_packet(self.id, 1)), reply)
        self.assertEqual(self.handler.handle(end_packet(self.id)), reply)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_wrong_hash_never_reports_complete(self):
        self.start(sha256="0" * 64)
        self.chunks()
        self.assertEqual(self.handler.handle(end_packet(self.id)).kind, MessageType.ERROR)
        self.assert_clean()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_wrong_sender_cannot_write_finish_or_read_cached_result(self):
        self.start()
        for packet in (chunk_packet(self.id, 0, sender="other"), end_packet(self.id, sender="other")):
            reply = self.handler.handle(packet)
            self.assertEqual(json.loads(reply.payload)["code"], "FILE_SENDER_MISMATCH")
        self.assertEqual(self.receiver.transfers[self.id]["received"], set())
        self.chunks()
        self.assertEqual(self.handler.handle(end_packet(self.id)).kind, MessageType.FILE_COMPLETE)
        self.assertEqual(self.handler.handle(end_packet(self.id, sender="other")).kind,
                         MessageType.ERROR)

    def test_bad_chunk_and_bad_end_cleanup(self):
        for packet_kind in ("chunk", "end"):
            self.id = uuid4().hex
            self.start()
            packet = (chunk_packet(self.id, 9) if packet_kind == "chunk"
                      else end_packet(self.id, payload=b"unexpected"))
            self.assertEqual(self.handler.handle(packet).kind, MessageType.ERROR)
            self.assert_clean()

    def test_unknown_and_expired_transfer_do_not_allocate(self):
        self.assertEqual(self.handler.handle(chunk_packet(self.id, 0)).kind, MessageType.ERROR)
        self.start()
        self.now = 45
        self.assertEqual(self.handler.expire(), [self.id])
        self.assertEqual(self.handler.handle(end_packet(self.id)).kind, MessageType.ERROR)
        self.assertEqual(self.handler.handle(meta_packet(self.id)).kind, MessageType.ERROR)
        self.assert_clean()

    def test_result_cache_expires_without_reopening_id(self):
        self.start()
        self.chunks()
        result = self.handler.handle(end_packet(self.id))
        self.now = 119
        self.assertEqual(self.handler.handle(end_packet(self.id)), result)
        self.now = 120
        self.handler.expire()
        self.assertFalse(self.handler.results)
        self.assertIn(self.id, self.handler.seen_ids)
        self.assertEqual(self.handler.handle(end_packet(self.id)).kind, MessageType.ERROR)
        self.assertEqual(len(list(self.root.iterdir())), 1)

    def test_publish_cleanup_failure_preserves_file_but_does_not_complete(self):
        self.start()
        self.chunks()
        with patch.object(Path, "unlink", side_effect=PermissionError("busy")):
            reply = self.handler.handle(end_packet(self.id))
        self.assertEqual(reply.kind, MessageType.ERROR)
        self.assertEqual(self.receiver.reserved_bytes, 1000)
        target = self.root / f"{self.id}_received.bin"
        self.assertEqual(target.read_bytes(), CONTENT)
        self.handler.expire()
        self.assert_clean()
        self.assertEqual(target.read_bytes(), CONTENT)
        self.assertEqual(self.handler.handle(end_packet(self.id)), reply)

    def test_two_transfers_do_not_mix_chunks(self):
        first, second = self.id, uuid4().hex
        self.start()
        self.handler.handle(meta_packet(second, content=b"xyz", sender="friend"))
        self.handler.handle(chunk_packet(second, 0, content=b"xyz", sender="friend"))
        self.chunks()
        self.assertEqual(self.handler.handle(end_packet(first)).stream_id, first)
        self.assertEqual(self.handler.handle(end_packet(second, sender="friend")).stream_id, second)
        self.assertEqual((self.root / f"{first}_received.bin").read_bytes(), CONTENT)
        self.assertEqual((self.root / f"{second}_received.bin").read_bytes(), b"xyz")
        self.assert_clean()


class CompleteBlockingTransport(MemoryTransport):
    def __init__(self, hub, address):
        super().__init__(hub, address)
        self.block_complete = False

    async def send(self, peer, packet):
        if self.block_complete and packet.kind == MessageType.FILE_COMPLETE:
            await asyncio.Event().wait()
        await super().send(peer, packet)


class ClientFileReceiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.receiver = FileReceiver(self.root)
        hub = MemoryHub()
        self.server = MemoryTransport(hub, ("server", 9001))
        self.local = CompleteBlockingTransport(hub, ("khoa", 10001))
        await self.server.start()
        await self.local.start()
        self.client = ClientService(self.local, self.server.address, "khoa", file_receiver=self.receiver)
        self.task = asyncio.create_task(self.client.receive_events())
        self.id = uuid4().hex
        await self.send(meta_packet(self.id))
        self.assertEqual((await self.reply()).kind, MessageType.FILE_ACCEPT)

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.local.close()
        await self.server.close()
        self.temp.cleanup()

    async def send(self, packet):
        await self.server.send(self.local.address, packet)

    async def reply(self):
        return (await asyncio.wait_for(self.server.receive(), timeout=2)).packet

    async def chunks(self):
        for index in [2, 0, 0, 1]:
            await self.send(chunk_packet(self.id, index))

    async def test_complete_through_dispatcher_and_duplicate_end(self):
        await self.chunks()
        await self.send(end_packet(self.id))
        result = await self.reply()
        self.assertEqual(result.kind, MessageType.FILE_COMPLETE)
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)
        await self.send(end_packet(self.id))
        self.assertEqual(await self.reply(), result)
        self.assertEqual(len(list(self.root.iterdir())), 1)

    async def test_chat_continues_while_finish_is_busy(self):
        entered, release = threading.Event(), threading.Event()
        original_finish = self.receiver.finish

        def slow_finish(transfer_id):
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError("Test did not release finish")
            return original_finish(transfer_id)

        with patch.object(self.receiver, "finish", slow_finish):
            try:
                await self.chunks()
                await self.send(end_packet(self.id))
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                update = self.client.poll_file_updates()[0]
                self.assertEqual((update["status"], update["received_bytes"]), ("verifying", 1000))
                await self.send(Packet(kind=MessageType.MSG, sender="hai", recipient="khoa",
                                       stream_id="chat-khoa", payload=b"still chatting"))
                event = await asyncio.wait_for(self.client.events.get(), timeout=1)
                self.assertEqual(event["text"], "still chatting")
            finally:
                release.set()
            self.assertEqual((await self.reply()).kind, MessageType.FILE_COMPLETE)

    async def test_complete_send_failure_keeps_verified_file(self):
        self.local.block_complete = True
        with patch("client.service.FILE_REPLY_TIMEOUT_SECONDS", 0.02):
            await self.chunks()
            await self.send(end_packet(self.id))
            with self.assertRaises(ExceptionGroup):
                await asyncio.wait_for(self.task, timeout=1)
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.glob("*.part")), [])
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["confirmation_delivery"]), ("completed", "failed"))

    async def test_shutdown_during_finish_reports_completed_but_unsent_confirmation(self):
        entered, release = threading.Event(), threading.Event()
        original_finish = self.receiver.finish

        def slow_finish(transfer_id):
            result = original_finish(transfer_id)
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError("Test did not release finish")
            return result

        with patch.object(self.receiver, "finish", slow_finish):
            try:
                await self.chunks()
                await self.send(end_packet(self.id))
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                self.task.cancel()
                await asyncio.sleep(0.01)
            finally:
                release.set()
            await asyncio.gather(self.task, return_exceptions=True)
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["confirmation_delivery"]), ("completed", "failed"))
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)

    async def test_local_receive_cancel_cleans_and_notifies_original_sender(self):
        self.assertTrue(await self.client.cancel_receive_file(self.id))
        reply = await self.reply()
        self.assertEqual((reply.kind, reply.sender, reply.recipient, reply.stream_id),
                         (MessageType.FILE_CANCEL, "khoa", "hai", "control"))
        self.assertEqual(json.loads(reply.payload)["transfer_id"], self.id)
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["cancel_delivery"]), ("cancelled", "sent_to_server"))
        self.assertFalse(update["cleanup_pending"])
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertFalse(await self.client.cancel_receive_file(self.id))
        await self.send(chunk_packet(self.id, 0))
        self.assertEqual(json.loads((await self.reply()).payload)["code"], "FILE_CANCELLED")

    async def test_local_cancel_does_not_delete_completed_or_unknown_file(self):
        self.assertFalse(await self.client.cancel_receive_file(uuid4().hex))
        with self.assertRaises(ValueError):
            await self.client.cancel_receive_file("bad")
        await self.chunks()
        await self.send(end_packet(self.id))
        self.assertEqual((await self.reply()).kind, MessageType.FILE_COMPLETE)
        self.assertFalse(await self.client.cancel_receive_file(self.id))
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)

    async def test_local_cancel_send_timeout_reports_failure_without_stopping_worker(self):
        original = self.local.send

        async def send(peer, packet):
            if packet.kind == MessageType.FILE_CANCEL:
                await asyncio.Event().wait()
            await original(peer, packet)

        with patch.object(self.local, "send", send), patch("client.service.LOCAL_CANCEL_TIMEOUT_SECONDS", 0.02):
            self.assertTrue(await self.client.cancel_receive_file(self.id))
        self.assertEqual(self.client.poll_file_updates()[0]["cancel_delivery"], "failed")
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertFalse(self.task.done())

    async def test_local_cancel_cleanup_failure_still_stops_sender(self):
        with patch.object(Path, "unlink", side_effect=PermissionError("busy")):
            self.assertTrue(await self.client.cancel_receive_file(self.id))
            update = self.client.poll_file_updates()[0]
            self.assertEqual(update["status"], "failed")
            self.assertTrue(update["cleanup_pending"])
        self.assertEqual((await self.reply()).kind, MessageType.FILE_CANCEL)

    async def test_cancel_queue_is_bounded_and_shutdown_resolves_waiter(self):
        entered, release = threading.Event(), threading.Event()
        original = self.receiver.accept_chunk

        def blocked(*args):
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError("Test did not release chunk")
            return original(*args)

        with patch.object(self.receiver, "accept_chunk", blocked):
            request = None
            try:
                await self.send(chunk_packet(self.id, 0))
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                request = asyncio.create_task(self.client.cancel_receive_file(self.id))
                await asyncio.sleep(0)
                while not self.client._file_queue.full():
                    self.client._file_queue.put_nowait(chunk_packet(self.id, 1))
                with self.assertRaisesRegex(RuntimeError, "bận"):
                    await self.client.cancel_receive_file(self.id)
                self.task.cancel()
                await asyncio.sleep(0.01)
            finally:
                release.set()
            await asyncio.gather(self.task, return_exceptions=True)
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(request, timeout=1)
        self.assertEqual(self.receiver.reserved_bytes, 0)
        with self.assertRaises(RuntimeError):
            await self.client.cancel_receive_file(self.id)

    async def test_shutdown_during_cancel_io_finishes_snapshot(self):
        entered, release = threading.Event(), threading.Event()
        original = self.receiver.cancel

        def blocked(transfer_id):
            result = original(transfer_id)
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError("Test did not release cancel")
            return result

        with patch.object(self.receiver, "cancel", blocked):
            request = asyncio.create_task(self.client.cancel_receive_file(self.id))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                self.task.cancel()
                await asyncio.sleep(0.01)
            finally:
                release.set()
            await asyncio.gather(self.task, return_exceptions=True)
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(request, timeout=1)
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["cancel_delivery"]), ("cancelled", "failed"))
        self.assertFalse(update["cleanup_pending"])


if __name__ == "__main__":
    unittest.main()
