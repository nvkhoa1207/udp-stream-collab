import asyncio
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from client.files import FileReceiver
from client.file_cancel import FileTransferCancelled
from client.service import ClientService
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


class ControlledTransport(MemoryTransport):
    def __init__(self, hub, address):
        super().__init__(hub, address)
        self.block_chunks = False
        self.release = asyncio.Event()
        self.full_batch = asyncio.Event()
        self.active = self.peak = 0

    async def send(self, peer, packet):
        if packet.kind != MessageType.FILE_CHUNK or not self.block_chunks:
            return await super().send(peer, packet)
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.active == 8:
            self.full_batch.set()
        try:
            await self.release.wait()
            await super().send(peer, packet)
        finally:
            self.active -= 1


class ClientFileSendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "source.bin"
        self.content = bytes(range(256)) * 20
        self.path.write_bytes(self.content)
        self.hub = MemoryHub()
        self.server = MemoryTransport(self.hub, ("server", 9001))
        self.local = ControlledTransport(self.hub, ("khoa", 10001))
        await self.server.start()
        await self.local.start()
        self.client = ClientService(self.local, self.server.address, "khoa")
        self.receive_task = asyncio.create_task(self.client.receive_events())
        await asyncio.sleep(0)
        self.tasks = [self.receive_task]

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.local.close()
        await self.server.close()
        self.temp.cleanup()

    def start_send(self):
        task = asyncio.create_task(self.client.send_file("hai", self.path))
        self.tasks.append(task)
        return task

    async def outgoing(self):
        return (await asyncio.wait_for(self.server.receive(), timeout=2)).packet

    async def reply(self, original, kind, payload=b"", **changes):
        packet = Packet(kind=kind, sender="hai", recipient="khoa",
                        stream_id=original.stream_id, payload=payload)
        await self.server.send(self.local.address, replace(packet, **changes))

    async def finish_sending(self, meta):
        chunks = []
        while True:
            packet = await self.outgoing()
            if packet.kind == MessageType.FILE_END:
                return packet, chunks
            self.assertEqual(packet.kind, MessageType.FILE_CHUNK)
            chunks.append(packet)

    async def complete(self, end, metadata, **changes):
        body = {"size": metadata["size"], "sha256": metadata["sha256"]}
        body.update(changes)
        await self.reply(end, MessageType.FILE_COMPLETE, json.dumps(body).encode())

    async def barrier(self):
        # Cùng dispatcher: khi chat đã được giao thì phản hồi trước đó đã xử lý.
        packet = Packet(kind=MessageType.MSG, sender="hai", recipient="khoa",
                        stream_id="chat-khoa", payload=b"barrier")
        await self.server.send(self.local.address, packet)
        await asyncio.wait_for(self.client.events.get(), timeout=1)

    async def expect_cancel(self):
        packet = await self.outgoing()
        self.assertEqual(packet.kind, MessageType.FILE_CANCEL)
        self.assertEqual(packet.stream_id, "control")
        self.assertTrue(self.server._queue.empty())
        return packet

    async def test_round_trip_with_real_receiver_through_mock_relay(self):
        remote = MemoryTransport(self.hub, ("hai", 10002))
        await remote.start()
        receiver = FileReceiver(self.root / "received")
        other = ClientService(remote, self.server.address, "hai", file_receiver=receiver)
        other_task = asyncio.create_task(other.receive_events())
        self.tasks.append(other_task)

        async def relay():
            while True:
                packet = (await self.server.receive()).packet
                target = remote.address if packet.recipient == "hai" else self.local.address
                await self.server.send(target, packet)

        relay_task = asyncio.create_task(relay())
        self.tasks.append(relay_task)
        try:
            await asyncio.wait_for(self.client.send_file("hai", self.path), timeout=3)
            files = list(receiver.output_directory.iterdir())
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].read_bytes(), self.content)
            self.assertEqual(receiver.reserved_bytes, 0)
            self.assertIsNone(self.client._file_sender)
        finally:
            other_task.cancel()
            await asyncio.gather(other_task, return_exceptions=True)
            await remote.close()

    async def test_empty_file_needs_accept_and_complete_but_no_chunks(self):
        self.path.write_bytes(b"")
        task = self.start_send()
        meta = await self.outgoing()
        await self.reply(meta, MessageType.FILE_ACCEPT)
        end, chunks = await self.finish_sending(meta)
        self.assertEqual(chunks, [])
        await self.barrier()
        self.assertFalse(task.done())
        await self.complete(end, json.loads(meta.payload))
        await asyncio.wait_for(task, timeout=1)

    async def test_wrong_sender_peer_stream_and_early_complete_do_not_unlock(self):
        task = self.start_send()
        meta = await self.outgoing()
        await self.reply(meta, MessageType.FILE_ACCEPT, sender="other")
        await self.reply(meta, MessageType.FILE_ACCEPT, stream_id="0" * 32)
        await self.reply(meta, MessageType.FILE_ACCEPT, recipient="*")
        await self.complete(meta, json.loads(meta.payload))  # COMPLETE đến quá sớm.
        rogue = MemoryTransport(self.hub, ("rogue", 10003))
        await rogue.start()
        try:
            await rogue.send(self.local.address, Packet(
                kind=MessageType.FILE_ACCEPT, sender="hai", recipient="khoa", stream_id=meta.stream_id))
        finally:
            await rogue.close()
        await self.barrier()
        self.assertFalse(task.done())
        self.assertTrue(self.server._queue.empty())
        await self.reply(meta, MessageType.FILE_ACCEPT)
        end, chunks = await self.finish_sending(meta)
        self.assertEqual(b"".join(p.payload for p in sorted(chunks, key=lambda p: p.chunk_index)), self.content)
        await self.barrier()
        self.assertFalse(task.done())  # COMPLETE trước END không được giữ để báo thành công.
        await self.complete(end, json.loads(meta.payload))
        await asyncio.wait_for(task, timeout=1)

    async def test_accept_timeout_sends_no_chunks_and_releases_sender(self):
        with patch("client.file_sender.FILE_RESPONSE_TIMEOUT_SECONDS", 0.02):
            task = self.start_send()
            self.assertEqual((await self.outgoing()).kind, MessageType.FILE_META)
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(task, timeout=1)
        await self.expect_cancel()
        self.assertTrue(self.server._queue.empty())
        self.assertIsNone(self.client._file_sender)

    async def test_missing_complete_times_out_after_end(self):
        with patch("client.file_sender.FILE_RESPONSE_TIMEOUT_SECONDS", 0.02):
            task = self.start_send()
            meta = await self.outgoing()
            await self.reply(meta, MessageType.FILE_ACCEPT)
            await self.finish_sending(meta)
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(task, timeout=1)

    async def test_invalid_complete_never_succeeds(self):
        for changes in ({"sha256": "0" * 64}, {"size": True}, {"size": -1}, {"extra": 1}):
            task = self.start_send()
            meta = await self.outgoing()
            await self.reply(meta, MessageType.FILE_ACCEPT)
            end, _ = await self.finish_sending(meta)
            await self.complete(end, json.loads(meta.payload), **changes)
            with self.assertRaises(ValueError):
                await asyncio.wait_for(task, timeout=1)
            await self.expect_cancel()
            self.assertIsNone(self.client._file_sender)

    async def test_invalid_accept_fails_before_chunks(self):
        task = self.start_send()
        meta = await self.outgoing()
        await self.reply(meta, MessageType.FILE_ACCEPT, b"unexpected")
        with self.assertRaises(ValueError):
            await asyncio.wait_for(task, timeout=1)
        await self.expect_cancel()
        self.assertTrue(self.server._queue.empty())

    async def test_error_correlates_meta_and_old_chunk_message_ids(self):
        for stage in ("meta", "chunk"):
            task = self.start_send()
            meta = await self.outgoing()
            original = meta
            if stage == "chunk":
                await self.reply(meta, MessageType.FILE_ACCEPT)
                _, chunks = await self.finish_sending(meta)
                original = chunks[0]
            body = dict(code="FILE_REJECTED", message="test rejection", related_message_id=original.message_id)
            await self.reply(original, MessageType.ERROR, json.dumps(body).encode(), stream_id="control")
            with self.assertRaisesRegex(RuntimeError, "FILE_REJECTED"):
                await asyncio.wait_for(task, timeout=1)
            await self.expect_cancel()

    async def test_unrelated_or_malformed_errors_are_ignored(self):
        task = self.start_send()
        meta = await self.outgoing()
        for payload in (b"[]", b"{", json.dumps(dict(code="FAIL", message="test", related_message_id="unknown")).encode()):
            await self.reply(meta, MessageType.ERROR, payload, stream_id="control")
        await self.barrier()
        self.assertFalse(task.done())
        await self.reply(meta, MessageType.FILE_ACCEPT)
        end, _ = await self.finish_sending(meta)
        await self.complete(end, json.loads(meta.payload))
        await asyncio.wait_for(task, timeout=1)

    async def test_bounded_concurrency_and_cancel_drains_chunk_tasks(self):
        self.local.block_chunks = True
        task = self.start_send()
        meta = await self.outgoing()
        await self.reply(meta, MessageType.FILE_ACCEPT)
        await asyncio.wait_for(self.local.full_batch.wait(), timeout=1)
        self.assertEqual(self.local.active, 8)
        await self.barrier()  # Chat vẫn được nhận trong lúc send đang chờ.
        self.assertEqual(self.local.peak, 8)
        with self.assertRaises(RuntimeError):
            await self.client.send_file("hai", self.path)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.local.active, 0)
        self.assertIsNone(self.client._file_sender)
        await self.expect_cancel()
        self.assertTrue(self.server._queue.empty())

    async def test_transport_send_timeout_cancels_all_chunk_sends(self):
        self.local.block_chunks = True
        with patch("client.file_sender.FILE_SEND_TIMEOUT_SECONDS", 0.03):
            task = self.start_send()
            meta = await self.outgoing()
            await self.reply(meta, MessageType.FILE_ACCEPT)
            with self.assertRaises(ExceptionGroup) as caught:
                await asyncio.wait_for(task, timeout=1)
            self.assertTrue(all(isinstance(exc, TimeoutError) for exc in caught.exception.exceptions))
        self.assertEqual(self.local.active, 0)

    async def test_stopping_receive_aborts_pending_file(self):
        task = self.start_send()
        await self.outgoing()
        self.receive_task.cancel()
        await asyncio.gather(self.receive_task, return_exceptions=True)
        with self.assertRaises(ConnectionError):
            await asyncio.wait_for(task, timeout=1)
        with self.assertRaises(RuntimeError):
            await self.client.send_file("hai", self.path)

    async def test_changed_source_never_sends_end(self):
        task = self.start_send()
        meta = await self.outgoing()
        self.path.write_bytes(b"changed")
        await self.reply(meta, MessageType.FILE_ACCEPT)
        with self.assertRaisesRegex(ValueError, "changed"):
            await asyncio.wait_for(task, timeout=1)
        while not self.server._queue.empty():
            self.assertNotEqual((await self.outgoing()).kind, MessageType.FILE_END)

    async def test_input_limits_before_network(self):
        for recipient in ("*", "", "bad name"):
            with self.assertRaises(ValueError):
                await self.client.send_file(recipient, self.path)
        with patch("client.file_sender.MAX_SEND_FILE_BYTES", 100):
            with self.assertRaises(ValueError):
                await self.client.send_file("hai", self.path)
        self.client._sent_file_count = 4096
        with self.assertRaises(RuntimeError):
            await self.client.send_file("hai", self.path)
        self.assertTrue(self.server._queue.empty())

    async def test_progress_counts_only_finished_sends_and_waits_for_complete(self):
        self.local.block_chunks = True
        task = self.start_send()
        meta = await self.outgoing()
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["acknowledged_bytes"]), ("waiting_accept", 0))
        await self.reply(meta, MessageType.FILE_ACCEPT)
        await asyncio.wait_for(self.local.full_batch.wait(), timeout=1)
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["acknowledged_bytes"]), ("sending", 0))
        self.local.release.set()
        end, _ = await self.finish_sending(meta)
        update = self.client.poll_file_updates()[0]
        self.assertEqual(update["acknowledged_bytes"], len(self.content))
        self.assertEqual(update["total_bytes"], len(self.content))
        self.assertEqual(update["status"], "verifying")
        self.assertFalse(task.done())
        await self.complete(end, json.loads(meta.payload))
        await asyncio.wait_for(task, timeout=1)
        update = self.client.poll_file_updates()[0]
        self.assertEqual(update["status"], "completed")
        self.assertEqual(update["acknowledged_bytes"], len(self.content))
        self.assertEqual(self.client.poll_file_updates(), [])
        self.assertFalse(self.client.file_updates.records)

    async def test_cancel_api_sends_control_and_waits_for_local_cleanup(self):
        self.local.block_chunks = True
        task = self.start_send()
        meta = await self.outgoing()
        await self.reply(meta, MessageType.FILE_ACCEPT)
        await asyncio.wait_for(self.local.full_batch.wait(), timeout=1)
        self.assertFalse(await self.client.cancel_file("wrong-id"))
        self.assertTrue(await self.client.cancel_file(meta.stream_id))
        with self.assertRaises(FileTransferCancelled):
            await task
        cancel = await self.expect_cancel()
        self.assertEqual(json.loads(cancel.payload)["transfer_id"], meta.stream_id)
        self.assertEqual(self.local.active, 0)
        update = self.client.poll_file_updates()[0]
        self.assertEqual(update["status"], "cancelled")
        self.assertEqual(update["cancel_delivery"], "sent_to_server")
        self.assertEqual(update["acknowledged_bytes"], 0)
        self.assertFalse(await self.client.cancel_file(meta.stream_id))

    async def test_remote_cancel_only_from_receiver_and_no_echo(self):
        task = self.start_send()
        meta = await self.outgoing()
        payload = json.dumps(dict(transfer_id=meta.stream_id, reason="Stop")).encode()
        await self.reply(meta, MessageType.FILE_CANCEL, payload, stream_id="control", sender="other")
        await self.barrier()
        self.assertFalse(task.done())
        await self.reply(meta, MessageType.FILE_CANCEL, payload, stream_id="control")
        with self.assertRaises(FileTransferCancelled):
            await asyncio.wait_for(task, timeout=1)
        self.assertTrue(self.server._queue.empty())
        self.assertEqual(self.client.poll_file_updates()[0]["status"], "cancelled")

    async def test_cancel_delivery_timeout_is_reported_without_hanging(self):
        original_send = self.local.send

        async def send(peer, packet):
            if packet.kind == MessageType.FILE_CANCEL:
                await asyncio.Event().wait()
            await original_send(peer, packet)

        with patch.object(self.local, "send", send), patch("client.file_sender.FILE_CANCEL_TIMEOUT_SECONDS", 0.02):
            task = self.start_send()
            meta = await self.outgoing()
            self.assertTrue(await asyncio.wait_for(self.client.cancel_file(meta.stream_id), timeout=1))
            with self.assertRaises(FileTransferCancelled):
                await task
        update = self.client.poll_file_updates()[0]
        self.assertEqual((update["status"], update["cancel_delivery"]), ("cancelled", "failed"))
        self.assertIsNone(self.client._file_sender)

    async def test_second_task_cancel_does_not_interrupt_cleanup(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original_send = self.local.send

        async def send(peer, packet):
            if packet.kind == MessageType.FILE_CANCEL:
                entered.set()
                await release.wait()
            await original_send(peer, packet)

        with patch.object(self.local, "send", send):
            task = self.start_send()
            await self.outgoing()
            sender = self.client._file_sender
            task.cancel()
            await asyncio.wait_for(entered.wait(), timeout=1)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        await self.expect_cancel()
        self.assertTrue(sender.finished.is_set())
        self.assertIsNone(self.client._file_sender)
        self.assertEqual(self.client.poll_file_updates()[0]["status"], "cancelled")

    async def test_unpolled_status_capacity_rejects_new_file_before_meta(self):
        self.client.file_updates.limit = 1
        with patch("client.file_sender.MAX_SEND_FILE_BYTES", 1):
            with self.assertRaises(ValueError):
                await self.client.send_file("hai", self.path)
        with self.assertRaisesRegex(RuntimeError, "updates"):
            await self.client.send_file("hai", self.path)
        self.assertTrue(self.server._queue.empty())
        self.assertEqual(self.client.poll_file_updates()[0]["status"], "failed")
        task = self.start_send()
        meta = await self.outgoing()
        await self.client.cancel_file(meta.stream_id)
        with self.assertRaises(FileTransferCancelled):
            await task

    async def test_cancel_reaches_receiver_and_removes_part_through_relay(self):
        remote = MemoryTransport(self.hub, ("hai", 10002))
        await remote.start()
        receiver = FileReceiver(self.root / "received")
        other = ClientService(remote, self.server.address, "hai", file_receiver=receiver)
        other_task = asyncio.create_task(other.receive_events())
        self.tasks.append(other_task)
        cancelled = asyncio.Event()

        async def relay():
            while True:
                packet = (await self.server.receive()).packet
                if packet.kind == MessageType.ERROR and json.loads(packet.payload)["code"] == "FILE_CANCELLED":
                    cancelled.set()
                target = remote.address if packet.recipient == "hai" else self.local.address
                await self.server.send(target, packet)

        self.tasks.append(asyncio.create_task(relay()))
        self.local.block_chunks = True
        try:
            task = self.start_send()
            await asyncio.wait_for(self.local.full_batch.wait(), timeout=1)
            self.assertEqual(len(list(receiver.output_directory.glob("*.part"))), 1)
            transfer_id = self.client.poll_file_updates()[0]["transfer_id"]
            await self.client.cancel_file(transfer_id)
            with self.assertRaises(FileTransferCancelled):
                await task
            await asyncio.wait_for(cancelled.wait(), timeout=1)
            self.assertEqual(receiver.reserved_bytes, 0)
            self.assertEqual(list(receiver.output_directory.iterdir()), [])
        finally:
            other_task.cancel()
            await asyncio.gather(other_task, return_exceptions=True)
            await remote.close()


if __name__ == "__main__":
    unittest.main()
