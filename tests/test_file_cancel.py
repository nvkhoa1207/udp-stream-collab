import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from client.file_offers import FileOfferReceiver
from client.file_updates import FileUpdates
from client.files import FileReceiver
from common.models import MessageType, Packet
from test_client_file_receive import meta_packet, chunk_packet, end_packet


def cancel_packet(transfer_id, sender="hai"):
    return Packet(kind=MessageType.FILE_CANCEL, sender=sender, recipient="khoa", stream_id="control",
                  payload=json.dumps(dict(transfer_id=transfer_id, reason="Stop")).encode())


class FileCancelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.receiver = FileReceiver(self.root)
        self.now = 0
        self.handler = FileOfferReceiver("khoa", self.receiver, clock=lambda: self.now)
        self.addCleanup(self.handler.close)
        self.id = uuid4().hex

    def test_active_cancel_cleans_and_duplicate_does_not_extend_cache(self):
        self.handler.handle(meta_packet(self.id))
        self.handler.handle(chunk_packet(self.id, 0))
        result = self.handler.handle(cancel_packet(self.id))
        self.assertEqual(json.loads(result.payload)["code"], "FILE_CANCELLED")
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.iterdir()), [])
        self.now = 119
        self.assertEqual(self.handler.handle(cancel_packet(self.id)), result)
        self.assertEqual(self.handler.results[self.id][2], 120)
        self.assertEqual(self.handler.handle(chunk_packet(self.id, 1)), result)
        self.assertEqual(self.handler.handle(meta_packet(self.id)).kind, MessageType.ERROR)
        self.assertFalse(self.receiver.transfers)

    def test_cancel_before_meta_prevents_late_allocation(self):
        self.handler.handle(cancel_packet(self.id))
        self.assertEqual(self.handler.handle(meta_packet(self.id)).kind, MessageType.ERROR)
        self.now = 120
        self.handler.expire()
        self.assertEqual(self.handler.handle(meta_packet(self.id)).kind, MessageType.ERROR)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_other_sender_cannot_cancel_active_or_completed_transfer(self):
        self.handler.handle(meta_packet(self.id))
        result = self.handler.handle(cancel_packet(self.id, sender="other"))
        self.assertEqual(json.loads(result.payload)["code"], "FILE_SENDER_MISMATCH")
        self.assertIn(self.id, self.receiver.transfers)
        for index in range(3):
            self.handler.handle(chunk_packet(self.id, index))
        completed = self.handler.handle(end_packet(self.id))
        result = self.handler.handle(cancel_packet(self.id, sender="other"))
        self.assertEqual(json.loads(result.payload)["code"], "FILE_SENDER_MISMATCH")
        self.assertIsNone(self.handler.handle(cancel_packet(self.id)))
        self.assertEqual(self.handler.results[self.id][1], completed)
        self.assertEqual(len(list(self.root.iterdir())), 1)

    def test_invalid_cancel_does_not_allocate_or_destroy(self):
        self.handler.handle(meta_packet(self.id))
        valid = cancel_packet(self.id)
        packets = [replace(valid, payload=b"[]"), replace(valid, stream_id=self.id),
                   replace(valid, recipient="*"), replace(valid, chunk_index=0),
                   replace(valid, payload=b"x" * 481), replace(valid, payload=b"{"),
                   replace(valid, payload=json.dumps(dict(transfer_id=self.id, reason=True)).encode())]
        for packet in packets:
            self.assertIsNone(self.handler.handle(packet))
        self.assertIn(self.id, self.receiver.transfers)
        self.assertEqual(len(self.handler.seen_ids), 1)

    def test_cancel_cleanup_failure_keeps_quota_and_retries(self):
        self.handler.handle(meta_packet(self.id))
        with patch.object(Path, "unlink", side_effect=PermissionError("busy")):
            result = self.handler.handle(cancel_packet(self.id))
        self.assertEqual(json.loads(result.payload)["code"], "FILE_STORAGE_ERROR")
        self.assertEqual(self.receiver.reserved_bytes, 1000)
        self.handler.expire()
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_unknown_cancel_obeys_session_quota(self):
        with patch("client.file_offers.MAX_FILE_OFFERS_PER_SESSION", 1):
            self.handler.handle(cancel_packet(self.id))
            other = uuid4().hex
            result = self.handler.handle(cancel_packet(other))
            self.assertEqual(json.loads(result.payload)["code"], "FILE_SESSION_LIMIT")
            self.assertEqual(len(self.handler.seen_ids), 1)
            self.assertEqual(self.handler.handle(meta_packet(other)).kind, MessageType.ERROR)


class FileUpdatesTests(unittest.TestCase):
    def test_many_updates_coalesce_without_losing_terminal_result(self):
        updates = FileUpdates(limit=1)
        updates.begin("id", "hai")
        for number in range(10000):
            updates.update("id", acknowledged_bytes=number)
        self.assertEqual(len(updates.records), 1)
        snapshot = updates.poll()[0]
        self.assertEqual(snapshot["acknowledged_bytes"], 9999)
        snapshot["acknowledged_bytes"] = 0
        self.assertEqual(updates.records[("send", "id")]["acknowledged_bytes"], 9999)
        self.assertEqual(updates.poll(), [])
        with self.assertRaises(RuntimeError):
            updates.begin("second", "hai")
        updates.update("id", status="completed")
        with self.assertRaises(RuntimeError):
            updates.begin("second", "hai")
        self.assertEqual(updates.poll()[0]["status"], "completed")
        updates.begin("second", "hai")


if __name__ == "__main__":
    unittest.main()
