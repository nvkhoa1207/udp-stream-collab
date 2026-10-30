import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from client.file_offers import FileOfferReceiver
from client.file_updates import FileUpdates
from client.files import FileReceiver
from common.models import MessageType
from test_client_file_receive import meta_packet, chunk_packet, end_packet, CONTENT
from test_file_cancel import cancel_packet


class ReceiveUpdatesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 0
        self.receiver = FileReceiver(self.root, clock=lambda: self.now)
        self.updates = FileUpdates()
        self.handler = FileOfferReceiver("khoa", self.receiver, updates=self.updates,
                                         clock=lambda: self.now)
        self.addCleanup(self.handler.close)
        self.id = uuid4().hex

    def start(self, **kwargs):
        reply = self.handler.handle(meta_packet(self.id, **kwargs))
        self.assertEqual(reply.kind, MessageType.FILE_ACCEPT)
        return self.updates.poll()[0]

    def chunks(self):
        for index in range(3):
            self.handler.handle(chunk_packet(self.id, index))

    def test_unique_written_bytes_reordered_duplicates_and_completion(self):
        update = self.start()
        self.assertEqual((update["direction"], update["sender"], update["status"]), ("receive", "hai", "receiving"))
        self.assertEqual((update["received_bytes"], update["total_bytes"]), (0, 1000))
        for index, expected in ((2, 40), (0, 520), (1, 1000)):
            self.handler.handle(chunk_packet(self.id, index))
            update = self.updates.poll()[0]
            self.assertEqual(update["received_bytes"], expected)
            self.assertEqual(update["status"], "receiving")
            self.handler.handle(chunk_packet(self.id, index))
            self.assertEqual(self.updates.poll(), [])
        original_finish = self.receiver.finish

        def finish(transfer_id):
            self.assertEqual(self.updates.poll()[0]["status"], "verifying")
            return original_finish(transfer_id)

        with patch.object(self.receiver, "finish", finish):
            reply = self.handler.handle(end_packet(self.id))
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["confirmation_delivery"]), ("completed", "pending"))
        self.assertEqual(len(self.updates.records), 1)
        self.handler.note_reply(reply, True)
        update = self.updates.poll()[0]
        self.assertEqual(update["confirmation_delivery"], "sent_to_server")
        self.assertFalse(self.updates.records)
        self.assertEqual((self.root / f"{self.id}_received.bin").read_bytes(), CONTENT)
        self.handler.note_reply(self.handler.handle(end_packet(self.id)), True)
        self.assertEqual(self.updates.poll(), [])

    def test_empty_file_completes_at_zero_bytes(self):
        self.start(content=b"")
        reply = self.handler.handle(end_packet(self.id))
        self.handler.note_reply(reply, True)
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["received_bytes"], update["total_bytes"]), ("completed", 0, 0))

    def test_wrong_hash_reports_failure_even_with_all_bytes(self):
        self.start(sha256="0" * 64)
        self.chunks()
        self.handler.handle(end_packet(self.id))
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["code"]), ("failed", "FILE_DATA_ERROR"))
        self.assertEqual(update["received_bytes"], 1000)
        self.assertFalse(update["cleanup_pending"])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_wrong_sender_and_repeated_meta_do_not_modify_active_snapshot(self):
        self.start()
        for packet in (meta_packet(self.id), chunk_packet(self.id, 0, sender="other"),
                       end_packet(self.id, sender="other"), cancel_packet(self.id, sender="other")):
            self.assertEqual(self.handler.handle(packet).kind, MessageType.ERROR)
        self.assertEqual(self.updates.poll(), [])
        self.assertEqual(self.receiver.transfers[self.id]["received_bytes"], 0)

    def test_idle_expiration_and_duplicate_do_not_extend_deadline(self):
        self.start()
        self.handler.handle(chunk_packet(self.id, 0))
        self.updates.poll()
        self.now = 44
        self.handler.handle(chunk_packet(self.id, 0))
        self.assertEqual(self.updates.poll(), [])
        self.now = 45
        self.assertEqual(self.handler.expire(), [self.id])
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["code"], update["received_bytes"]), ("expired", "FILE_EXPIRED", 480))
        self.assertFalse(update["cleanup_pending"])
        self.handler.expire()
        self.assertEqual(self.updates.poll(), [])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_begin_another_file_reports_implicit_expiration(self):
        self.start()
        old_id = self.id
        self.now = 45
        self.id = uuid4().hex
        self.handler.handle(meta_packet(self.id))
        updates = {item["transfer_id"]: item for item in self.updates.poll()}
        self.assertEqual(updates[old_id]["status"], "expired")
        self.assertEqual(updates[self.id]["status"], "receiving")

    def test_cleanup_failure_keeps_snapshot_until_retry_succeeds(self):
        self.start()
        self.now = 45
        with patch.object(Path, "unlink", side_effect=PermissionError("/private/path")):
            with self.assertRaises(ExceptionGroup):
                self.handler.expire()
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["code"]), ("failed", "FILE_STORAGE_ERROR"))
        self.assertTrue(update["cleanup_pending"])
        self.assertNotIn("/private/path", str(update))
        self.assertEqual(len(self.updates.records), 1)
        self.assertEqual(self.receiver.reserved_bytes, 1000)
        self.handler.expire()
        update = self.updates.poll()[0]
        self.assertEqual(update["status"], "failed")
        self.assertFalse(update["cleanup_pending"])
        self.assertFalse(self.updates.records)
        self.assertEqual(self.receiver.reserved_bytes, 0)

    def test_remote_cancel_and_shutdown_have_distinct_reasons(self):
        self.start()
        self.handler.handle(cancel_packet(self.id))
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["code"]), ("cancelled", "FILE_CANCELLED"))
        self.id = uuid4().hex
        self.start()
        self.handler.close()
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["code"]), ("cancelled", "CLIENT_CLOSED"))
        self.assertFalse(update["cleanup_pending"])

    def test_invalid_metadata_has_safe_failure_snapshot(self):
        self.handler.handle(meta_packet(self.id, name="../private/path"))
        update = self.updates.poll()[0]
        self.assertEqual(update["status"], "failed")
        self.assertEqual(update["code"], "FILE_REJECTED")
        self.assertNotIn("../private/path", str(update))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_status_capacity_rejects_before_disk_allocation(self):
        self.updates.limit = 1
        self.start()
        other = uuid4().hex
        reply = self.handler.handle(meta_packet(other))
        self.assertEqual(reply.kind, MessageType.ERROR)
        self.assertEqual(list(self.receiver.transfers), [self.id])
        self.assertFalse((self.root / f"{other}.part").exists())
        self.assertEqual(self.updates.poll(), [])

    def test_same_id_in_both_directions_does_not_mix_counters(self):
        self.updates.begin(self.id, "hai")
        self.handler.handle(meta_packet(self.id))
        self.handler.handle(chunk_packet(self.id, 0))
        self.updates.update(self.id, acknowledged_bytes=123)
        updates = {item["direction"]: item for item in self.updates.poll()}
        self.assertEqual(updates["send"]["acknowledged_bytes"], 123)
        self.assertEqual(updates["receive"]["received_bytes"], 480)
        self.assertNotIn("acknowledged_bytes", updates["receive"])

    def test_close_marks_unsent_complete_failed_without_deleting_file(self):
        self.start()
        self.chunks()
        self.handler.handle(end_packet(self.id))
        self.handler.close()
        update = self.updates.poll()[0]
        self.assertEqual((update["status"], update["confirmation_delivery"]), ("completed", "failed"))
        self.assertEqual(len(list(self.root.iterdir())), 1)


if __name__ == "__main__":
    unittest.main()
