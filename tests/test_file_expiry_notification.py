import json
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from client.file_offers import FileOfferReceiver
from client.files import FileReceiver
from common.models import MessageType
from test_client_file_receive import meta_packet, end_packet


class FileExpiryNotificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 0
        self.receiver = FileReceiver(Path(self.temp.name), clock=lambda: self.now)
        self.handler = FileOfferReceiver("khoa", self.receiver, clock=lambda: self.now)
        self.addCleanup(self.handler.close)
        self.meta = meta_packet(uuid4().hex)
        self.handler.handle(self.meta)

    def test_expiry_correlates_meta_and_notifies_once(self):
        self.now = 45
        self.handler.expire()
        replies = self.handler.take_notifications()
        self.assertEqual(len(replies), 1)
        reply = replies[0]
        self.assertEqual((reply.kind, reply.stream_id, reply.recipient), (MessageType.ERROR, "control", "hai"))
        self.assertEqual(json.loads(reply.payload)["related_message_id"], self.meta.message_id)
        self.assertEqual(json.loads(reply.payload)["code"], "FILE_EXPIRED")
        self.handler.expire()
        self.assertEqual(self.handler.take_notifications(), [])
        self.assertEqual(self.handler.handle(end_packet(self.meta.stream_id)), reply)
        self.assertFalse(self.handler._offers)
        self.assertEqual(self.receiver.reserved_bytes, 0)

    def test_opening_another_file_also_notifies_expired_owner(self):
        self.now = 45
        other = meta_packet(uuid4().hex, sender="friend")
        self.assertEqual(self.handler.handle(other).kind, MessageType.FILE_ACCEPT)
        reply = self.handler.take_notifications()[0]
        self.assertEqual(reply.recipient, "hai")
        self.assertEqual(json.loads(reply.payload)["related_message_id"], self.meta.message_id)
        self.assertEqual(list(self.receiver.transfers), [other.stream_id])

    def test_shutdown_discards_unsent_notifications(self):
        self.now = 45
        self.handler.expire()
        self.handler.close()
        self.assertEqual(self.handler.take_notifications(), [])
        self.assertFalse(self.handler._offers)


if __name__ == "__main__":
    unittest.main()
