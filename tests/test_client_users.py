import asyncio
import json
import unittest
from unittest.mock import patch

from client.service import ClientService
from client.user_list import UserListAssembler
from common.models import MessageType, Packet
from mocks.memory_transport import MemoryHub, MemoryTransport


def page_payload(revision=1, page=0, pages=1, users=None):
    return json.dumps({
        "revision": revision, "page": page, "pages": pages,
        "users": ["khoa"] if users is None else users,
    }).encode("utf-8")


class UserListAssemblerTests(unittest.TestCase):
    def setUp(self):
        self.assembler = UserListAssembler()

    def test_reordered_pages_only_publish_when_complete(self):
        last = page_payload(page=1, pages=2, users=["hai"])
        self.assertIsNone(self.assembler.add(last))
        self.assertIsNone(self.assembler.add(last))
        event = self.assembler.add(page_payload(page=0, pages=2))
        self.assertEqual(event, {
            "type": "users", "revision": 1, "users": ["khoa", "hai"],
        })
        self.assertIsNone(self.assembler.add(last))
        self.assertEqual(self.assembler.pages, {})

    def test_new_revision_after_complete_and_old_pages_ignored(self):
        self.assembler.add(page_payload(revision=1))
        self.assertIsNone(self.assembler.add(page_payload(
            revision=3, page=1, pages=2, users=["hai"])))
        self.assertIsNone(self.assembler.add(page_payload(revision=2)))
        self.assertIsNone(self.assembler.add(page_payload(revision=1)))
        event = self.assembler.add(page_payload(revision=3, pages=2))
        self.assertEqual(event["users"], ["khoa", "hai"])
        self.assertEqual(event["revision"], 3)

    def test_new_revision_replaces_incomplete_snapshot(self):
        self.assembler.add(page_payload(revision=1, pages=2, users=["old"]))
        self.assembler.add(page_payload(revision=2, pages=2, users=["new"]))
        self.assertIsNone(self.assembler.add(page_payload(
            revision=1, page=1, pages=2, users=["late"])))
        event = self.assembler.add(page_payload(
            revision=2, page=1, pages=2, users=["hai"]))
        self.assertEqual(event["users"], ["new", "hai"])

    def test_empty_list_is_a_complete_snapshot(self):
        self.assertEqual(self.assembler.add(page_payload(users=[])), {
            "type": "users", "revision": 1, "users": [],
        })

    def test_bad_payloads_rejected(self):
        cases = [b"\xff", b"{", b"[]", b"{}", b" " * 481, "not bytes"]
        for changes in [
            {"revision": True}, {"revision": -1}, {"page": True},
            {"page": -1}, {"page": 1}, {"pages": 0}, {"pages": 129},
            {"pages": True}, {"users": "khoa"}, {"users": [True]},
            {"users": ["*"]}, {"users": ["a" * 33]},
            {"users": ["tên có dấu"]}, {"users": ["khoa", "khoa"]},
        ]:
            cases.append(page_payload(**changes))
        cases.append(b'{"revision":1,"page":0,"pages":1,"users":[],"extra":0}')
        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.assembler.add(payload)
        self.assertEqual(self.assembler.revision, -1)
        self.assertEqual(self.assembler.pages, {})

    def test_bad_page_does_not_destroy_valid_pending_pages(self):
        self.assembler.add(page_payload(pages=2))
        bad = [
            page_payload(pages=3, page=1),
            page_payload(pages=2, users=["changed"]),
            page_payload(pages=2, page=1, users=["khoa"]),
            page_payload(revision=2, pages=129),
        ]
        for payload in bad:
            with self.assertRaises(ValueError):
                self.assembler.add(payload)
        event = self.assembler.add(page_payload(pages=2, page=1, users=["hai"]))
        self.assertEqual(event["users"], ["khoa", "hai"])

    def test_user_quota_is_checked_across_pages(self):
        with patch("client.user_list.MAX_ONLINE_USERS", 2):
            self.assembler.add(page_payload(pages=2, users=["khoa", "hai"]))
            with self.assertRaises(ValueError):
                self.assembler.add(page_payload(page=1, pages=2, users=["friend"]))
            self.assertEqual(self.assembler.pages, {0: ["khoa", "hai"]})


class ClientUsersTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        hub = MemoryHub()
        self.server = MemoryTransport(hub, ("server", 9001))
        self.local = MemoryTransport(hub, ("client", 10001))
        self.other = MemoryTransport(hub, ("other", 10002))
        for transport in (self.server, self.local, self.other):
            await transport.start()
        self.client = ClientService(self.local, self.server.address, "khoa")
        self.task = asyncio.create_task(self.client.receive_events())

    async def asyncTearDown(self):
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        for transport in (self.server, self.local, self.other):
            await transport.close()

    async def send_page(self, payload, *, sender="server", recipient="khoa", transport=None):
        await (transport or self.server).send(self.local.address, Packet(
            kind=MessageType.USER_LIST, sender=sender, recipient=recipient,
            stream_id="control", payload=payload,
        ))

    async def collect_before_marker(self):
        # Marker ở sau các gói thử trong cùng queue: không dựa vào sleep để kiểm tra.
        await self.server.send(self.local.address, Packet(
            kind=MessageType.MSG, sender="hai", recipient="khoa",
            stream_id="chat-khoa", payload=b"marker",
        ))
        events = []
        while True:
            event = await asyncio.wait_for(self.client.events.get(), timeout=1)
            if event.get("type") == "message" and event.get("text") == "marker":
                return events
            events.append(event)

    async def test_chat_continues_while_waiting_for_remaining_page(self):
        await self.send_page(page_payload(page=1, pages=2, users=["hai"]))
        self.assertEqual(await self.collect_before_marker(), [])
        await self.send_page(page_payload(pages=2))
        self.assertEqual(await self.collect_before_marker(), [
            {"type": "users", "revision": 1, "users": ["khoa", "hai"]},
        ])

    async def test_wrong_peer_sender_and_recipient_ignored(self):
        await self.send_page(page_payload(), transport=self.other)
        await self.send_page(page_payload(), sender="hai")
        await self.send_page(page_payload(), recipient="friend")
        self.assertEqual(await self.collect_before_marker(), [])
        await self.send_page(page_payload(), recipient="*")
        self.assertEqual(len(await self.collect_before_marker()), 1)

    async def test_bad_payload_reports_error_and_receiving_continues(self):
        await self.send_page(b"\xff")
        await self.send_page(page_payload())
        events = await self.collect_before_marker()
        self.assertEqual([event["type"] for event in events], ["error", "users"])
        self.assertFalse(self.task.done())

    async def test_duplicate_and_old_snapshots_do_not_repeat_events(self):
        await self.send_page(page_payload(revision=2))
        await self.send_page(page_payload(revision=2))
        await self.send_page(page_payload(revision=1))
        events = await self.collect_before_marker()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["revision"], 2)


if __name__ == "__main__":
    unittest.main()
