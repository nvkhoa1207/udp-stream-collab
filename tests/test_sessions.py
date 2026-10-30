import unittest

from server.sessions import SessionRegistry


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.registry = SessionRegistry(clock=lambda: self.now)
        self.sid = self.registry.create("hai", "127.0.0.1")

    def test_pending_not_online(self):
        self.assertRegex(self.sid, r"^[0-9a-f]{32}$")
        self.assertEqual(self.registry.online_users(), [])

    def test_duplicate_username(self):
        with self.assertRaises(ValueError):
            self.registry.create("hai", "127.0.0.1")

    def test_bind_and_duplicate_bind(self):
        peer = ("127.0.0.1", 12000)

        self.registry.bind_peer(self.sid, peer)
        self.registry.bind_peer(self.sid, peer)

        self.assertEqual(self.registry.get(self.sid).peer, peer)
        self.assertEqual(self.registry.online_users(), ["hai"])

    def test_reject_wrong_ip(self):
        with self.assertRaises(ValueError):
            self.registry.bind_peer(
                self.sid, ("127.0.0.2", 12000)
            )

        self.assertIsNone(self.registry.get(self.sid).peer)

    def test_reject_endpoint_change(self):
        self.registry.bind_peer(
            self.sid, ("127.0.0.1", 12000)
        )

        with self.assertRaises(ValueError):
            self.registry.bind_peer(
                self.sid, ("127.0.0.1", 12001)
            )

    def test_reject_endpoint_reuse(self):
        self.registry.bind_peer(
            self.sid, ("127.0.0.1", 12000)
        )
        other = self.registry.create("khoa", "127.0.0.1")

        with self.assertRaises(ValueError):
            self.registry.bind_peer(
                other, ("127.0.0.1", 12000)
            )

    def test_pending_timeout(self):
        self.now += 10

        with self.assertRaises(ValueError):
            self.registry.bind_peer(
                self.sid, ("127.0.0.1", 12000)
            )

        self.assertEqual(self.registry.expire(self.now), [])
        self.registry.create("hai", "127.0.0.1")

    def test_touch_extends_online_session(self):
        self.registry.bind_peer(
            self.sid, ("127.0.0.1", 12000)
        )

        self.now += 9
        self.registry.touch(self.sid)

        self.now += 9
        self.assertEqual(self.registry.expire(self.now), [])

        self.now += 1
        self.assertEqual(
            self.registry.expire(self.now), ["hai"]
        )
        self.assertEqual(self.registry.online_users(), [])

    def test_touch_cannot_revive_expired_session(self):
        self.registry.bind_peer(
            self.sid, ("127.0.0.1", 12000)
        )
        self.now += 10

        with self.assertRaises(ValueError):
            self.registry.touch(self.sid)

    def test_pending_cannot_touch(self):
        with self.assertRaises(ValueError):
            self.registry.touch(self.sid)

    def test_remove_allows_relogin(self):
        self.registry.remove(self.sid)
        self.registry.remove(self.sid)

        new_sid = self.registry.create("hai", "127.0.0.1")
        self.assertNotEqual(new_sid, self.sid)

    def test_capacity(self):
        registry = SessionRegistry(max_sessions=1)
        registry.create("hai", "127.0.0.1")

        with self.assertRaises(ValueError):
            registry.create("khoa", "127.0.0.1")