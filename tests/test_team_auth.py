"""Thử đăng nhập bằng client TCP và server thật của nhóm."""
import asyncio
import unittest

from client.auth import authenticate
from server.app import ServerApp
from server.auth import AuthService
from server.sessions import SessionRegistry


class TeamAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_client_duplicate_and_pending_expiry(self):
        auth = await asyncio.to_thread(AuthService)
        await asyncio.to_thread(auth.add_user, 'hai', 'test-password')
        clock = [0.0]
        sessions = SessionRegistry(clock=lambda: clock[0], bind_timeout=1)
        app = ServerApp(tcp_port=0, auth=auth, sessions=sessions)
        await app.start()
        try:
            with self.assertRaises(PermissionError):
                await authenticate(app.address, 'hai', 'wrong')
            _, sid = await authenticate(app.address, 'hai', 'test-password')
            self.assertIsNone(sessions.get(sid).peer)
            self.assertEqual(sessions.online_users(), [])
            with self.assertRaises(PermissionError):
                await authenticate(app.address, 'hai', 'test-password')
            clock[0] = 2
            await asyncio.sleep(0.15)
            _, new_sid = await authenticate(app.address, 'hai', 'test-password')
            self.assertNotEqual(sid, new_sid)
        finally:
            await app.close()
        self.assertFalse(app._tasks)
        self.assertFalse(sessions._sessions)
