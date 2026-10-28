import unittest

from server.auth import AuthService


class AuthTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.auth = AuthService()
        cls.auth.add_user("hai", "MatKhauDemo123")
        cls.auth.add_user("khoa", "MatKhauDemo123")
        cls.auth.add_user("unicode", "mật khẩu tiếng Việt")

    async def test_correct_password(self):
        self.assertTrue(
            await self.auth.verify("hai", "MatKhauDemo123")
        )

    async def test_wrong_password(self):
        self.assertFalse(await self.auth.verify("hai", "wrong"))

    async def test_unknown_user(self):
        self.assertFalse(
            await self.auth.verify("missing", "MatKhauDemo123")
        )

    async def test_unicode_password(self):
        self.assertTrue(
            await self.auth.verify("unicode", "mật khẩu tiếng Việt")
        )

    async def test_invalid_credentials(self):
        for username, password in (
            ("", "password"),
            ("bad name", "password"),
            ("x" * 33, "password"),
            ("hai", ""),
            ("hai", "x" * 129),
            (None, "password"),
            ("hai", None),
        ):
            with self.subTest(username=username):
                self.assertFalse(
                    await self.auth.verify(username, password)
                )

    def test_reject_duplicate_user(self):
        with self.assertRaises(ValueError):
            self.auth.add_user("hai", "another-password")

    def test_reject_invalid_new_user(self):
        with self.assertRaises(ValueError):
            self.auth.add_user("bad name", "password")

    def test_passwords_have_independent_salts(self):
        first = self.auth._accounts["hai"]
        second = self.auth._accounts["khoa"]

        self.assertNotEqual(first.salt, second.salt)
        self.assertNotEqual(first.digest, second.digest)
        self.assertEqual(len(first.digest), 32)