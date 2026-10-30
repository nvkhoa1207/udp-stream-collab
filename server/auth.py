"""Xác thực tài khoản demo bằng PBKDF2-HMAC-SHA256."""

import asyncio
import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass


@dataclass(frozen=True)
class PasswordRecord:
    salt: bytes
    digest: bytes
    iterations: int


class AuthService:
    def __init__(self) -> None:
        self._accounts: dict[str, PasswordRecord] = {}
        # Tạo mẫu giả nhanh khi khởi động. Tài khoản không tồn tại vẫn được
        # tính PBKDF2 trong thread khi kiểm tra mật khẩu.
        self._dummy = PasswordRecord(secrets.token_bytes(16), secrets.token_bytes(32), 600_000)

    @staticmethod
    def _valid_credentials(username: str, password: str) -> bool:
        return (
            isinstance(username, str)
            and username != 'server'
            and re.fullmatch(r"[A-Za-z0-9_-]{1,32}", username) is not None
            and isinstance(password, str)
            and 1 <= len(password) <= 128
        )

    @staticmethod
    def _make_record(password: str) -> PasswordRecord:
        salt = secrets.token_bytes(16)
        iterations = 600_000
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, iterations
        )
        return PasswordRecord(salt, digest, iterations)

    def add_user(self, username: str, password: str) -> None:
        if not self._valid_credentials(username, password):
            raise ValueError("Invalid username or password")

        if username in self._accounts:
            raise ValueError("Account already exists")

        self._accounts[username] = self._make_record(password)

    async def verify(self, username: str, password: str) -> bool:
        if not self._valid_credentials(username, password):
            return False

        account = self._accounts.get(username)
        record = account if account is not None else self._dummy

        digest = await asyncio.to_thread(
            hashlib.pbkdf2_hmac,
            "sha256",
            password.encode("utf-8"),
            record.salt,
            record.iterations,
        )

        matches = hmac.compare_digest(digest, record.digest)
        return account is not None and matches
