"""Lưu phiên trong RAM và dùng đồng hồ monotonic để tính thời gian hết hạn."""

import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from ipaddress import ip_address

from common.config import OFFLINE_TIMEOUT_SECONDS
from common.models import Peer


@dataclass(frozen=True)
class Session:
    session_id: str
    username: str
    tcp_ip: str
    created_at: float
    last_seen: float
    peer: Peer | None = None


class SessionRegistry:
    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        bind_timeout: float = 10.0,
        offline_timeout: float = OFFLINE_TIMEOUT_SECONDS,
        max_sessions: int = 100,
    ) -> None:
        if bind_timeout <= 0 or offline_timeout <= 0:
            raise ValueError("Timeout must be greater than zero")

        if type(max_sessions) is not int or max_sessions < 1:
            raise ValueError("Invalid session limit")

        self._clock = clock
        self._bind_timeout = bind_timeout
        self._offline_timeout = offline_timeout
        self._max_sessions = max_sessions
        self._sessions: dict[str, Session] = {}

    def create(self, username: str, tcp_ip: str) -> str:
        if not isinstance(username, str) or username == 'server' or re.fullmatch(
            r"[A-Za-z0-9_-]{1,32}", username
        ) is None:
            raise ValueError("Invalid username")

        tcp_ip = str(ip_address(tcp_ip))

        if any(s.username == username for s in self._sessions.values()):
            raise ValueError("Username is already in use")

        if len(self._sessions) >= self._max_sessions:
            raise ValueError("Server session limit reached")

        session_id = secrets.token_hex(16)
        while session_id in self._sessions:
            session_id = secrets.token_hex(16)

        now = self._clock()
        self._sessions[session_id] = Session(
            session_id, username, tcp_ip, now, now
        )
        return session_id

    def get(self, session_id: str) -> Session:
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError("Session does not exist")

        now = self._clock()
        expired = (
            now - session.created_at >= self._bind_timeout
            if session.peer is None
            else now - session.last_seen >= self._offline_timeout
        )

        if expired:
            raise ValueError("Session expired")

        return session

    def bind_peer(self, session_id: str, peer: Peer) -> None:
        session = self.get(session_id)
        host, port = peer
        endpoint = (str(ip_address(host)), port)

        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("Invalid UDP port")

        if endpoint[0] != session.tcp_ip:
            raise ValueError("UDP IP does not match TCP IP")

        if session.peer is not None and session.peer != endpoint:
            raise ValueError("Session is already bound to another endpoint")

        if any(
            s.peer == endpoint and s.session_id != session_id
            for s in self._sessions.values()
        ):
            raise ValueError("Endpoint is in use by another session")

        self._sessions[session_id] = replace(
            session, peer=endpoint, last_seen=self._clock()
        )

    def touch(self, session_id: str) -> None:
        session = self.get(session_id)

        if session.peer is None:
            raise ValueError("Session has not completed BIND")

        self._sessions[session_id] = replace(
            session, last_seen=self._clock()
        )

    def remove(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    def expire(self, now: float) -> list[str]:
        offline = []

        for session_id, session in list(self._sessions.items()):
            expired = (
                now - session.created_at >= self._bind_timeout
                if session.peer is None
                else now - session.last_seen >= self._offline_timeout
            )

            if expired:
                del self._sessions[session_id]

                if session.peer is not None:
                    offline.append(session.username)

        return sorted(offline)

    def online_users(self) -> list[str]:
        now = self._clock()

        return sorted(
            s.username
            for s in self._sessions.values()
            if s.peer is not None
            and now - s.last_seen < self._offline_timeout
        )

    def sweep(self) -> list[Session]:
        """Xóa phiên hết hạn, trả danh sách để dọn kết nối UDP tương ứng."""
        before = dict(self._sessions)
        self.expire(self._clock())
        return [s for sid, s in before.items() if sid not in self._sessions]

    def active(self) -> list[Session]:
        return [s for s in self._sessions.values() if s.peer is not None
                and self._clock() - s.last_seen < self._offline_timeout]

    def clear(self) -> None:
        self._sessions.clear()
