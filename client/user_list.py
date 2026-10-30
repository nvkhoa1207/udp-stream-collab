"""Gom các trang USER_LIST trước khi cập nhật danh sách online của một phiên."""

import json
import re

from common.config import MAX_PAYLOAD_BYTES


# Chỉ giữ một phiên bản danh sách đang chờ để tiết kiệm bộ nhớ.
MAX_USER_LIST_PAGES = 128
MAX_ONLINE_USERS = 1024


class UserListAssembler:
    def __init__(self) -> None:
        self.revision = -1
        self.page_count = 0
        self.pages: dict[int, list[str]] = {}
        self.complete = False

    def add(self, payload: bytes) -> dict | None:
        """Trả event khi đủ trang; None nếu còn thiếu, trùng hoặc đã cũ."""
        if not isinstance(payload, bytes) or len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("USER_LIST exceeds the payload limit or is not bytes")
        try:
            data = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise ValueError("USER_LIST is not valid UTF-8 JSON") from exc

        if type(data) is not dict or set(data) != {"revision", "page", "pages", "users"}:
            raise ValueError("USER_LIST has missing or extra fields")
        revision, page, pages = data["revision"], data["page"], data["pages"]
        if (type(revision) is not int or revision < 0
                or type(pages) is not int or not 1 <= pages <= MAX_USER_LIST_PAGES
                or type(page) is not int or not 0 <= page < pages):
            raise ValueError("Invalid USER_LIST revision or page index")
        users = data["users"]
        if (type(users) is not list or any(
                type(name) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,32}", name) is None
                for name in users)):
            raise ValueError("Invalid username list")

        if revision < self.revision or (revision == self.revision and self.complete):
            return None

        # Kiểm tra trang mới trước khi sửa danh sách đã nhận.
        pending = {}
        if revision == self.revision:
            if pages != self.page_count:
                raise ValueError("Page count changed within the same revision")
            if page in self.pages:
                if users != self.pages[page]:
                    raise ValueError("Page content changed within the same revision")
                return None
            pending = dict(self.pages)

        pending[page] = list(users)
        all_users = []
        for page_index in sorted(pending):
            all_users.extend(pending[page_index])
        if len(all_users) > MAX_ONLINE_USERS:
            raise ValueError("Online user list exceeds the client limit")
        if len(set(all_users)) != len(all_users):
            raise ValueError("Duplicate username in the online user list")

        self.revision = revision
        self.page_count = pages
        self.pages = pending
        self.complete = len(pending) == pages
        if not self.complete:
            return None

        self.pages = {}  # Đã ghép xong danh sách, có thể bỏ các trang rời.
        return {"type": "users", "revision": revision, "users": all_users}
