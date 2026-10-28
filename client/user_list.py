"""Gom các trang USER_LIST trước khi cập nhật danh sách online của một phiên."""

import json
import re

from common.config import MAX_PAYLOAD_BYTES


# Giới hạn bộ nhớ phía client, không giữ nhiều revision đang chờ cùng lúc.
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
            raise ValueError("USER_LIST vượt giới hạn payload hoặc không phải bytes")
        try:
            data = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise ValueError("USER_LIST không phải JSON UTF-8 hợp lệ") from exc

        if type(data) is not dict or set(data) != {"revision", "page", "pages", "users"}:
            raise ValueError("USER_LIST thiếu hoặc thừa trường")
        revision, page, pages = data["revision"], data["page"], data["pages"]
        if (type(revision) is not int or revision < 0
                or type(pages) is not int or not 1 <= pages <= MAX_USER_LIST_PAGES
                or type(page) is not int or not 0 <= page < pages):
            raise ValueError("Revision hoặc chỉ số trang USER_LIST không hợp lệ")
        users = data["users"]
        if (type(users) is not list or any(
                type(name) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,32}", name) is None
                for name in users)):
            raise ValueError("Danh sách username không hợp lệ")

        if revision < self.revision or (revision == self.revision and self.complete):
            return None

        # Kiểm tra trước khi thay đổi state để trang lỗi không phá các trang tốt.
        pending = {}
        if revision == self.revision:
            if pages != self.page_count:
                raise ValueError("Số trang thay đổi trong cùng revision")
            if page in self.pages:
                if users != self.pages[page]:
                    raise ValueError("Nội dung trang thay đổi trong cùng revision")
                return None
            pending = dict(self.pages)

        pending[page] = list(users)
        all_users = []
        for page_index in sorted(pending):
            all_users.extend(pending[page_index])
        if len(all_users) > MAX_ONLINE_USERS:
            raise ValueError("Danh sách online vượt giới hạn của client")
        if len(set(all_users)) != len(all_users):
            raise ValueError("Username bị trùng trong danh sách online")

        self.revision = revision
        self.page_count = pages
        self.pages = pending
        self.complete = len(pending) == pages
        if not self.complete:
            return None

        self.pages = {}  # Đã có event đầy đủ, không giữ thêm bản sao các trang.
        return {"type": "users", "revision": revision, "users": all_users}
