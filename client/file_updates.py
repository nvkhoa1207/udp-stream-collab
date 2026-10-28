"""Snapshot file; lock bảo vệ khi worker đĩa cập nhật và event loop polling."""

from threading import Lock


class FileUpdates:
    def __init__(self, limit=100):
        self.limit = limit
        self.records = {}
        self.dirty = set()
        self._lock = Lock()

    def begin(self, transfer_id, peer, *, direction="send"):
        key = (direction, transfer_id)
        with self._lock:
            if key in self.records or len(self.records) >= self.limit:
                raise RuntimeError("Cần đọc cập nhật file trước khi mở thêm lượt truyền")
            record = dict(type="file", direction=direction, transfer_id=transfer_id,
                          status="preparing", total_bytes=None, cleanup_pending=False)
            if direction == "send":
                record.update(recipient=peer, acknowledged_bytes=0, cancel_delivery="not_needed")
            else:
                record.update(sender=peer, received_bytes=0, confirmation_delivery="not_needed")
            self.records[key] = record
            self.dirty.add(key)

    def update(self, transfer_id, *, direction="send", **changes):
        key = (direction, transfer_id)
        with self._lock:
            # Phản hồi cache đến muộn không tạo lại snapshot đã được UI đọc.
            if key not in self.records:
                return
            self.records[key].update(changes)
            self.dirty.add(key)

    def poll(self):
        updates = []
        with self._lock:
            for key, record in list(self.records.items()):
                if key not in self.dirty:
                    continue
                updates.append(dict(record))
                terminal = record["status"] in ("completed", "cancelled", "failed", "expired")
                pending = record["cleanup_pending"] or record.get("confirmation_delivery") == "pending"
                if terminal and not pending:
                    del self.records[key]
            self.dirty.clear()
        return updates
