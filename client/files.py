"""Owner Khoa. Chia/ghép file có thể làm trước, dùng file tạm và mock."""
import hashlib
import math
import os
import re
import time

from pathlib import Path
from typing import Callable, Iterator

from common.config import MAX_PAYLOAD_BYTES


def build_metadata(path: Path) -> dict:
    sha256 = hashlib.sha256()

    size = 0
    total_chunks = 0

    for chunk in iter_chunks(path):
        sha256.update(chunk)

        size += len(chunk)
        total_chunks += 1

    return {
        "name": path.name,
        "size": size,
        "sha256": sha256.hexdigest(),
        "chunk_size": MAX_PAYLOAD_BYTES,
        "total_chunks": total_chunks,
    }

def iter_chunks(
    path: Path,
    chunk_size: int = MAX_PAYLOAD_BYTES
) -> Iterator[bytes]:

    # Validate chunk size
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise TypeError("Chunk size must be an integer")

    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0")

    if chunk_size > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Chunk size cannot exceed {MAX_PAYLOAD_BYTES} bytes")

    # Read and yield chunks from file
    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)

            if chunk == b"":
                break

            yield chunk

class FileReceiver:
    """Nhận file local; caller tuần tự hóa các lệnh trên một instance.

    Quota dành ngay từ META; cancel/expire/close chỉ xóa .part do instance sở hữu.
    Caller phải gọi expire() định kỳ và close() khi logout/shutdown.
    """

    def __init__(
        self, output_directory: Path, *, max_file_bytes: int = 16 * 1024 * 1024,
        max_total_bytes: int = 64 * 1024 * 1024, max_transfers: int = 4,
        max_chunks: int = 65536, timeout_seconds: float = 45.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        for value in (max_file_bytes, max_total_bytes, max_transfers, max_chunks):
            if type(value) is not int or value <= 0:
                raise ValueError("Quota phải là số nguyên dương")
        if (type(timeout_seconds) not in (int, float)
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise ValueError("Timeout phải hữu hạn và dương")
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.max_file_bytes = max_file_bytes
        self.max_total_bytes = max_total_bytes
        self.max_transfers = max_transfers
        self.max_chunks = max_chunks
        self.timeout_seconds = timeout_seconds
        self._clock = clock
        self.transfers: dict[str, dict] = {}

    @property
    def reserved_bytes(self) -> int:
        return sum(t["metadata"]["size"] for t in self.transfers.values())

    def _metadata(self, metadata: dict) -> dict:
        fields = {"name", "size", "sha256", "chunk_size", "total_chunks"}
        if type(metadata) is not dict or set(metadata) != fields:
            raise ValueError("Metadata thiếu/thừa trường hoặc không phải object")
        name = metadata["name"]
        if (type(name) is not str or not name or name in (".", "..")
                or name.endswith((" ", "."))
                or any(ord(c) < 32 or ord(c) == 127 or c in '/\\:<>"|?*' for c in name)):
            raise ValueError("Tên file không hợp lệ")
        try:
            name_size = len(name.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ValueError("Tên file không phải UTF-8 hợp lệ") from exc
        stem = name.split(".")[0].upper()
        if (name_size > 128 or stem in {"CON", "PRN", "AUX", "NUL"}
                or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)):
            raise ValueError("Tên file quá dài hoặc là tên thiết bị")
        size, chunk_size, chunks = (metadata[k] for k in ("size", "chunk_size", "total_chunks"))
        if type(size) is not int or not 0 <= size <= self.max_file_bytes:
            raise ValueError("Kích thước file không hợp lệ hoặc vượt quota")
        if type(chunk_size) is not int or not 1 <= chunk_size <= MAX_PAYLOAD_BYTES:
            raise ValueError("Kích thước chunk không hợp lệ")
        if (type(chunks) is not int or chunks != (size + chunk_size - 1) // chunk_size
                or chunks > self.max_chunks):
            raise ValueError("Số chunk sai hoặc vượt quota")
        digest = metadata["sha256"]
        if type(digest) is not str or re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None:
            raise ValueError("SHA-256 phải có 64 ký tự hex")
        if size == 0 and digest.lower() != hashlib.sha256(b"").hexdigest():
            raise ValueError("SHA-256 của file rỗng không đúng")
        return dict(metadata, sha256=digest.lower())

    def begin(self, transfer_id: str, metadata: dict) -> None:
        if type(transfer_id) is not str or re.fullmatch(r"[0-9a-f]{32}", transfer_id) is None:
            raise ValueError("Transfer ID phải là UUID hex thường, 32 ký tự")
        if transfer_id in self.transfers:
            raise ValueError("ID này đang được sử dụng")
        metadata = self._metadata(metadata)
        self.expire()
        if (len(self.transfers) >= self.max_transfers
                or self.reserved_bytes + metadata["size"] > self.max_total_bytes):
            raise ValueError("Đã hết quota nhận file")
        temp_path = self.output_directory / f"{transfer_id}.part"
        final_path = self.output_directory / f"{transfer_id}_{metadata['name']}"
        if final_path.exists() or final_path.is_symlink():
            raise FileExistsError("File đích đã tồn tại")
        now = self._clock()
        # 'x' không ghi đè tệp tạm của transfer/process khác, kể cả symlink.
        try:
            with temp_path.open("xb"):
                self.transfers[transfer_id] = {
                    "metadata": metadata, "temp_path": temp_path, "final_path": final_path,
                    "received": set(), "received_bytes": 0, "last_progress": now, "failed": False,
                    "published": False,
                }
        except Exception:
            # Kể cả lỗi đóng file sau khi tạo: chỉ dọn file vừa thuộc instance.
            if transfer_id in self.transfers:
                self.cancel(transfer_id)
            raise

    def cancel(self, transfer_id: str) -> bool:
        transfer = self.transfers.get(transfer_id)
        if transfer is None:
            return False
        transfer["failed"] = True
        # Nếu unlink lỗi, giữ state/quota để caller biết và có thể thử dọn lại.
        transfer["temp_path"].unlink(missing_ok=True)
        del self.transfers[transfer_id]
        return True

    def expire(self) -> list[str]:
        now = self._clock()
        expired = []
        errors = []
        for transfer_id, transfer in list(self.transfers.items()):
            if transfer["failed"] or now - transfer["last_progress"] >= self.timeout_seconds:
                try:
                    self.cancel(transfer_id)
                    expired.append(transfer_id)
                except OSError as exc:
                    errors.append(exc)
        if errors:
            raise ExceptionGroup("Không dọn được một số file tạm", errors)
        return expired

    def close(self) -> None:
        errors = []
        for transfer_id in list(self.transfers):
            try:
                self.cancel(transfer_id)
            except OSError as exc:
                errors.append(exc)
        if errors:
            raise ExceptionGroup("Không dọn được một số file tạm", errors)

    def _active(self, transfer_id: str) -> dict:
        transfer = self.transfers[transfer_id]
        if transfer["failed"]:
            raise ValueError("Transfer đã lỗi; cần dọn bằng cancel/expire/close")
        if self._clock() - transfer["last_progress"] >= self.timeout_seconds:
            self.cancel(transfer_id)
            raise TimeoutError("Transfer quá hạn không có tiến triển")
        return transfer

    def accept_chunk(self, transfer_id: str, index: int, data: bytes) -> None:
        transfer = self._active(transfer_id)
        metadata = transfer["metadata"]
        try:
            if type(index) is not int or not 0 <= index < metadata["total_chunks"]:
                raise ValueError("Chỉ số chunk không hợp lệ")
            offset = index * metadata["chunk_size"]
            expected_size = min(metadata["chunk_size"], metadata["size"] - offset)
            if not isinstance(data, bytes) or len(data) != expected_size:
                raise ValueError("Dữ liệu chunk không hợp lệ")
            if index in transfer["received"]:
                return  # Gói trùng không kéo dài timeout.
            with transfer["temp_path"].open("r+b") as file:
                file.seek(offset)
                if file.write(data) != len(data):
                    raise OSError("Ghi chunk không đủ byte")
            transfer["received"].add(index)
            transfer["received_bytes"] += len(data)
            transfer["last_progress"] = self._clock()
        except Exception:
            self.cancel(transfer_id)
            raise

    def finish(self, transfer_id: str) -> Path:
        transfer = self._active(transfer_id)
        metadata = transfer["metadata"]
        try:
            if not transfer["published"]:
                if len(transfer["received"]) != metadata["total_chunks"]:
                    raise ValueError("Chưa nhận đủ các chunk")
                actual = build_metadata(transfer["temp_path"])
                if actual["size"] != metadata["size"] or actual["sha256"] != metadata["sha256"]:
                    raise ValueError("Size hoặc SHA-256 không khớp")
                # Cùng filesystem: tạo tên đích nguyên tử, thất bại nếu đã tồn tại.
                # Không dùng rename vì có thể ghi đè đích xuất hiện sau kiểm tra.
                os.link(transfer["temp_path"], transfer["final_path"])
                transfer["published"] = True
            transfer["temp_path"].unlink()
            del self.transfers[transfer_id]
            return transfer["final_path"]
        except Exception:
            if not transfer["published"]:
                self.cancel(transfer_id)
            # Nếu đã publish nhưng unlink lỗi, giữ state để retry finish/dọn lại;
            # không xóa file hoàn chỉnh và không báo hoàn tất khi cleanup chưa xong.
            raise
