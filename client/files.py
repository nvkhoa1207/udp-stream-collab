"""Owner Khoa. Chia/ghép file có thể làm trước, dùng file tạm và mock."""
import hashlib

from pathlib import Path
from typing import Iterator

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
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise TypeError("chunk_size must be an integer")
    if chunk_size <= 0 or chunk_size > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"chunk_size phải từ 1 đến {MAX_PAYLOAD_BYTES} byte"
        )

    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)

            if not chunk:
                break

            yield chunk

class FileReceiver:
    def __init__(self, output_directory: Path) -> None:
        self.output_directory = output_directory

    def begin(self, transfer_id: str, metadata: dict) -> None:
        # TODO[K3]: validate name/size/hash/chunks, tạo .part, không overwrite bừa.
        raise NotImplementedError("TODO[K3]: FileReceiver.begin")

    def accept_chunk(self, transfer_id: str, index: int, data: bytes) -> None:
        # TODO[K3]: index hợp lệ, chunk cuối, dedup/reorder, ghi có giới hạn bộ nhớ.
        raise NotImplementedError("TODO[K3]: FileReceiver.accept_chunk")

    def finish(self, transfer_id: str) -> Path:
        # TODO[K4]: đủ chunk + size + SHA-256 đúng mới đổi tên và trả path.
        raise NotImplementedError("TODO[K4]: FileReceiver.finish")
