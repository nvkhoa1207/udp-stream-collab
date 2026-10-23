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
    def __init__(self, output_directory: Path) -> None:
        self.output_directory = output_directory
        self.output_directory.mkdir(parents=True, exist_ok=True)

        self.transfers = {}

    def begin(self, transfer_id: str, metadata: dict) -> None:
        if not transfer_id.isalnum():
            raise ValueError("ID chỉ được chứa chữ và số")

        if transfer_id in self.transfers:
            raise ValueError("ID này đang được sử dụng")

        name = metadata["name"]
        size = metadata["size"]
        chunk_size = metadata["chunk_size"]
        total_chunks = metadata["total_chunks"]

        # file name not include url
        if not name or name in (".", "..") or "/" in name or "\\" in name:
            raise ValueError("Tên file không hợp lệ")

        if type(size) is not int or size < 0:
            raise ValueError("Kích thước file không hợp lệ")

        if type(chunk_size) is not int or not 1 <= chunk_size <= MAX_PAYLOAD_BYTES:
            raise ValueError("Kích thước chunk không hợp lệ")

        expected_chunks = (size + chunk_size - 1) // chunk_size

        if type(total_chunks) is not int or total_chunks != expected_chunks:
            raise ValueError("Số chunk không khớp kích thước file")

        temp_path = self.output_directory / f"{transfer_id}.part"
        final_path = self.output_directory / f"{transfer_id}_{name}"

        if final_path.exists():
            raise FileExistsError("File đích đã tồn tại")

        temp_path.touch(exist_ok=False)

        self.transfers[transfer_id] = {
            "metadata": metadata.copy(),
            "temp_path": temp_path,
            "final_path": final_path,
            "received": set(),
        }

    def accept_chunk(
        self,
        transfer_id: str,
        index: int,
        data: bytes
    ) -> None:
        transfer = self.transfers[transfer_id]
        metadata = transfer["metadata"]

        if type(index) is not int or not 0 <= index < metadata["total_chunks"]:
            raise ValueError("Chỉ số chunk không hợp lệ")

        offset = index * metadata["chunk_size"]

        expected_size = min(
            metadata["chunk_size"],
            metadata["size"] - offset
        )

        if not isinstance(data, bytes) or len(data) != expected_size:
            raise ValueError("Dữ liệu chunk không hợp lệ")

        # Chunk đã nhận rồi thì bỏ qua.
        if index in transfer["received"]:
            return

        with transfer["temp_path"].open("r+b") as file:
            file.seek(offset)
            file.write(data)

        transfer["received"].add(index)

    def finish(self, transfer_id: str) -> Path:
        transfer = self.transfers[transfer_id]
        metadata = transfer["metadata"]

        if len(transfer["received"]) != metadata["total_chunks"]:
            raise ValueError("Chưa nhận đủ các chunk")

        actual = build_metadata(transfer["temp_path"])

        if actual["size"] != metadata["size"]:
            raise ValueError("Kích thước file nhận không đúng")

        if actual["sha256"] != metadata["sha256"]:
            raise ValueError("Nội dung file nhận không đúng")

        if transfer["final_path"].exists():
            raise FileExistsError("File đích đã tồn tại")

        transfer["temp_path"].rename(transfer["final_path"])

        del self.transfers[transfer_id]

        return transfer["final_path"]
