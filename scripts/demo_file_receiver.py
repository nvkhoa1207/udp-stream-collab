from pathlib import Path
from uuid import uuid4

from client.files import FileReceiver, build_metadata, iter_chunks


def main():
    data_folder = Path("data")
    data_folder.mkdir(exist_ok=True)

    original = data_folder / "demo_original.bin"

    # Mỗi chunk có nội dung khác nhau để kiểm tra thứ tự.
    content = b"A" * 480 + b"B" * 480 + b"C" * 40
    original.write_bytes(content)

    metadata = build_metadata(original)

    # Demo nhỏ nên đưa các chunk vào list để chọn thứ tự gửi.
    chunks = list(iter_chunks(original))

    receiver = FileReceiver(Path("data/received"))

    # Tạo ID mới mỗi lần chạy để tránh trùng tên file.
    transfer_id = uuid4().hex

    receiver.begin(transfer_id, metadata)

    # Đến sai thứ tự và chunk 0 xuất hiện hai lần.
    for index in [2, 0, 0, 1]:
        receiver.accept_chunk(
            transfer_id,
            index,
            chunks[index]
        )

        print(f"Delivered chunk {index} into FileReceiver")

    received_count = len(
        receiver.transfers[transfer_id]["received"]
    )

    print("Unique chunks received:", received_count)
    assert received_count == metadata["total_chunks"]

    saved_path = receiver.finish(transfer_id)

    saved_metadata = build_metadata(saved_path)

    same_hash = (
        metadata["sha256"] == saved_metadata["sha256"]
    )

    print("Hashes match:", same_hash)
    print("Saved file:", saved_path)
    assert same_hash and saved_path.read_bytes() == content
    assert transfer_id not in receiver.transfers
    assert not (receiver.output_directory / f"{transfer_id}.part").exists()


if __name__ == "__main__":
    main()
