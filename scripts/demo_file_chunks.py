from pathlib import Path

from client.files import build_metadata, iter_chunks


def main():
    # Tạo thư mục chứa dữ liệu thử.
    data_folder = Path("data")
    data_folder.mkdir(exist_ok=True)

    original = data_folder / "demo_original.bin"
    copied = data_folder / "demo_copy.bin"

    # Tạo file mẫu gồm 1.000 byte.
    original.write_bytes(b"A" * 1000)

    # Đọc thông tin file gốc.
    metadata = build_metadata(original)

    print("Tên file:", metadata["name"])
    print("Kích thước:", metadata["size"], "byte")
    print("Số chunk:", metadata["total_chunks"])

    # Đọc từng chunk từ file gốc và ghi vào file mới.
    with copied.open("wb") as file:
        for index, chunk in enumerate(
            iter_chunks(original), start=1
        ):
            print(f"Chunk {index}: {len(chunk)} byte")
            file.write(chunk)

    # Kiểm tra nội dung sau khi ghép.
    copied_metadata = build_metadata(copied)

    same_hash = (
        metadata["sha256"] == copied_metadata["sha256"]
    )

    print("Hash giống nhau:", same_hash)
    assert same_hash and copied.read_bytes() == original.read_bytes()


if __name__ == "__main__":
    main()
