"""Chỗ dành cho AES-GCM nếu nhóm bổ sung mã hóa sau này.

Ứng dụng hiện không gọi lớp này. Hàm chưa làm thì báo lỗi,
không trả dữ liệu chưa mã hóa rồi xem như đã mã hóa.
"""


class PayloadCipher:
    def __init__(self, key: bytes) -> None:
        if len(key) not in (16, 24, 32):
            raise ValueError("AES key must contain 16, 24 or 32 bytes")
        self._key = key

    def seal(self, plaintext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # Nếu bổ sung mã hóa, mỗi khóa phải dùng nonce khác nhau cho từng gói.
        raise NotImplementedError("Optional AES-GCM: PayloadCipher.seal")

    def open(self, ciphertext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # Nếu kiểm tra mã xác thực thất bại thì phải bỏ gói.
        raise NotImplementedError("Optional AES-GCM: PayloadCipher.open")
