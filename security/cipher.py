"""Mở rộng AES-GCM tùy chọn, nằm ngoài bản đầu và không được core gọi.

Giữ stub fail-closed: không bao giờ trả plaintext thay ciphertext.
K5 hiện là kiểm tra an toàn đầu vào/file/bridge, không bắt buộc triển khai cipher.
"""


class PayloadCipher:
    def __init__(self, key: bytes) -> None:
        if len(key) not in (16, 24, 32):
            raise ValueError("Khóa AES phải có 16, 24 hoặc 32 byte")
        self._key = key

    def seal(self, plaintext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # OPTIONAL: AESGCM.encrypt; nonce duy nhất cho từng key; ràng buộc header ngoài.
        # A quản lý counter/lifetime; hai người chốt envelope trước khi code riêng.
        raise NotImplementedError("Optional AES-GCM: PayloadCipher.seal")

    def open(self, ciphertext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # OPTIONAL: AESGCM.decrypt; InvalidTag phải được báo/loại, không bypass.
        raise NotImplementedError("Optional AES-GCM: PayloadCipher.open")
