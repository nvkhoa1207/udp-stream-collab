"""Owner Khoa. Fail-closed skeleton: không bao giờ trả plaintext thay ciphertext."""


class PayloadCipher:
    def __init__(self, key: bytes) -> None:
        if len(key) not in (16, 24, 32):
            raise ValueError("Khóa AES phải có 16, 24 hoặc 32 byte")
        self._key = key

    def seal(self, plaintext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # TODO[K5]: AESGCM.encrypt; nonce duy nhất cho từng key; ràng buộc header ngoài.
        # A quản lý counter/lifetime; hai người chốt envelope trước khi code riêng.
        raise NotImplementedError("TODO[K5]: PayloadCipher.seal")

    def open(self, ciphertext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        # TODO[K5]: AESGCM.decrypt; InvalidTag phải được báo/loại, không bypass.
        raise NotImplementedError("TODO[K5]: PayloadCipher.open")
