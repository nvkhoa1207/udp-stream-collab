"""AES-GCM từ thư viện cryptography; không tự viết thuật toán mã hóa."""
import argparse
import os
from pathlib import Path
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class PayloadCipher:
    def __init__(self, key: bytes) -> None:
        self._aes = AESGCM(key)

    def seal(self, plaintext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        if len(nonce) != 12:
            raise ValueError('AES-GCM nonce must contain 12 bytes')
        return self._aes.encrypt(nonce, plaintext, associated_data)

    def open(self, ciphertext: bytes, nonce: bytes, associated_data: bytes) -> bytes:
        if len(nonce) != 12:
            raise ValueError('AES-GCM nonce must contain 12 bytes')
        try:
            return self._aes.decrypt(nonce, ciphertext, associated_data)
        except InvalidTag as exc:
            raise ValueError('Encrypted record authentication failed') from exc


def generate_key(path: Path) -> None:
    # Không ghi đè khóa đang dùng; trên Unix chỉ chủ sở hữu đọc/ghi được.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as output:
        output.write(AESGCM.generate_key(bit_length=256))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a shared AES-256 key; never commit it.')
    parser.add_argument('--generate-key', type=Path, required=True)
    args = parser.parse_args()
    try:
        generate_key(args.generate_key)
    except OSError as exc:
        parser.exit(1, f'Could not create key: {exc}\n')
    print('Key created. Share this file privately with the server and clients.')
