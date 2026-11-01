"""Bọc bản tin bằng AES-GCM với khóa chia sẻ trước ngoài ứng dụng.

Envelope: BTL1 (4 byte) | nonce ngẫu nhiên (12) | ciphertext + tag (16).
UDP và TCP dùng AAD khác nhau, không chấp nhận gói rõ hoặc hạ cấp mã hóa.
"""
import os
from functools import lru_cache
from pathlib import Path
from security.cipher import PayloadCipher

MAGIC = b'BTL1'
OVERHEAD = 4 + 12 + 16


@lru_cache(maxsize=4)
def _load_cipher(path: str) -> PayloadCipher:
    try:
        key = Path(path).read_bytes()
    except OSError as exc:
        raise ValueError(
            'Cannot read encryption key. Create shared.key with '
            'python -m security.cipher --generate-key shared.key; '
            'all peers must use the same file (or set BTL_KEY_FILE).'
        ) from exc
    if len(key) != 32:
        raise ValueError('Shared key file must contain exactly 32 binary bytes')
    return PayloadCipher(key)


def get_cipher() -> PayloadCipher:
    return _load_cipher(os.path.abspath(os.environ.get('BTL_KEY_FILE', 'shared.key')))


def seal_record(plaintext: bytes, channel: bytes) -> bytes:
    # Nonce mới cho mỗi lần mã hóa, kể cả retry; không dùng seq vì seq
    # có thể lặp giữa các stream/client/hướng truyền.
    nonce = os.urandom(12)
    return MAGIC + nonce + get_cipher().seal(plaintext, nonce, MAGIC + channel)


def open_record(data: bytes, channel: bytes) -> bytes:
    if len(data) < OVERHEAD or not data.startswith(MAGIC):
        raise ValueError('Encrypted BTL1 record required')
    return get_cipher().open(data[16:], data[4:16], MAGIC + channel)
