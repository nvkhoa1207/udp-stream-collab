"""Hợp đồng dữ liệu nháp. Hai người thống nhất trước khi thay đổi."""

from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

Peer = tuple[str, int]


class MessageType(str, Enum):
    BIND = "BIND"
    BIND_OK = "BIND_OK"
    HEARTBEAT = "HEARTBEAT"
    USER_LIST = "USER_LIST"
    MSG = "MSG"
    FILE_META = "FILE_META"
    FILE_ACCEPT = "FILE_ACCEPT"
    FILE_CHUNK = "FILE_CHUNK"
    FILE_END = "FILE_END"
    FILE_COMPLETE = "FILE_COMPLETE"
    FILE_CANCEL = "FILE_CANCEL"
    ACK = "ACK"
    ERROR = "ERROR"
    LOGOUT = "LOGOUT"


@dataclass(frozen=True)
class Packet:
    kind: MessageType
    sender: str
    recipient: str
    stream_id: str
    payload: bytes = b""
    message_id: str = field(default_factory=lambda: uuid4().hex)
    session_id: str = ""  # Real transport gán sau auth; mock không xác thực.
    seq: int = 0  # 0 ở lớp ứng dụng; real transport gán sequence >= 1.
    ack: int | None = None
    chunk_index: int | None = None
    version: int = 1


@dataclass(frozen=True)
class ReceivedPacket:
    peer: Peer  # Địa chỉ chặng gửi trực tiếp, không phải sender nghiệp vụ.
    packet: Packet
