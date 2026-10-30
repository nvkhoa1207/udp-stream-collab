"""Các kiểu gói tin dùng chung cho client, server và UDP."""

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
    WINDOW_UPDATE = "WINDOW_UPDATE"
    WINDOW_PROBE = "WINDOW_PROBE"
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
    session_id: str = ""  # Được gán sau đăng nhập; bản mock không xác thực.
    seq: int = 0  # Gói dữ liệu được đánh số từ 1; gói điều khiển dùng 0.
    ack: int | None = None
    chunk_index: int | None = None
    version: int = 1
    # ACK/WINDOW_UPDATE báo số thứ tự cao nhất mà bên kia được phép gửi.
    # Giới hạn chỉ tăng lên; None nghĩa là gói này không báo cửa sổ nhận.
    recv_limit: int | None = None


@dataclass(frozen=True)
class ReceivedPacket:
    peer: Peer  # Địa chỉ máy gửi trực tiếp; có thể là server chuyển tiếp.
    packet: Packet
