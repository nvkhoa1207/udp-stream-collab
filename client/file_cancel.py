"""Schema CANCEL dùng chung ở phía gửi và phía nhận."""

import json
import re

from common.config import MAX_PAYLOAD_BYTES
from common.models import MessageType


class FileTransferCancelled(RuntimeError):
    pass


def cancelled_transfer_id(packet):
    if (packet.kind != MessageType.FILE_CANCEL or packet.stream_id != "control"
            or packet.chunk_index is not None or not isinstance(packet.payload, bytes)
            or len(packet.payload) > MAX_PAYLOAD_BYTES):
        return None
    try:
        body = json.loads(packet.payload.decode("utf-8"))
    except (ValueError, UnicodeError, RecursionError):
        return None
    if (type(body) is not dict or set(body) != {"transfer_id", "reason"}
            or type(body["transfer_id"]) is not str
            or re.fullmatch(r"[0-9a-f]{32}", body["transfer_id"]) is None
            or type(body["reason"]) is not str or not 1 <= len(body["reason"]) <= 128):
        return None
    return body["transfer_id"]
