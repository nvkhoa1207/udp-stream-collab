"""Owner A, ngày 1. Chỉ encode/decode, không thực hiện mã hóa hay socket."""
import base64
import json
import re
import binascii
import asyncio

from common.config import (
    MAX_PAYLOAD_BYTES,
    MAX_PLAINTEXT_BYTES,
    MAX_TCP_RECORD_BYTES,
)
from common.models import MessageType, Packet


def _require_int(value: object, name: str, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError(
            f"{name} phải là số nguyên >= {minimum}"
        )
        

def _require_username(
    value: object,
    name: str,
    allow_broadcast: bool = False,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} phải là chuỗi")

    if allow_broadcast and value == "*":
        return

    if re.fullmatch(r"[A-Za-z0-9_-]{1,32}", value) is None:
        raise ValueError(
            f"{name} phải gồm 1–32 ký tự: chữ ASCII, số, _ hoặc -"
        )


def _require_hex_id(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} phải là chuỗi")

    if re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise ValueError(
            f"{name} phải gồm đúng 32 ký tự hex viết thường"
        )


def _validate_packet_fields(packet: Packet) -> None:
    _require_username(packet.sender, "sender")
    _require_username(
        packet.recipient,
        "recipient",
        allow_broadcast=True,
    )

    _require_hex_id(packet.message_id, "message_id")
    _require_hex_id(packet.session_id, "session_id")

    if not isinstance(packet.stream_id, str):
        raise ValueError("stream_id phải là chuỗi")

    if not 1 <= len(packet.stream_id) <= 64:
        raise ValueError("stream_id phải có từ 1 đến 64 ký tự")

    if not packet.stream_id.isprintable():
        raise ValueError("stream_id không được chứa ký tự điều khiển")
    
    
    _require_int(packet.version, "version", minimum=1)

    if packet.version != 1:
        raise ValueError("Chỉ hỗ trợ giao thức phiên bản 1")

    _require_int(packet.seq, "seq")

    unsequenced_types = {
        MessageType.BIND,
        MessageType.BIND_OK,
        MessageType.HEARTBEAT,
        MessageType.ACK,
        MessageType.WINDOW_UPDATE,
        MessageType.WINDOW_PROBE,
    }

    if packet.kind in unsequenced_types:
        if packet.seq != 0:
            raise ValueError("Loại packet này phải có seq = 0")
    elif packet.seq == 0:
        raise ValueError("Packet dữ liệu trên mạng phải có seq >= 1")

    if packet.kind == MessageType.ACK:
        _require_int(packet.ack, "ack", minimum=1)
    elif packet.ack is not None:
        raise ValueError("Chỉ ACK được chứa trường ack")

    window_types = {
        MessageType.ACK,
        MessageType.WINDOW_UPDATE,
    }

    if packet.kind in window_types:
        _require_int(packet.recv_limit, "recv_limit")
    elif packet.recv_limit is not None:
        raise ValueError(
            "Chỉ ACK/WINDOW_UPDATE được chứa recv_limit"
        )

    if packet.kind == MessageType.FILE_CHUNK:
        _require_int(packet.chunk_index, "chunk_index")
    elif packet.chunk_index is not None:
        raise ValueError("Chỉ FILE_CHUNK được chứa chunk_index")

    empty_payload_types = unsequenced_types | {
        MessageType.FILE_ACCEPT,
        MessageType.FILE_END,
        MessageType.LOGOUT,
    }

    if packet.kind in empty_payload_types and packet.payload != b"":
        raise ValueError("Loại packet này phải có payload rỗng")


def encode_packet(packet: Packet) -> bytes:
    # Measure the complete UTF-8 JSON body, including base64 and headers.
    if not isinstance(packet, Packet):
        raise TypeError("Đầu vào phải là Packet")

    if not isinstance(packet.kind, MessageType):
        raise ValueError("Loại thông điệp không hợp lệ")

    if not isinstance(packet.payload, bytes):
        raise TypeError("Payload phải là bytes")

    if len(packet.payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"Payload vượt giới hạn {MAX_PAYLOAD_BYTES} byte"
        )
        
    _validate_packet_fields(packet)

    record = {
        "version": packet.version,
        "kind": packet.kind.value,
        "session_id": packet.session_id,
        "message_id": packet.message_id,
        "sender": packet.sender,
        "recipient": packet.recipient,
        "stream_id": packet.stream_id,
        "seq": packet.seq,
        "ack": packet.ack,
        "chunk_index": packet.chunk_index,
        "recv_limit": packet.recv_limit,
        "payload": base64.b64encode(packet.payload).decode("ascii"),
    }

    data = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(data) > MAX_PLAINTEXT_BYTES:
        raise ValueError(
            f"Packet JSON vượt giới hạn {MAX_PLAINTEXT_BYTES} byte"
        )

    return data


def _unique_json_object(pairs: list) -> dict:
    result = {}

    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON có trường trùng: {key}")

        result[key] = value

    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"JSON chứa giá trị số không hợp lệ: {value}")


def decode_packet(data: bytes) -> Packet:
    # Strict wire validation runs before any socket/session/application state changes.
    if not isinstance(data, bytes):
        raise TypeError("Dữ liệu nhận phải là bytes")

    if not data or len(data) > MAX_PLAINTEXT_BYTES:
        raise ValueError("Kích thước packet không hợp lệ")

    try:
        record = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("Packet không phải JSON UTF-8 hợp lệ") from exc

    if not isinstance(record, dict):
        raise ValueError("Packet JSON phải là object")

    expected_fields = {
        "version",
        "kind",
        "session_id",
        "message_id",
        "sender",
        "recipient",
        "stream_id",
        "seq",
        "ack",
        "chunk_index",
        "recv_limit",
        "payload",
    }

    if set(record) != expected_fields:
        raise ValueError("Packet thiếu trường hoặc chứa trường không hỗ trợ")

    if not isinstance(record["kind"], str):
        raise ValueError("kind phải là chuỗi")

    try:
        kind = MessageType(record["kind"])
    except ValueError as exc:
        raise ValueError("Loại thông điệp không được hỗ trợ") from exc

    if not isinstance(record["payload"], str):
        raise ValueError("Payload trên wire phải là chuỗi Base64")

    try:
        payload = base64.b64decode(
            record["payload"],
            validate=True,
        )
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Payload Base64 không hợp lệ") from exc

    packet = Packet(
        version=record["version"],
        kind=kind,
        session_id=record["session_id"],
        message_id=record["message_id"],
        sender=record["sender"],
        recipient=record["recipient"],
        stream_id=record["stream_id"],
        seq=record["seq"],
        ack=record["ack"],
        chunk_index=record["chunk_index"],
        recv_limit=record["recv_limit"],
        payload=payload,
    )

    if len(packet.payload) > MAX_PAYLOAD_BYTES:
        raise ValueError("Payload vượt giới hạn cho phép")

    _validate_packet_fields(packet)

    return packet


def encode_tcp_record(record: dict) -> bytes:
    # Length is the encoded byte length, never a Unicode character count.
    if not isinstance(record, dict):
        raise TypeError("Record TCP phải là dictionary")

    body = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(body) > MAX_TCP_RECORD_BYTES:
        raise ValueError("Record TCP vượt giới hạn kích thước")

    prefix = len(body).to_bytes(4, byteorder="big")

    return prefix + body


async def read_tcp_record(
    reader: asyncio.StreamReader,
    timeout: float = 10.0,
) -> dict:
    async def read_one() -> dict:
        prefix = await reader.readexactly(4)
        body_size = int.from_bytes(prefix, byteorder="big")

        if not 1 <= body_size <= MAX_TCP_RECORD_BYTES:
            raise ValueError("Độ dài record TCP không hợp lệ")

        body = await reader.readexactly(body_size)

        try:
            record = json.loads(
                body.decode("utf-8"),
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            RecursionError,
        ) as exc:
            raise ValueError("Record TCP không phải JSON UTF-8 hợp lệ") from exc

        if not isinstance(record, dict):
            raise ValueError("Record TCP phải chứa JSON object")

        return record

    return await asyncio.wait_for(read_one(), timeout=timeout)
