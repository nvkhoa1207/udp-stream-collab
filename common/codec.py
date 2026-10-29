"""Đổi gói tin sang bytes và kiểm tra dữ liệu nhận từ mạng."""
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
            f"{name} must be an integer >= {minimum}"
        )
        

def _require_username(
    value: object,
    name: str,
    allow_broadcast: bool = False,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")

    if allow_broadcast and value == "*":
        return

    if re.fullmatch(r"[A-Za-z0-9_-]{1,32}", value) is None:
        raise ValueError(
            f"{name} must contain 1 to 32 ASCII letters, digits, _ or -"
        )


def _require_hex_id(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")

    if re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise ValueError(
            f"{name} must contain exactly 32 lowercase hexadecimal characters"
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
        raise ValueError("stream_id must be a string")

    if not 1 <= len(packet.stream_id) <= 64:
        raise ValueError("stream_id must contain 1 to 64 characters")

    if not packet.stream_id.isprintable():
        raise ValueError("stream_id must not contain control characters")
    
    
    _require_int(packet.version, "version", minimum=1)

    if packet.version != 1:
        raise ValueError("Only protocol version 1 is supported")

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
            raise ValueError("This packet type must have seq = 0")
    elif packet.seq == 0:
        raise ValueError("Data packets on the network must have seq >= 1")

    if packet.kind == MessageType.ACK:
        _require_int(packet.ack, "ack", minimum=1)
    elif packet.ack is not None:
        raise ValueError("Only ACK can contain the ack field")

    window_types = {
        MessageType.ACK,
        MessageType.WINDOW_UPDATE,
    }

    if packet.kind in window_types:
        _require_int(packet.recv_limit, "recv_limit")
    elif packet.recv_limit is not None:
        raise ValueError(
            "Only ACK/WINDOW_UPDATE can contain recv_limit"
        )

    if packet.kind == MessageType.FILE_CHUNK:
        _require_int(packet.chunk_index, "chunk_index")
    elif packet.chunk_index is not None:
        raise ValueError("Only FILE_CHUNK can contain chunk_index")

    empty_payload_types = unsequenced_types | {
        MessageType.FILE_ACCEPT,
        MessageType.FILE_END,
        MessageType.LOGOUT,
    }

    if packet.kind in empty_payload_types and packet.payload != b"":
        raise ValueError("This packet type must have an empty payload")


def encode_packet(packet: Packet) -> bytes:
    # Tính cả phần header và Base64 khi kiểm tra kích thước gói tin.
    if not isinstance(packet, Packet):
        raise TypeError("Input must be a Packet")

    if not isinstance(packet.kind, MessageType):
        raise ValueError("Invalid message type")

    if not isinstance(packet.payload, bytes):
        raise TypeError("Payload must be bytes")

    if len(packet.payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"Payload exceeds the limit of {MAX_PAYLOAD_BYTES} byte"
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
            f"Packet JSON exceeds the limit of {MAX_PLAINTEXT_BYTES} byte"
        )

    return data


def _unique_json_object(pairs: list) -> dict:
    result = {}

    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")

        result[key] = value

    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON numeric value: {value}")


def decode_packet(data: bytes) -> Packet:
    # Kiểm tra gói tin trước khi đưa vào xử lý.
    if not isinstance(data, bytes):
        raise TypeError("Received data must be bytes")

    if not data or len(data) > MAX_PLAINTEXT_BYTES:
        raise ValueError("Invalid packet size")

    try:
        record = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("Packet is not valid UTF-8 JSON") from exc

    if not isinstance(record, dict):
        raise ValueError("Packet JSON must be an object")

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
        raise ValueError("Packet has missing or unsupported fields")

    if not isinstance(record["kind"], str):
        raise ValueError("kind must be a string")

    try:
        kind = MessageType(record["kind"])
    except ValueError as exc:
        raise ValueError("Unsupported message type") from exc

    if not isinstance(record["payload"], str):
        raise ValueError("Network payload must be a Base64 string")

    try:
        payload = base64.b64decode(
            record["payload"],
            validate=True,
        )
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Invalid Base64 payload") from exc

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
        raise ValueError("Payload exceeds the allowed limit")

    _validate_packet_fields(packet)

    return packet


def encode_tcp_record(record: dict) -> bytes:
    # Độ dài tính theo byte UTF-8, không tính theo số ký tự.
    if not isinstance(record, dict):
        raise TypeError("TCP record must be a dictionary")

    body = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(body) > MAX_TCP_RECORD_BYTES:
        raise ValueError("TCP record exceeds the size limit")

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
            raise ValueError("Invalid TCP record length")

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
            raise ValueError("TCP record is not valid UTF-8 JSON") from exc

        if not isinstance(record, dict):
            raise ValueError("TCP record must contain a JSON object")

        return record

    return await asyncio.wait_for(read_one(), timeout=timeout)
