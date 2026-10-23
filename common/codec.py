"""Owner A, ngày 1. Chỉ encode/decode, không thực hiện mã hóa hay socket."""

from common.models import Packet


def encode_packet(packet: Packet) -> bytes:
    # TODO[A1]: JSON UTF-8; kind.value; payload base64; validate field/size.
    # Đo số byte thực tế theo docs/01_PROTOCOL.md; bản đầu không có envelope mật mã.
    raise NotImplementedError("TODO[A1]: encode_packet")


def decode_packet(data: bytes) -> Packet:
    # TODO[A1]: giới hạn input, parse JSON, strict type/version/length/range.
    # Base64 sai, kind lạ, số âm, packet quá lớn -> ValueError.
    # recv_limit bắt buộc trên ACK/WINDOW_UPDATE; bool không phải số nguyên hợp lệ.
    raise NotImplementedError("TODO[A1]: decode_packet")


def encode_tcp_record(record: dict) -> bytes:
    # TODO[A1]: 4 byte big-endian length + JSON, kiểm tra MAX_TCP_RECORD_BYTES.
    raise NotImplementedError("TODO[A1]: encode_tcp_record")
