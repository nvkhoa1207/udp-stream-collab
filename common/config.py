"""Cổng mạng và các giới hạn đang dùng trong chương trình."""

TCP_PORT = 9000
UDP_PORT = 9001
MAX_DATAGRAM_BYTES = 1200
MAX_PLAINTEXT_BYTES = 1100  # Số byte tối đa của JSON; giữ tên cũ để các module vẫn dùng được.
MAX_PAYLOAD_BYTES = 480
MAX_TCP_RECORD_BYTES = 64 * 1024
HEARTBEAT_SECONDS = 3.0
OFFLINE_TIMEOUT_SECONDS = 10.0
INITIAL_RETRY_SECONDS = 0.5
MAX_RETRIES = 8
# Cửa sổ gửi ban đầu; CUBIC sẽ điều chỉnh trong lúc truyền.
INITIAL_WINDOW_PACKETS = 8
RECEIVE_WINDOW_PACKETS = 8  # Mỗi luồng nhận giữ tối đa 8 gói tin.
WINDOW_PROBE_SECONDS = 1.0
WINDOW_STALL_TIMEOUT_SECONDS = 10.0
MAX_RECEIVE_STREAMS = 128  # Số luồng nhận tối đa trên một kết nối UDP.


def configure_console() -> None:
    """Dùng UTF-8 để terminal Windows in được tên file và dữ liệu Unicode."""
    import sys
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            stream.reconfigure(encoding='utf-8', errors='backslashreplace')
