"""Giá trị khởi đầu do nhóm đề xuất, cần đo/điều chỉnh khi triển khai."""

TCP_PORT = 9000
UDP_PORT = 9001
MAX_DATAGRAM_BYTES = 1200
MAX_PLAINTEXT_BYTES = 1100  # Trần body JSON; giữ tên để tương thích, không yêu cầu cipher.
MAX_PAYLOAD_BYTES = 480
MAX_TCP_RECORD_BYTES = 64 * 1024
HEARTBEAT_SECONDS = 3.0
OFFLINE_TIMEOUT_SECONDS = 10.0
INITIAL_RETRY_SECONDS = 0.5
MAX_RETRIES = 8
# Cửa sổ congestion ban đầu; fixed window riêng lẻ chưa phải CUBIC.
INITIAL_WINDOW_PACKETS = 8
RECEIVE_WINDOW_PACKETS = 8  # Số slot giữ packet của mỗi stream được cấp nhận.
WINDOW_PROBE_SECONDS = 1.0
WINDOW_STALL_TIMEOUT_SECONDS = 10.0
MAX_RECEIVE_STREAMS = 128  # Tổng stream được cấp buffer trên một transport.
