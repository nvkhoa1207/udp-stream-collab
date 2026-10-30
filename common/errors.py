"""Phân biệt luồng đã đóng với lỗi mất kết nối ngoài dự kiến."""


class StreamClosedError(ConnectionError):
    pass


# Các lý do server dùng khi phải dừng file vì lỗi, khác với người dùng bấm Cancel.
FILE_FAILURE_REASONS = frozenset({
    'File relay failed', 'File relay expired', 'Relay queue quota reached',
    'Transfer peer disconnected', 'Transfer peer is offline',
})
