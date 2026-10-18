# API để code song song

Nguồn chuẩn: `common/contracts.py` và `common/models.py`. Các function trong
starter có `NotImplementedError` là chỗ bạn tự làm, không phải tính năng đã có.

## Transport chung

| API | Hợp đồng |
|---|---|
| `await start()` | Bắt đầu nhận dữ liệu; không gọi send trước start |
| `await send(peer, packet)` | Truyền tới **chặng kế tiếp**; real transport đợi ACK nếu loại gói yêu cầu; lỗi raise rõ ràng |
| `await receive()` | Chờ một `ReceivedPacket(peer, packet)` đã hợp lệ và dedup/order theo stream |
| `await close()` | Dừng nhận/gửi, giải phóng tài nguyên; người dùng hủy task receive nền |

`peer` là `(host, port)` của socket kế tiếp. Client luôn gửi đến server; server
gửi đến đích đã tra trong session. `packet.recipient` là đích nghiệp vụ, không
phải địa chỉ IP. Packet được định nghĩa frozen để retransmit không sửa dữ liệu
đang chờ ACK; transport có thể dùng `dataclasses.replace` khi gán seq/session.

`ReliableUdpTransport.register_peer(...)` được A thêm cho phiên/khóa sau auth;
đây là cấu hình transport thật, không phải API chat/file. Mock không mô phỏng
auth/keys. Không làm mạng thật bằng mock rồi coi đã đạt yêu cầu bảo mật.

## Client và UI

- `ClientService` nhận một Transport qua constructor.
- `send_chat(recipient, text)` tạo Packet rồi gọi transport; `receive_events()`
  hoặc queue sự kiện chuyển kết quả cho UI. Giao diện không gọi socket trực tiếp.
- Event đề xuất: `{type: "message"|"users"|"progress"|"status"|"error", ...}`.
- `send_file(recipient, path)` điều phối FILE_META/ACCEPT/CHUNK/END/COMPLETE.
- Vòng nhận chạy nền và dispatch theo loại gói/transfer; không để nhiều coroutine
  cùng tranh `receive()` rồi lấy mất ACK ứng dụng của nhau.

## Thay mock bằng mạng thật

Ngày 1–2, Khoa dùng `MemoryHub` và `MemoryTransport`, tham khảo demo. Đến ngày 3,
A cung cấp ReliableUdpTransport và cấu hình phiên, Khoa truyền vào ClientService.
Mock chỉ đưa Packet nguyên vẹn vào queue; chưa có mất gói, mã hóa, socket, ACK,
window, lỗi mạng hay đảm bảo thứ tự của UDP thật.

## Quy ước phối hợp

1. A không đổi tên field/hàm mà không cập nhật docs và báo Khoa.
2. Khoa không tự sửa protocol để UI chạy tạm; đề xuất thay đổi trước khi merge.
3. Thay đổi breaking đi cùng một commit cập nhật caller và tài liệu.
4. Không chia code theo frontend/backend đơn thuần: Khoa có nghiệp vụ client/file/
   mật mã; A có server và transport; cùng chịu trách nhiệm luồng đầu-cuối.
