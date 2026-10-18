# Bạn A — server và truyền dữ liệu

Owner: `server/`, `transport/`; điều phối `common/` sau khi hai người đồng ý.
Không sửa file Khoa đang làm nếu chưa trao đổi. API ở `docs/02_API.md`.

| ID | Ngày | Việc cần code / file | Xong khi |
|---|---|---|---|
| A1 | 1 | `common/codec.py`: encode/decode; TCP length prefix | Gói unicode/binary đi-về đúng; loại gói sai/quá lớn bị từ chối |
| A2 | 1 | `server/main.py`, `server/app.py`: TCP handshake, async lifecycle | Client thật kết nối nhận phản hồi; shutdown đóng socket/task |
| A3 | 2 | `server/auth.py`, `server/sessions.py`: auth, session và địa chỉ | Đăng nhập đúng/sai rõ ràng; username không bị chiếm/ghi đè; khóa phiên bảo vệ |
| A4 | 2 | `server/sessions.py`: heartbeat, online/offline | Mất heartbeat quá hạn bị loại; người khác nhận danh sách mới |
| A5 | 2 | `server/router.py`, `transport/reliable_udp.py`: UDP và định tuyến | Chat chung/riêng đúng; client không giả được sender/session |
| A6 | 3 | `transport/reliable_udp.py`: ACK, sequence, retry, sliding window | Mất DATA/ACK, gói trùng/sai thứ tự không làm hỏng nội dung; buffer có giới hạn |
| A7 | 4 | `transport/congestion.py`, tích hợp và stress test | Cơ chế congestion theo yêu cầu đã xác nhận; 10–20 client; luồng chat không đợi file lỗi |
| A8 | 5 | Báo cáo kiến trúc/giao thức + README server | Một người khác chạy được bằng hướng dẫn |

## Checklist không bỏ sót

- [ ] Dùng `Transport` chung, không viết transport riêng cho server và client.
- [ ] Số thứ tự theo phiên/chặng/stream, tránh một hàng đợi chặn toàn bộ luồng.
- [ ] ACK xác nhận đã nhận/giữ dữ liệu an toàn; không ACK dữ liệu bị loại vì buffer đầy.
- [ ] Gửi lại chỉ phần thiếu; ACK thất lạc vẫn có thể khôi phục; gói trùng được ACK lại.
- [ ] Cửa sổ đang bay chịu cả giới hạn bên nhận và giới hạn congestion.
- [ ] Việc đợi ACK của một đích không khóa vòng nhận hay chat của đích khác.
- [ ] Server dùng danh tính trong session đã xác thực, không tin `sender` tự khai.
- [ ] UDP endpoint chỉ gắn với session sau bước bind được xác thực.
- [ ] ACK client gửi tới server chỉ là chặng đầu; báo lỗi nếu chuyển tiếp tới đích thất bại.
- [ ] Phối hợp Khoa: mã hóa envelope cả header/payload; phân biệt khóa hai chiều/nonce.
- [ ] Timer retry, timeout, buffer, task, session đều được dọn khi đóng/mất kết nối.
- [ ] Không ghi mật khẩu/khóa vào log; chỉ tài khoản demo được tạo cục bộ.

## Đầu vào từ Khoa

`PayloadCipher` dùng chung, client handshake đúng schema, metadata/file completion
theo protocol. Có thể thử router trước bằng `MemoryTransport` và tài khoản fixture.
