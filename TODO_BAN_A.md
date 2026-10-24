# Hải (Bạn A) — server và truyền dữ liệu

Owner: `server/`, `transport/`; điều phối `common/` sau khi hai người đồng ý.
API chung ở `docs/02_API.md`, phạm vi theo README. Bản đầu chưa mã hóa;
CUBIC giản lược và bridge localhost + polling đã được chọn. Các mục dưới đây
vẫn chưa hoàn thành trên mạng thật; cập nhật ngày 23/10/2026.

| ID | Ngày | Việc cần code / file | Xong khi |
|---|---|---|---|
| A1 | 1 | `common/codec.py`: encode/decode; TCP length prefix | Gói unicode/binary đi-về đúng; loại gói sai/quá lớn bị từ chối |
| A2 | 1 | `server/main.py`, `server/app.py`: TCP handshake, async lifecycle | Client thật kết nối nhận phản hồi; shutdown đóng socket/task |
| A3 | 2 | `server/auth.py`, `server/sessions.py`: auth, session và địa chỉ | Đăng nhập đúng/sai rõ ràng; username không bị chiếm/ghi đè; session/BIND gắn đúng endpoint, không cần khóa mã hóa |
| A4 | 2 | `server/sessions.py`: heartbeat, online/offline | Mất heartbeat quá hạn bị loại; người khác nhận danh sách mới |
| A5 | 2 | `server/router.py`, `transport/reliable_udp.py`: UDP và định tuyến | Chat chung/riêng đúng; kiểm tra phiên/endpoint; lấy sender từ phiên, ghi rõ giới hạn chưa có xác thực mật mã |
| A6 | 3 | `transport/reliable_udp.py`: ACK, sequence, retry, sliding window, recv_limit và probe | Mất DATA/ACK, gói trùng/sai thứ tự không làm hỏng nội dung; buffer có giới hạn; mất thông báo mở cửa sổ vẫn phục hồi |
| A7 | 4 | `transport/congestion.py`, tích hợp và stress test | CUBIC giản lược có tăng bậc ba, giảm loss/timeout và số liệu; 10–20 client; luồng chat không đợi file lỗi |
| A8 | 5 | Báo cáo kiến trúc/giao thức + README server | Một người khác chạy được bằng hướng dẫn |

## Checklist không bỏ sót

- [ ] Dùng `Transport` chung, không viết transport riêng cho server và client.
- [ ] Số thứ tự theo phiên/chặng/stream, tránh một hàng đợi chặn toàn bộ luồng.
- [ ] ACK xác nhận đã nhận/giữ dữ liệu an toàn; không ACK dữ liệu bị loại vì buffer đầy.
- [ ] Gửi lại chỉ phần thiếu; ACK thất lạc vẫn có thể khôi phục; gói trùng được ACK lại.
- [ ] Cửa sổ đang bay chịu cả giới hạn bên nhận và giới hạn congestion.
- [ ] Việc đợi ACK của một đích không khóa vòng nhận hay chat của đích khác.
- [ ] Server dùng danh tính trong session đã xác thực, không tin `sender` tự khai.
- [ ] BIND kiểm tra phiên AUTH đang chờ, IP nguồn và endpoint; không tự đổi endpoint.
- [ ] `register_peer(peer, session_id)` không yêu cầu khóa; không gọi cipher stub.
- [ ] ACK/UPDATE mang recv_limit theo packet; giới hạn tăng dần, không cộng credit từ ACK trùng.
- [ ] Stream mới xin window bằng probe; mất UPDATE mở cửa sổ vẫn hỏi lại được, có timeout.
- [ ] Dành buffer trước khi cấp credit; tuân giới hạn tổng stream/bộ nhớ.
- [ ] ACTIVE/DRAINING → CLOSED trả slot buffer; dấu CLOSED tách riêng, cache
      kết quả 120s, giữ ID đến hết phiên (quota 4096); thử >128 file nối tiếp.
- [ ] ACK client gửi tới server chỉ là chặng đầu; báo lỗi nếu chuyển tiếp tới đích thất bại.
- [ ] Phối hợp Khoa: băm mật khẩu lưu trữ, kiểm tra session/đầu vào và giới hạn tài nguyên.
- [ ] Ghi rõ TCP AUTH và UDP chưa được bảo vệ bằng mật mã; không nhận là chống nghe lén/giả mạo chủ động.
- [ ] Timer retry, timeout, buffer, task, session đều được dọn khi đóng/mất kết nối.
- [ ] Không ghi mật khẩu/khóa vào log; chỉ tài khoản demo được tạo cục bộ.

## Đầu vào từ Khoa

Client handshake đúng schema AUTH_OK(session_id, udp_port), metadata/file
completion theo protocol, FILE_ACCEPT do chương trình tự gửi. Không phụ thuộc
PayloadCipher trong bản đầu. Có thể thử router trước bằng MemoryTransport và
tài khoản fixture; vẫn phải kiểm thử mạng thật khi ghép.
