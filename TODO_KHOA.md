# Khoa — client, file và giao diện

Owner: `client/`, `ui/`; phối hợp kiểm tra an toàn đầu vào/file/phiên.
`security/cipher.py` là mở rộng tùy chọn ngoài bản đầu. Dùng `common/` và API đã
cập nhật ngày 23/10/2026; thay đổi field phải cập nhật protocol/caller cùng lúc.
Mở `scripts/demo_mock.py` trước: đây là cách làm song song mà chưa cần server thật.

| ID | Ngày | Việc cần code / file | Xong khi |
|---|---|---|---|
| K1 | 1 | `client/main.py`, `client/service.py`: connect/login và event | Client nhận handshake thật; lỗi kết nối hiển thị rõ; import không tự mở socket |
| K2 | 2 | `client/service.py`: chat và danh sách người dùng | Nhập/gửi/nhận cùng lúc; chọn đúng recipient; thử trước với mock |
| K3 | 2–3 | `client/files.py`: metadata, chunk, reassembly, hash | File rỗng, nhị phân, nhiều chunk đều đúng byte; không đọc cả file lớn vào RAM |
| K4 | 3 | `client/service.py`, `client/files.py`: file đầu-cuối | Tiến độ từ byte đã ACK; chỉ hoàn tất khi nhận FILE_COMPLETE với hash đúng |
| K5 | 3–4 | Kiểm tra đầu vào client/file/bridge; phối hợp session/BIND với Hải | Tên file/path an toàn; giới hạn tài nguyên; không log password/session; ghi đúng giới hạn chưa mã hóa |
| K6 | 2–4 | `ui/`: fixture ngày 2, HTTP bridge localhost + polling đã chọn | Hiện online, chat, progress và lỗi; không dùng WebSocket/framework bị cấm |
| K7 | 4 | Kiểm thử client/file/mất gói cùng A | Có kết quả thật, hash, log, ảnh; UI không đứng khi file đang gửi |
| K8 | 5 | Báo cáo luồng chat/file, UI, kiểm thử + README client | Người khác chạy được client; mô tả đúng cái đã làm |

## Trạng thái local

Đã có send_chat, event MSG, chia file và FileReceiver cơ bản; hai demo client
chat/file receiver đã có trong cây nguồn. K1, K4, K6 còn TODO; K2/K3/K5 mới có
một phần, chưa tick hoàn tất theo kết quả mock. Xem [tiến độ](docs/06_TIEN_DO.md).

## Thứ tự bắt đầu

1. Chạy `python -m scripts.demo_mock` và đọc `common/contracts.py`.
2. Dùng `ClientService(transport, server_peer, username)`, hoàn thiện vòng nhận
   nền và event queue cho user/file/error bên cạnh MSG đã có.
3. Dùng fixture online/chat/file-progress để làm UI khi A chưa có auth/router.
4. Viết chia/ghép file bằng `BytesIO`/file tạm trước; sau đó thay đường gửi bằng
   `transport.send()`, giữ nguyên nghiệp vụ.
5. Khi A có UDP, đổi đối tượng transport, không viết lại toàn bộ client.

## Checklist không bỏ sót

- [ ] Không `input()`, đọc file lớn hay xử lý nặng chặn event loop; dùng cơ chế nền phù hợp.
- [ ] Client nhận tự kiểm tra META, chuẩn bị nơi lưu rồi gửi FILE_ACCEPT; không có nút Accept/Reject.
- [ ] Sender đợi FILE_ACCEPT trước chunk; metadata giới hạn kích thước/tên.
- [ ] Login dùng `register_peer(peer, session_id)` và BIND; không tạo khóa giả.
- [ ] Chunk có `chunk_index`; không dùng tên file để định danh transfer; dùng `stream_id`.
- [ ] Nhận file vào `.part`; chỉ đổi tên sau khi đủ byte và SHA-256 khớp.
- [ ] Tên file từ mạng không được chứa đường dẫn để ghi ngoài thư mục nhận.
- [ ] Lỗi ghi file/dung lượng/hủy truyền báo rõ; không hiển thị thành công giả.
- [ ] Gửi bằng số tác vụ giới hạn để tận dụng window; không tạo hàng triệu task.
- [ ] Kết thúc file chờ xác nhận ứng dụng từ người nhận, không chỉ ACK từ server.
- [ ] Không gọi PayloadCipher hoặc bắt buộc cài cryptography trong bản đầu.
- [ ] Báo cáo rõ chưa có TLS/HMAC/AES; session/endpoint và SHA-256 không thay xác thực mật mã.

## Phụ thuộc và cách không bị chờ

| Chưa có từ A | Khoa có thể làm ngay |
|---|---|
| TCP/auth | UI với tài khoản giả; parse schema AUTH_OK; phần lỗi kết nối |
| UDP tin cậy | Nghiệp vụ dựa trên `MemoryTransport` |
| Routing | Demo forward thủ công hoặc fixture nhận từ server giả |
| Congestion | Thử file/queue giới hạn và chuẩn bị kịch bản tải |

Mock không thay được kiểm thử mạng ngày 3–4. Nếu phần A bị trễ, cùng hỗ trợ ghép
và sửa lỗi trước khi tiếp tục trang trí UI.
