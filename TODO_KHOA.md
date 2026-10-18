# Khoa — client, file, mật mã và giao diện

Owner: `client/`, `security/`, `ui/`. Dùng `common/` đã chốt, không tự đổi field.
Mở `scripts/demo_mock.py` trước: đây là cách làm song song mà chưa cần server thật.

| ID | Ngày | Việc cần code / file | Xong khi |
|---|---|---|---|
| K1 | 1 | `client/main.py`, `client/service.py`: connect/login và event | Client nhận handshake thật; lỗi kết nối hiển thị rõ; import không tự mở socket |
| K2 | 2 | `client/service.py`: chat và danh sách người dùng | Nhập/gửi/nhận cùng lúc; chọn đúng recipient; thử trước với mock |
| K3 | 2–3 | `client/files.py`: metadata, chunk, reassembly, hash | File rỗng, nhị phân, nhiều chunk đều đúng byte; không đọc cả file lớn vào RAM |
| K4 | 3 | `client/service.py`, `client/files.py`: file đầu-cuối | Tiến độ từ byte đã ACK; chỉ hoàn tất khi nhận FILE_COMPLETE với hash đúng |
| K5 | 3–4 | `security/cipher.py`: AES-GCM, phối hợp TLS/cấp khóa với A | Mã hóa/giải mã đúng; dữ liệu bị sửa bị loại; retry không làm reuse nonce sai |
| K6 | 2–4 | `ui/`: fixture ngày 2, giao diện thật và cầu nối sau khi xác nhận | Hiện online, chat, progress và lỗi; không dùng WebSocket/framework bị cấm |
| K7 | 4 | Kiểm thử client/file/mất gói cùng A | Có kết quả thật, hash, log, ảnh; UI không đứng khi file đang gửi |
| K8 | 5 | Báo cáo luồng chat/file, UI, kiểm thử + README client | Người khác chạy được client; mô tả đúng cái đã làm |

## Thứ tự bắt đầu

1. Chạy `python -m scripts.demo_mock` và đọc `common/contracts.py`.
2. Tạo `ClientService(transport)` có vòng nhận chạy nền và đẩy event vào queue.
3. Dùng fixture online/chat/file-progress để làm UI khi A chưa có auth/router.
4. Viết chia/ghép file bằng `BytesIO`/file tạm trước; sau đó thay đường gửi bằng
   `transport.send()`, giữ nguyên nghiệp vụ.
5. Khi A có UDP, đổi đối tượng transport, không viết lại toàn bộ client.

## Checklist không bỏ sót

- [ ] Không `input()`, đọc file lớn hay xử lý nặng chặn event loop; dùng cơ chế nền phù hợp.
- [ ] FILE_META được chấp thuận trước khi gửi chunk; metadata giới hạn kích thước/tên.
- [ ] Chunk có `chunk_index`; không dùng tên file để định danh transfer; dùng `stream_id`.
- [ ] Nhận file vào `.part`; chỉ đổi tên sau khi đủ byte và SHA-256 khớp.
- [ ] Tên file từ mạng không được chứa đường dẫn để ghi ngoài thư mục nhận.
- [ ] Lỗi ghi file/dung lượng/hủy truyền báo rõ; không hiển thị thành công giả.
- [ ] Gửi bằng số tác vụ giới hạn để tận dụng window; không tạo hàng triệu task.
- [ ] Kết thúc file chờ xác nhận ứng dụng từ người nhận, không chỉ ACK từ server.
- [ ] Mật mã dùng thư viện chuẩn; không tự viết AES; không commit khóa/cert private.
- [ ] Báo cáo rõ bảo vệ theo từng chặng client–server, không gọi là end-to-end encryption.

## Phụ thuộc và cách không bị chờ

| Chưa có từ A | Khoa có thể làm ngay |
|---|---|
| TCP/auth | UI với tài khoản giả; parse schema AUTH_OK; phần lỗi kết nối |
| UDP tin cậy | Nghiệp vụ dựa trên `MemoryTransport` |
| Routing | Demo forward thủ công hoặc fixture nhận từ server giả |
| Congestion | Thử file/queue giới hạn và chuẩn bị kịch bản tải |

Mock không thay được kiểm thử mạng ngày 3–4. Nếu phần A bị trễ, cùng hỗ trợ ghép
và sửa lỗi trước khi tiếp tục trang trí UI.
