# UI và HTTP bridge — Khoa

## Cập nhật giao diện 29/10/2026

Nhãn, thông báo và lỗi của ứng dụng dùng tiếng Anh. Comment trong code dùng
tiếng Việt ngắn gọn. HTML/CSS được xuống dòng; JavaScript tách thành các hàm
đăng nhập, chọn người nhận, hiển thị chat, cập nhật file và lấy sự kiện.

- Desktop hiển thị người online, chat và file cạnh nhau; màn hình nhỏ xếp dọc.
- Chọn người nhận bằng danh sách nút hoặc ô Recipient, dùng chung cho chat/file.
- Chat có nhãn Broadcast/Private và bộ đếm byte UTF-8. Enter vẫn xuống dòng;
  bấm Send message để gửi, giới hạn vẫn là 480 byte.
- File có tên, dung lượng, phần trăm, trạng thái và nút Cancel. Tối đa 16 MiB,
  tự nhận như cũ; 100% số byte chưa đồng nghĩa với Completed.
- Giữ nguyên endpoint, token, CSP, cách polling và dùng textContent để hiển thị.
  Event file gửi có thêm trường name để hiển thị tên từ metadata đã có;
  không thay gói tin truyền qua TCP/UDP.

Đọc code theo thứ tự: `index.html` (các khung), `style.css` (bố cục),
`app.js` (sự kiện trên browser), `bridge.py` (HTTP), `runtime.py` (gọi client).
Phần gửi UDP tách riêng bước đợi cửa sổ nhận và đợi CUBIC để dễ theo dõi.

Đã kiểm tra bản refactor: 202 test đạt khi bật BTL_NETWORK_TEST, 11 lượt demo
đạt và JavaScript qua node --check. Hai browser thật đã thử đăng nhập, chat
hai chiều, Unicode, giới hạn 480 byte, truyền file 1200 byte và hủy ở cả hai
phía. Bố cục 390 px không tràn ngang; nội dung chứa thẻ HTML vẫn hiện như chữ.

## Chạy ứng dụng

Đã nối HTML/JS với HTTP bridge/core, bỏ fixture. Chạy từ repo:

```bash
python -m client.main --http-port 8000 --received-dir data/received
```

Mở http://127.0.0.1:8000. Form đăng nhập, chat, upload nhị phân, polling và hủy
hai chiều đã có. Thư mục nhận được in ở terminal. Nhập server TCP từ UI, không
truyền password qua CLI. Transport/server đã ghép thật; chạy server.main và hai client theo README.
Sai mật khẩu trả 401, mất kết nối trả 503; không có fixture thay tính năng thật. Ctrl+C dọn job/phiên.

## Cách kết nối đã chọn

- Mỗi người chạy client Python và browser trên cùng máy. Bridge chỉ bind
  `127.0.0.1`, phục vụ HTML/CSS/JS và API cùng origin.
- Browser gọi `fetch()` đến bridge; bridge gọi ClientService. Python client
  dùng TCP handshake/login và UDP chat/file với server trung tâm.
- Dùng thư viện chuẩn `http.server` và socket/asyncio hoặc thread; Python không
  dùng framework Flask/Django/FastAPI, không WebSocket. JS được dùng thư viện,
  bản đầu chọn HTML/CSS/JS thuần.
- Polling định kỳ để lấy event; chưa cần long-polling. Chỉ một request polling
  đang chạy mỗi UI, đặt lần tiếp theo sau khi request trước kết thúc.
- Queue/request worker/upload đều có giới hạn; đợi ACK hoặc ghi file không chặn
  vòng UDP hay giao diện. Bridge chuyển lệnh/sự kiện, không làm lại transport.

## Endpoint bridge

| Endpoint | Ý nghĩa |
|---|---|
| `POST /api/login` | `{host,port,username,password}` |
| `POST /api/messages` | Gửi `{recipient, text}` |
| `GET /api/events?after=<id>` | Poll users/message/progress/status/error; cursor/ring 512 event đã có |
| `POST /api/files?recipient=...&name=...` | Body nhị phân tối đa 16 MiB |
| `POST /api/cancel` | `{direction,transfer_id}`, gửi hoặc nhận |
| `POST /api/logout` | Logout và dọn phiên |

Không có endpoint để người dùng Accept/Reject file. Core kiểm tra metadata,
chuẩn bị nơi lưu rồi tự trả FILE_ACCEPT; lỗi thì ERROR. UI hiển thị tiến độ/lỗi.
Tiến độ sender phản ánh byte ACK đến server; 100% ở chặng này là “chờ xác minh”,
chỉ FILE_COMPLETE đúng size/hash mới thành “hoàn tất”. ACK chat chưa phải đã đọc.

Upload cần giới hạn body/file tạm, không nhét file lớn vào JSON/base64. Bridge đã
kiểm tra Host/Origin và token API cục bộ, bind loopback, hiển thị
text bằng `textContent`; không đưa password/session vào log hoặc URL.

## Demo hai máy sau khi ghép mạng

| Máy Khoa | Máy Hải |
|---|---|
| Server TCP/UDP + client/bridge + browser | Client/bridge + browser |
| Server nghe địa chỉ LAN hoặc `0.0.0.0` | Client trỏ IP LAN của máy Khoa |
| Client cũng trỏ IP LAN máy Khoa | Browser mở bridge localhost của máy Hải |

Hai máy là đủ. Server nghe LAN không làm HTTP bridge nghe LAN: bridge của cả hai
máy vẫn chỉ localhost. Một máy chạy nhiều client cần cổng HTTP khác nhau, ví dụ
8000/8001. HTTP bridge đã có cờ --http-port; phần kết nối LAN vẫn chưa nghiệm thu.

## Kiểm tra

Ghi nhận cũ: 117 test local đạt tại commit bridge. Browser localhost đã kiểm tra form,
trạng thái chưa kết nối, nút chat/file bị khóa và lỗi transport chưa triển khai.
JS kiểm tra cú pháp bằng node --check. API có Host/Origin/token, ring 512 event,
4 job, upload tạm; UI textContent tránh diễn giải nội dung chat thành HTML.
Khi cursor quá cũ, UI báo mất một phần lịch sử; đây không phải lưu chat lâu dài.
Ngày 29/10/2026 đã thử hai browser: login, online, chat, file 1200 byte completed
hai phía và hủy từ sender/receiver. Hủy dùng fixture chậm 15s trước mỗi send chunk
để thao tác nút; không thay ACK/socket thật. HTTP E2E còn kiểm tra upload/poll/logout
với backend thật, không factory mock. Đã sửa đọc HTML UTF-8 trên Windows và bỏ
ERROR đáp ERROR sau CANCEL. LAN hai máy/Docker vẫn chưa chạy.
