# UI và HTTP bridge — Khoa

Đã nối HTML/JS với HTTP bridge/core, bỏ fixture. Chạy từ repo:

```bash
python -m client.main --http-port 8000 --received-dir data/received
```

Mở http://127.0.0.1:8000. Form đăng nhập, chat, upload nhị phân, polling và hủy
hai chiều đã có. Thư mục nhận được in ở terminal. Nhập server TCP từ UI, không
truyền password qua CLI. Transport/server của Hải còn stub nên login thật hiện
báo 503; không có chế độ giả tự động báo connected. Ctrl+C dọn job/phiên.

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
| `GET /api/events?after=<id>` | Poll users/message/progress/status/error; cursor event còn cần triển khai |
| `POST /api/files?recipient=...&name=...` | Body nhị phân tối đa 16 MiB |
| `POST /api/cancel` | `{direction,transfer_id}`, gửi hoặc nhận |
| `POST /api/logout` | Logout và dọn phiên |

Không có endpoint để người dùng Accept/Reject file. Core kiểm tra metadata,
chuẩn bị nơi lưu rồi tự trả FILE_ACCEPT; lỗi thì ERROR. UI hiển thị tiến độ/lỗi.
Tiến độ sender phản ánh byte ACK đến server; 100% ở chặng này là “chờ xác minh”,
chỉ FILE_COMPLETE đúng size/hash mới thành “hoàn tất”. ACK chat chưa phải đã đọc.

Upload cần giới hạn body/file tạm, không nhét file lớn vào JSON/base64. Khi
triển khai, kiểm tra Host/Origin và token API cục bộ, bind loopback, hiển thị
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

117 test local đã đạt tại commit bridge. Browser localhost đã kiểm tra form,
trạng thái chưa kết nối, nút chat/file bị khóa và lỗi transport chưa triển khai.
JS kiểm tra cú pháp bằng node --check. API có Host/Origin/token, ring 512 event,
4 job, upload tạm; UI textContent tránh diễn giải nội dung chat thành HTML.
Khi cursor quá cũ, UI báo mất một phần lịch sử; đây không phải lưu chat lâu dài.
Cần Hải cung cấp transport/server để nghiệm thu online/chat/file trên UI thật.
