# UI — Khoa phụ trách

Cập nhật 23/10/2026 theo README. **Đã chọn HTTP bridge cục bộ + polling**;
`bridge.py` vẫn TODO, `index.html`/`app.js` vẫn là fixture có nút disabled.
Mở HTML hiện tại chỉ xem khung, chưa đăng nhập/chat/file thật.

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

## Endpoint bridge dự kiến (chưa triển khai)

| Endpoint | Ý nghĩa |
|---|---|
| `POST /api/login` | Nhận thông tin server/login và gọi core |
| `POST /api/messages` | Gửi `{recipient, text}` |
| `GET /api/events?after=<id>` | Poll users/message/progress/status/error; cursor event còn cần triển khai |
| `POST /api/files` | Upload bytes có giới hạn, tạo transfer tới một người online |
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
8000/8001. Đây là cấu hình dự kiến; chưa có cờ `--http-port` hoặc lệnh chạy bridge
hoàn chỉnh, nên chưa dùng các ví dụ cổng làm hướng dẫn chạy thật.

## TODO[K6]

- [ ] Form login, trạng thái mạng, lỗi và logout.
- [ ] Online, chọn chat chung/riêng, tin đến cập nhật qua polling.
- [ ] Input vẫn đáp ứng khi đợi ACK; giới hạn tin nhắn theo byte UTF-8.
- [ ] File picker, tự nhận file, progress đúng nghĩa và lỗi rõ ràng.
- [ ] Event ID/cursor, queue hữu hạn và xử lý polling gián đoạn.
- [ ] Loại fixture khỏi đường chạy thật; chưa nối mạng thì không hiển thị thành công.
- [ ] Kiểm thử chat đồng thời file/loss và demo hai máy theo README sau khi ghép.
