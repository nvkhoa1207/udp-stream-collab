# UI — Khoa phụ trách

`index.html`/`app.js` là placeholder có label/form và fixture nhỏ; các nút đang
disabled, chưa đăng nhập, gửi chat hay truyền file được. Mở bằng trình duyệt để
xem khung. Không coi đây là giao diện hoàn chỉnh đạt mục 9.

## Chốt cách kết nối với giảng viên ngày 1

Phương án đề xuất nếu được chấp thuận:

- Mỗi máy chạy chương trình client Python có core UDP và một HTTP bridge chỉ
  bind `127.0.0.1`. Browser gọi bridge cùng origin bằng JavaScript `fetch`.
- Bridge gọi ClientService; ClientService gửi/nhận UDP với server mạng.
- HTTP là phần nối UI cục bộ, không thay thế giao thức UDP tự thiết kế giữa client/server.
- Server web/bridge dùng thư viện chuẩn hoặc socket theo mức giảng viên cho phép;
  không Flask/Django/WebSockets. Hỏi rõ `http.server` có được phép cho UI bridge không.
- Poll hoặc long-poll nhận event; bounded queue, tránh block vòng UDP. Phục vụ
  HTML/JS từ cùng bridge; không dùng file:// để gọi bridge khi tích hợp thật.

Đề còn cho CLI/GUI ở chỗ khác; nếu giảng viên chấp thuận CLI đủ điểm, ghi xác nhận
vào TODO_5_NGAY và có thể làm CLI để tiết kiệm thời gian. Không tự suy ra CLI đủ.

## Endpoint bridge đề xuất (chưa triển khai)

| Endpoint | Ý nghĩa |
|---|---|
| `POST /api/login` | Đọc form và gọi core login |
| `POST /api/messages` | Gửi `{recipient, text}` |
| `GET /api/events?after=<id>` | Lấy event mới: users/message/progress/error |
| `POST /api/files` | Nhận bytes file có giới hạn, tạo transfer qua core |
| `POST /api/logout` | Logout và dọn session |

File upload cần giới hạn body và file tạm, không nhét file lớn vào một JSON/base64
khổng lồ. Bind loopback, kiểm tra Origin và có token cục bộ cho API thay đổi trạng
thái; không mở bridge ra LAN. Dùng textContent khi hiện tin/username/tên file.

## TODO[K6]

- [ ] Thêm form login, trạng thái mạng và lỗi.
- [ ] Danh sách online, chọn chat chung/riêng.
- [ ] Tin đến tự cập nhật; không khóa input khi đợi ACK.
- [ ] File picker và progress; chỉ hiện hoàn tất sau FILE_COMPLETE đúng hash.
- [ ] Hiện rõ chế độ fixture/mock; loại fixture khỏi đường chạy thật.
- [ ] Nối bridge với ClientService và kiểm tra UI vẫn đáp ứng trong khi gửi file.
