# Nghiệm thu — ghi kết quả thật

Tất cả các mục bên dưới **chưa được kiểm thử trên ứng dụng thật**. Demo mock
không chứng minh đạt bất kỳ kiểm thử mạng nào trong bảng. Kích thước file, tỷ lệ
loss dưới đây là kịch bản đề xuất, không phải ngưỡng điểm do giảng viên quy định.

| ID | Tình huống | Kết quả mong đợi | Kết quả thực tế / bằng chứng |
|---|---|---|---|
| T01 | Kết nối + đăng nhập đúng/sai | Phản hồi rõ; sai không tạo session | Chưa chạy |
| T02 | 3 client, chat chung | Các đích dự kiến nhận đúng một lần | Chưa chạy |
| T03 | A nhắn riêng B, có C | B nhận; C không nhận nội dung | Chưa chạy |
| T04 | Gửi file rỗng, binary, tên tiếng Việt | Đúng byte/size/hash; tên an toàn | Chưa chạy |
| T05 | File 1 KiB, 1 MiB và 10 MiB | Không hỏng; bộ nhớ có giới hạn; progress đúng | Chưa chạy |
| T06 | Mô phỏng mất DATA và ACK 5–10% | Gửi lại, hoàn tất hoặc báo lỗi timeout rõ ràng | Chưa chạy |
| T07 | Gói trùng, sai thứ tự, ACK trùng | Không nhân đôi nội dung/progress; buffer hữu hạn | Chưa chạy |
| T08 | Tắt một client khi gửi file | Server sống; báo thất bại, dọn trạng thái/tệp tạm theo chính sách | Chưa chạy |
| T09 | Một file bị mất gói trong lúc chat | Chat/luồng khác tiếp tục; đo độ trễ thực tế | Chưa chạy |
| T10 | 10 rồi 20 client đồng thời | Auth/chat/online đúng; server không crash | Chưa chạy |
| T11 | Sai tag, gói sửa, session giả | Loại gói; không giả danh hay nhận ACK hợp lệ | Chưa chạy |
| T12 | Filename chứa ../ hoặc path tuyệt đối | Không ghi ra ngoài thư mục nhận | Chưa chạy |
| T13 | File thiếu chunk, hash sai, lỗi ghi | Không hiển thị thành công; không xuất file hoàn chỉnh sai | Chưa chạy |
| T14 | UI được giảng viên chấp thuận | Online/chat/progress/lỗi rõ, không block | Chưa chạy |
| T15 | Congestion dưới loss/delay theo yêu cầu | Hành vi/thuật toán đúng mức đã xác nhận; có số liệu | Chưa chạy |
| T16 | Máy còn lại cài/chạy từ README | Làm được từng bước, không cần hướng dẫn miệng | Chưa chạy |

## Mô phỏng mạng

Có thể viết lớp fault-injection quanh socket send/receive để drop/duplicate/
delay có seed cố định. Chạy với dữ liệu thử trong localhost/LAN. Ghi seed,
loss rate, loại gói bị tác động, file size và cấu hình. Chỉ dùng memory queue
không kiểm tra được UDP thật. Mỗi kiểm thử lưu PASS/FAIL cùng bằng chứng, không
điền số thời gian/tốc độ chưa đo. Hai máy vật lý bổ sung cho thử nhiều terminal.

## Đối chiếu 10 mục chấm điểm

| Mục đề | Module/bằng chứng chính |
|---|---|
| 1. Protocol | docs/01_PROTOCOL + sơ đồ trạng thái, viết trước code |
| 2. Handshake | server/app + client/service; T01 |
| 3. Auth/state | server/auth, sessions; T01, T08 |
| 4. Concurrency | async lifecycle; T09, T10 |
| 5. Messaging | router, service; T02, T03 |
| 6. File UDP | transport + files; T04–T07, T13 |
| 7. Errors | timeout/disconnect/cleanup; T08, T13 |
| 8. Security | cipher + auth/key handling; T11 |
| 9. UI | ui theo phản hồi giảng viên; T14 |
| 10. Quality/tests | code sạch, README, kết quả thật; T01–T16 |

Congestion và tránh HoL vẫn cần đối chiếu phần yêu cầu kỹ thuật dù không có
dòng điểm riêng. Không dùng bảng này để suy ra chắc chắn 10/10.
