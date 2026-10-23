# Nghiệm thu — ghi kết quả thật

Tất cả các mục bên dưới **chưa được kiểm thử trên ứng dụng thật**. Demo mock
không chứng minh đạt bất kỳ kiểm thử mạng nào trong bảng. Kích thước file, tỷ lệ
loss dưới đây là kịch bản đề xuất, không phải ngưỡng điểm do giảng viên quy định.

| ID | Tình huống | Kết quả mong đợi | Kết quả thực tế / bằng chứng |
|---|---|---|---|
| T01 | Kết nối + đăng nhập đúng/sai | Phản hồi rõ; sai không tạo session | Chưa chạy |
| T02 | 3 client, chat chung | Các đích dự kiến nhận đúng một lần | Chưa chạy |
| T03 | A nhắn riêng B, có C | B nhận; C không nhận nội dung | Chưa chạy |
| T04 | Gửi file rỗng, binary, tên tiếng Việt | Tự FILE_ACCEPT, không cần click nhận; đúng byte/size/hash; tên an toàn | Chưa chạy |
| T05 | File 1 KiB, 1 MiB và 10 MiB | Không hỏng; bộ nhớ có giới hạn; progress đúng | Chưa chạy |
| T06 | Mô phỏng mất DATA và ACK 5–10% | Gửi lại, hoàn tất hoặc báo lỗi timeout rõ ràng | Chưa chạy |
| T07 | Gói trùng, sai thứ tự, ACK trùng | Không nhân đôi nội dung/progress; buffer hữu hạn | Chưa chạy |
| T08 | Tắt một client khi gửi file | Server sống; báo thất bại, dọn trạng thái/tệp tạm theo chính sách | Chưa chạy |
| T09 | Một file bị mất gói trong lúc chat | Chat/luồng khác tiếp tục; đo độ trễ thực tế | Chưa chạy |
| T10 | 10 rồi 20 client đồng thời | Auth/chat/online đúng; server không crash | Chưa chạy |
| T11 | Session không tồn tại, sai endpoint, sender tự khai sai, packet sai schema/quá lớn | Từ chối sai phiên/nguồn/schema; sender lấy từ phiên; không tuyên bố chống giả mạo mật mã | Chưa chạy |
| T12 | Filename chứa ../ hoặc path tuyệt đối | Không ghi ra ngoài thư mục nhận | Chưa chạy |
| T13 | File thiếu chunk, hash sai, lỗi ghi | Không hiển thị thành công; không xuất file hoàn chỉnh sai | Chưa chạy |
| T14 | HTTP bridge localhost + polling | Online/chat/progress/lỗi rõ, không block | Chưa chạy |
| T15 | CUBIC giản lược dưới loss/delay | Cửa sổ tăng bậc ba, giảm loss/timeout, ACK trùng không tăng; có số liệu | Chưa chạy |
| T16 | Máy còn lại cài/chạy từ README | Làm được từng bước, không cần hướng dẫn miệng | Chưa chạy |
| T17 | Receiver dừng consume, window cạn dù DATA đã ACK | Không gửi seq vượt recv_limit; buffer/queue/task hữu hạn | Chưa chạy |
| T18 | Mất WINDOW_UPDATE mở cửa sổ, mất cả probe/phản hồi đầu | Probe nhịp 1 giây khôi phục credit; timeout hữu hạn nếu không tăng limit | Chưa chạy |
| T19 | ACK/UPDATE trùng, đảo thứ tự, sai stream/session | Không cộng credit; limit chỉ tăng ở đúng stream/phiên/hướng; ACK không nhận dữ liệu giả | Chưa chạy |
| T20 | Stream mới xin window lúc hết quota, rồi có chỗ; phiên kết thúc | Chưa cấp buffer thì limit=0; không ACK dữ liệu bỏ; giải phóng đúng quota/phiên | Chưa chạy |

Sai tag/nonce/replay mật mã chỉ kiểm thử nếu bổ sung TLS/HMAC/AES sau này,
không phải tiêu chí bắt buộc của bản đầu. T11 không chứng minh chống nghe lén
hay giả mạo chủ động khi kênh chưa được bảo vệ. Kiểm tra local riêng được ghi
trong [tiến độ](06_TIEN_DO.md), không tự chuyển các hàng mạng trên thành PASS.

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
| 8. Security | auth/session/endpoint, đầu vào/path/quota/localhost; T11–T13, ghi giới hạn chưa mã hóa |
| 9. UI | ui theo phản hồi giảng viên; T14 |
| 10. Quality/tests | code sạch, README, kết quả thật; T01–T20 |

Congestion và tránh HoL vẫn cần đối chiếu phần yêu cầu kỹ thuật dù không có
dòng điểm riêng. Không dùng bảng này để suy ra chắc chắn 10/10.
