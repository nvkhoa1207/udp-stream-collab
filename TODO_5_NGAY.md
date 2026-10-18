# TODO chung — 5 ngày, 2 người

Ngày 1–5 tính từ ngày nhóm bắt đầu, không tự gán hạn nộp. Hai bạn nên có phần lớn
thời gian mỗi ngày cho BTL. Nếu chỉ làm vài giờ buổi tối, phạm vi đầy đủ có thể
không kịp; báo sớm phần còn thiếu và hỏi giảng viên, không ghi hoàn thành giả.

## Nguyên tắc

- [ ] Chốt owner: A = __________ ; Khoa = client/file/security/UI.
- [ ] Đọc câu hỏi cần xác nhận trong README và ghi trả lời ở cuối file này.
- [ ] Chốt API trước khi code riêng; thay đổi `common/` phải báo người còn lại.
- [ ] Đầu ngày: 15 phút chốt đầu vào/đầu ra. Cuối ngày: ghép code và demo 30–45 phút.
- [ ] Một người không phải đợi người kia: dùng mock/fixture, ghi rõ đây là dữ liệu giả.
- [ ] Không thêm đăng ký tài khoản, avatar, lịch sử chat, database, cloud deploy,
      audio/video hoặc resume file sau restart trong 5 ngày nếu chưa xong lõi.
- [ ] Không bỏ sequence/ACK/gửi lại/mã hóa/UI bắt buộc hoặc congestion mà không xác nhận.

## Ngày 1 — chốt hợp đồng và nối hai đầu

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A1, A2: codec, TCP handshake và khung server async | K1: client kết nối; chuẩn bị fixture người dùng/tin nhắn | Client thật nhận phản hồi server; codec thống nhất; mock chạy riêng |

- [ ] Chốt transport TCP/UDP, cách UI và mức BBR/CUBIC cần làm.
- [ ] Chốt loại gói, tên field, port, giới hạn kích thước và API.
- [ ] Quyết định và thử sớm cách bảo vệ đăng nhập/cấp khóa; TLS tự ký với CA/cert
      được client tin cậy là phương án đề xuất, không gửi mật khẩu/khóa thô qua LAN.
- [ ] Tạo hai nhánh riêng; cùng chạy một commit nền trước khi tách việc.

## Ngày 2 — đăng nhập, online, chat; file và UI làm trên mock

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A3–A5: auth, session, heartbeat, routing; UDP send/receive cơ bản | K2–K3: chat và chia/ghép file trên mock; K6: UI với fixture | 3 client: chat chung/riêng; danh sách online cập nhật |

- [ ] Chưa có ACK thì ghi rõ UDP tạm thời chưa tin cậy; không đánh dấu xong mục 6.
- [ ] Kiểm tra ngắt một client không làm server sập.
- [ ] Chốt khóa phiên, định dạng envelope bảo vệ với Khoa; không để đến ngày 4 mới bàn.

## Ngày 3 — độ tin cậy và truyền file đầu-cuối

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A6: ACK, sequence, retransmit chọn lọc, window, buffer theo stream | K3–K5: file thật, hash, tiến độ; mã hóa dùng chung | File nhỏ/lớn có hash trùng nhau; mất gói mô phỏng vẫn hoàn tất; chat vẫn dùng được |

- [ ] Cả DATA và ACK đều được kiểm tra tính xác thực khi đã tích hợp mật mã.
- [ ] Mất ACK, gói trùng, sai thứ tự, retry hết hạn đều có xử lý.
- [ ] `send()` ACK theo từng chặng; chỉ báo gửi file xong khi người nhận xác minh file.

## Ngày 4 — tích hợp đủ các yêu cầu còn lại

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A7: congestion theo xác nhận, độc lập các stream, 10–20 client | K5–K7: tích hợp bảo mật/UI, lỗi file, kiểm thử tải và dữ liệu | Chạy kịch bản đầy đủ qua UI được chấp thuận, lấy log và ảnh minh chứng |

- [ ] Nếu BBR/CUBIC đầy đủ là bắt buộc, triển khai và đo theo phạm vi đó; bộ điều
      chỉnh tốc độ đơn giản không được tự gọi là BBR/CUBIC.
- [ ] Kiểm tra cùng lúc chat và file; kiểm tra đường truyền có loss/delay.
- [ ] Chụp bằng chứng thật, ghi cả lỗi/giới hạn còn tồn tại.

## Ngày 5 — chốt, thử máy sạch và nộp

| A | Khoa | Kết quả |
|---|---|---|
| Sửa lỗi lõi, viết kiến trúc/giao thức | Sửa client/UI, viết luồng file và kết quả kiểm thử | README chạy đúng, PDF, source sạch, kịch bản demo |

- [ ] Buổi đầu: chạy hết `docs/04_ACCEPTANCE.md`, sửa lỗi làm mất/hỏng dữ liệu trước.
- [ ] Giữa ngày: đóng tính năng, chỉ sửa lỗi; hai người viết báo cáo phần của mình.
- [ ] Cuối ngày: thử cài/chạy theo README trên máy còn lại; gói bản nộp, không chứa khóa.
- [ ] Mỗi người giải thích được luồng tổng thể và code mình phụ trách.

## Trả lời của giảng viên / thay đổi phạm vi

| Câu hỏi | Trả lời thực tế | Ngày / người cập nhật |
|---|---|---|
| TCP và UDP | Chưa xác nhận | |
| Web UI / cầu nối | Chưa xác nhận | |
| BBR/CUBIC | Chưa xác nhận | |
| Giới hạn ngôn ngữ/thư viện | Chưa xác nhận | |
