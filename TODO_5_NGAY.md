# TODO chung — 5 ngày, 2 người

Ngày 1–5 tính từ ngày nhóm bắt đầu, không tự gán hạn nộp. Hai bạn nên có phần lớn
thời gian mỗi ngày cho BTL. Nếu chỉ làm vài giờ buổi tối, phạm vi đầy đủ có thể
không kịp; báo sớm phần còn thiếu và hỏi giảng viên, không ghi hoàn thành giả.

## Nguyên tắc

- [x] Owner: A = Hải (server/transport/common); Khoa = client/file/UI.
      Hai người phối hợp bảo mật theo phạm vi bản đầu.
- [x] Đồng bộ quyết định phạm vi từ README; bảng căn cứ ở cuối file này.
      Dấu tick quyết định không có nghĩa tính năng đã triển khai.
- [ ] Chốt API trước khi code riêng; thay đổi `common/` phải báo người còn lại.
- [ ] Đầu ngày: 15 phút chốt đầu vào/đầu ra. Cuối ngày: ghép code và demo 30–45 phút.
- [ ] Một người không phải đợi người kia: dùng mock/fixture, ghi rõ đây là dữ liệu giả.
- [ ] Không thêm đăng ký tài khoản, avatar, lịch sử chat, database, cloud deploy,
      audio/video hoặc resume file sau restart trong 5 ngày nếu chưa xong lõi.
- [ ] Giữ sequence/ACK/retry, flow control, CUBIC giản lược và HTTP/JS UI.
      Mã hóa ngoài phạm vi đầu theo README; không bỏ kiểm tra phiên/đầu vào.

## Ngày 1 — chốt hợp đồng và nối hai đầu

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A1, A2: codec, TCP handshake và khung server async | K1: client kết nối; chuẩn bị fixture người dùng/tin nhắn | Client thật nhận phản hồi server; codec thống nhất; mock chạy riêng |

- [x] TCP handshake/login; UDP chat/file; bridge localhost + polling; CUBIC giản lược.
- [x] Hợp đồng packet/API ở docs/01_PROTOCOL.md và docs/02_API.md: session
      không cần khóa; recv_limit theo packet, WINDOW_UPDATE/PROBE. Codec/transport
      và kiểm thử hợp đồng trên mạng vẫn A1/A6, chưa hoàn thành.
- [ ] Triển khai băm mật khẩu lưu trữ, session/BIND và kiểm tra đầu vào;
      ghi rõ AUTH qua TCP chưa TLS và UDP chưa có xác thực mật mã.
- [ ] Tạo hai nhánh riêng; cùng chạy một commit nền trước khi tách việc.

## Ngày 2 — đăng nhập, online, chat; file và UI làm trên mock

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A3–A5: auth, session, heartbeat, routing; UDP send/receive cơ bản | K2–K3: chat và chia/ghép file trên mock; K6: UI với fixture | 3 client: chat chung/riêng; danh sách online cập nhật |

- [ ] Chưa có ACK thì ghi rõ UDP tạm thời chưa tin cậy; không đánh dấu xong mục 6.
- [ ] Kiểm tra ngắt một client không làm server sập.
- [ ] Ghép AUTH_OK(session_id, udp_port) và register_peer(peer, session_id);
      không cấp khóa hoặc gọi cipher stub.

## Ngày 3 — độ tin cậy và truyền file đầu-cuối

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A6: ACK, sequence, retransmit chọn lọc, window, buffer theo stream | K3–K5: file tự động nhận, hash, tiến độ, kiểm tra an toàn đầu vào | File nhỏ/lớn có hash trùng nhau; mất gói mô phỏng vẫn hoàn tất; chat vẫn dùng được |

- [ ] DATA/ACK/window đối chiếu session/endpoint; không gọi đây là xác thực mật mã.
- [ ] Thử hết credit, mất UPDATE mở cửa sổ, probe bị mất và receiver chậm.
- [ ] Mất ACK, gói trùng, sai thứ tự, retry hết hạn đều có xử lý.
- [ ] `send()` ACK theo từng chặng; chỉ báo gửi file xong khi người nhận xác minh file.

## Ngày 4 — tích hợp đủ các yêu cầu còn lại

| A | Khoa | Ghép cuối ngày |
|---|---|---|
| A7: CUBIC giản lược, độc lập các stream, 10–20 client | K5–K7: tích hợp bảo mật/UI, lỗi file, kiểm thử tải và dữ liệu | Chạy kịch bản đầy đủ qua HTTP UI localhost, lấy log và ảnh minh chứng |

- [ ] CUBIC có tăng theo hàm bậc ba, phản ứng loss/timeout và số liệu; fixed
      window không được gọi là CUBIC. Ghi rõ phần giản lược đã làm.
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

## Phạm vi đã ghi nhận

Căn cứ: README ghi phản hồi do Khoa cung cấp ngày 21/10/2026; cuộc trò chuyện
chia sẻ xác nhận TCP/UDP và giới hạn thư viện/framework. Hai điều về mã hóa và
CUBIC giữ theo phản hồi đã có trong README, không coi là xác nhận mới ngày 23/10.

| Câu hỏi | Trả lời thực tế | Ngày / người cập nhật |
|---|---|---|
| TCP và UDP | Dùng cả hai; nhóm phân TCP login, UDP chat/file | README + cuộc trò chuyện; đồng bộ 23/10/2026 |
| Web UI / cầu nối | Web đơn giản; nhóm chọn HTTP localhost + polling | README + cuộc trò chuyện; đồng bộ 23/10/2026 |
| BBR/CUBIC | Được giản lược theo phản hồi ghi trong README; nhóm chọn CUBIC | README 21/10/2026 |
| Giới hạn ngôn ngữ/thư viện | Python dùng thư viện, không framework; JS được dùng thư viện | README + cuộc trò chuyện; đồng bộ 23/10/2026 |
| Mã hóa payload | Có thể chưa làm theo phản hồi ghi trong README; vẫn kiểm tra auth/session/đầu vào | README 21/10/2026 |
