# Giao thức v0 — đề xuất để hai người cùng code

Trạng thái: **chưa được nhóm/giảng viên phê duyệt, chưa được triển khai đầy đủ**.
Đây là quyết định thiết kế của starter, không phải cấu trúc bắt buộc do đề cho.
Sửa file này cùng `common/models.py` khi thay đổi giao thức.

## 1. Kiến trúc dự kiến

- Server trung tâm quản lý người dùng và chuyển tiếp chat/file.
- TCP: handshake và đăng nhập; dùng TLS để bảo vệ mật khẩu/cấp khóa phiên nếu
  phương án này được nhóm chọn. Không gửi khóa phiên thô qua LAN.
- UDP: dữ liệu chat/file, ACK, heartbeat, cập nhật người dùng và thông báo lỗi.
- UI độc lập core. Nếu web bắt buộc: xem phương án cầu nối ở `ui/README.md`.
- Dùng socket chuẩn, async I/O hoặc thread; không Flask/Django/WebSocket.

## 2. Handshake và auth TCP

Định dạng mỗi record: 4 byte độ dài big-endian + JSON UTF-8. Giới hạn record
64 KiB. Một lần `recv()` không đảm bảo đọc đủ record: phải đọc đủ prefix rồi
đọc đủ body, xử lý EOF giữa chừng. JSON không chứa dữ liệu file.

Các record dự kiến:

- `HELLO`: `version`, `client_name`.
- `HELLO_ACK`: `version`, `server_name`.
- `AUTH`: `username`, `password`; chỉ gửi trong kênh bảo vệ đã thiết lập.
- `AUTH_OK`: `session_id`, địa chỉ/cổng UDP server và thông tin khóa theo thiết
  kế bảo mật. Dùng hai khóa hướng client→server/server→client để tách nonce.
- `AUTH_ERROR`: `code`, `message`; không trả thông tin bí mật.

Mật khẩu lưu dưới dạng băm có salt bằng cơ chế chuẩn. Đăng ký tài khoản/database
không có trong phạm vi starter; có thể tạo trước tài khoản demo ở máy server.

## 3. UDP packet trước mã hóa

Định nghĩa trong `common/models.py`. JSON là lựa chọn dễ debug; dữ liệu nhị phân
được base64 khi serialize. Chưa có codec chạy thật: A thực hiện A1.

| Field | Ý nghĩa |
|---|---|
| `version` | Phiên bản giao thức, tạm là 1 |
| `kind` | Loại thông điệp trong `MessageType` |
| `session_id` | Phiên của chặng đang truyền, transport gán |
| `message_id` | ID logic UUID hex; giữ khi gửi lại và chuyển tiếp để đối chiếu |
| `sender`, `recipient` | Username; `recipient="*"` là chat chung |
| `stream_id` | Luồng độc lập, tối đa 64 ký tự; file dùng ID riêng |
| `seq` | Sequence bắt đầu từ 1 theo mỗi chặng/phiên/stream; transport gán |
| `ack` | Sequence được xác nhận cho packet ACK; ACK không được ACK tiếp |
| `chunk_index` | Chỉ số chunk bắt đầu từ 0; chỉ dùng với FILE_CHUNK |
| `payload` | Bytes trong code; base64 trong JSON wire |

Giới hạn khởi đầu: UDP cuối cùng tối đa 1200 byte; JSON trước mã hóa tối đa 1100
byte; payload thô mỗi packet tối đa 480 byte. Đây là **giá trị do nhóm đề xuất**.
Phải đo đúng byte sau serialize + envelope/tag; không cho rằng kích thước file
chunk bằng toàn bộ datagram. Từ chối gói quá lớn/trường sai, không âm thầm cắt.
Với tin chat dài hơn giới hạn, UI báo rõ; cơ chế phân mảnh chat là mở rộng sau.

Metadata, user list và lỗi cũng phải tuân giới hạn. USER_LIST phân trang trong
các payload `{revision, page, pages, users:[...]}`; số user trên mỗi trang theo
kích thước encode thực tế. Client chỉ thay danh sách khi nhận đủ cùng revision.
Như vậy 10–20 user không phải nhét vào một gói quá lớn.

## 4. Loại gói và payload

| Loại | Payload / hành vi |
|---|---|
| `BIND` / `BIND_OK` | Gắn UDP endpoint với session TCP đã xác thực; không tin IP/port tự khai |
| `HEARTBEAT` | Cập nhật last_seen; không cần giữ vô hạn khi mất gói |
| `USER_LIST` | JSON phân trang theo quy ước ở trên |
| `MSG` | Text UTF-8; người nhận `*` hoặc username |
| `FILE_META` | JSON: `name`, `size`, `sha256`, `chunk_size`, `total_chunks`; stream_id là transfer ID |
| `FILE_ACCEPT` | Người nhận đồng ý và đã chuẩn bị chỗ nhận |
| `FILE_CHUNK` | Bytes thô; chunk_index và stream_id trong header |
| `FILE_END` | Bên gửi đã hoàn tất chuỗi gửi; chưa được coi là file nhận thành công |
| `FILE_COMPLETE` | Người nhận kiểm tra đủ byte/hash rồi xác nhận; chứa `sha256`, `size` |
| `FILE_CANCEL` | Hủy truyền; giải phóng buffer/tệp tạm |
| `ACK` | ack tham chiếu một seq trong stream/phiên đó; payload rỗng |
| `ERROR` | JSON: `code`, `message`, `related_message_id` |
| `LOGOUT` | Thoát có chủ đích; mất gói vẫn được heartbeat timeout xử lý |

## 5. Độ tin cậy theo từng chặng

1. A→server: transport xác nhận nhận dữ liệu. Server kiểm tra session và định tuyến.
2. Server→B: một chặng riêng với sequence, ACK và khóa phiên của B.
3. ACK từ server **không chứng minh B đã lưu được file**. Chỉ FILE_COMPLETE từ B
   sau kiểm tra hash mới là thành công ở cấp ứng dụng. Lỗi chuyển tiếp phải báo A.

Tách sequence/window/buffer theo `(peer, session_id, stream_id)`; không tạo một
dãy seq toàn cục chặn mọi người dùng. Cùng stream cần giữ thứ tự khi nghiệp vụ
yêu cầu; các stream độc lập vẫn tiến triển khi file mất gói. Chat, quản lý user
và mỗi file dùng stream riêng; server tránh forward tuần tự chặn toàn bộ luồng.

Retransmit chỉ gói chưa ACK sau timeout; ACK trùng không nhân đôi tiến độ. Gói
trùng đã nhận cần ACK lại để xử lý ACK mất, nhưng không giao cho ứng dụng hai lần.
Không ACK packet đã bị loại vì buffer đầy. Buffer/window đều có giới hạn và cơ
chế báo khả năng nhận (receiver window) cần được nhóm chốt trong A6.

`send()` được await đến ACK hoặc thất bại. Để có sliding window, caller gửi bằng
producer/queue hoặc số task hữu hạn; không await tuần tự từng chunk thành stop-
and-wait, cũng không tạo task không giới hạn. Hết số retry báo lỗi/hủy rõ ràng.

## 6. Luồng file

1. Gửi FILE_META với transfer ID mới; đợi FILE_ACCEPT (hoặc lỗi/từ chối/timeout).
2. Gửi FILE_CHUNK theo cửa sổ truyền; ghi nhận tiến độ bằng byte duy nhất đã ACK.
3. Bên nhận lưu `.part`, xử lý chunk trùng/sai thứ tự và kiểm tra giới hạn.
4. Sau các chunk, gửi FILE_END. File rỗng có 0 chunk, vẫn có metadata/end/hash.
5. Bên nhận kiểm tra size và SHA-256, đổi tên file tạm rồi gửi FILE_COMPLETE.
6. Bên gửi nhận FILE_COMPLETE đúng transfer/hash mới hiển thị hoàn tất.

Giới hạn tên, quota, kích thước và thời gian chờ cần có; không dùng tên file do
mạng gửi để ghi ngoài thư mục nhận; tránh overwrite file sẵn có. Hash dùng để
kiểm tra toàn vẹn nội dung, không thay thế xác thực/mã hóa.

## 7. Bảo mật

Khoa triển khai AES-GCM bằng thư viện chuẩn. Mã hóa/authenticate cả header trong
và payload. Envelope ngoài có session ID và counter/nonce đủ để chọn khóa, được
ràng buộc vào associated data. A và Khoa chốt định dạng trước khi viết riêng.

Nonce phải duy nhất với mỗi khóa. Có thể dùng counter 96-bit riêng mỗi hướng
với khóa hướng riêng, không tái dùng khóa khi reset counter. Retry: gửi lại đúng
ciphertext cũ hoặc tạo nonce mới cho lần mã hóa mới; không đổi plaintext rồi mã
hóa lại với nonce cũ. Kiểm tra replay/dedup ở transport, cho phép ACK lại bản sao
hợp lệ. Gói bị sửa/sai tag bị loại, không cấp ACK hợp lệ cho dữ liệu giả.

Đây là bảo vệ theo chặng client–server. Server giải mã rồi mã hóa lại cho người
nhận; không mô tả là end-to-end encryption giữa hai người dùng.

## 8. Congestion và tài liệu tham khảo

Đề nêu BBR/CUBIC; xác nhận chọn thuật toán và mức triển khai ngày 1. Fixed window
hay timeout đơn giản chưa phải BBR/CUBIC. Ghi đúng thuật toán thực hiện và đo trên
loss/delay, phối hợp với flow control. Không lấy phần này ra khỏi TODO khi chưa xác nhận.

- Python asyncio/socket: https://docs.python.org/3/library/asyncio-protocol.html
- AES-GCM và nonce: https://cryptography.io/en/stable/hazmat/primitives/aead/
- Đề bài, mục 3, 6–10: nguồn các yêu cầu; giao thức cụ thể ở trên là đề xuất nhóm.
