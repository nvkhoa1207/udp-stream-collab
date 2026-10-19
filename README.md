# UDP Stream Collab

Bài tập lớn Mạng máy tính: **Distributed and Streaming-based Client-Server
Collaboration Application**.

Ứng dụng hướng đến chat chung, chat riêng, danh sách người dùng online và truyền
file tin cậy qua UDP. Nhóm tự xây giao thức ứng dụng, cơ chế xác nhận/gửi lại,
điều khiển luồng và điều khiển tắc nghẽn.

**Thành viên: Nguyễn Việt Khoa và Hải.**

> Dự án đang phát triển. Bảng tiến độ bên dưới mô tả code đã push lên GitHub,
> không đại diện cho toàn bộ code đang có trên máy của từng thành viên.
> Hiện chưa có bản chạy mạng đầu-cuối để nghiệm thu hoặc nộp bài.

## Phân công

| Thành viên | Phần phụ trách chính |
|---|---|
| **Hải** | Codec chung; server; đăng nhập/session/online; định tuyến; UDP socket; sequence, ACK, timeout, gửi lại; flow control và congestion control |
| **Khoa** | Client service; chat; chia/ghép file, metadata và SHA-256; hàng đợi sự kiện; giao diện HTTP/JavaScript; phối hợp phần bảo mật |
| **Cả hai** | Thống nhất giao thức/API; ghép module; kiểm thử mạng; báo cáo và demo |

Trong các TODO và comment cũ, **A / Bạn A là Hải**, còn **K / Khoa là Khoa**.
File [TODO_BAN_A.md](TODO_BAN_A.md) vẫn giữ tên hiện tại để các liên kết không bị
đứt. Danh sách của Khoa nằm ở [TODO_KHOA.md](TODO_KHOA.md).

## Tiến độ trên GitHub

Mốc kiểm tra: **19/10/2026**.

| Thành phần | Trạng thái code đã push |
|---|---|
| `Packet`, `MessageType`, `ReceivedPacket`, API `Transport` | Có định nghĩa chung |
| `MemoryHub`, `MemoryTransport` | Có demo chuyển packet qua server giả trong một process |
| `iter_chunks()`, `build_metadata()` | Đã chia file, tính kích thước, số chunk và SHA-256 |
| `scripts/demo_file_chunks.py` | Đã thử đọc chunk và ghi nối tiếp, đối chiếu hash |
| `FileReceiver.begin/accept_chunk/finish` | Bản trên GitHub còn TODO |
| `ClientService.login/send_chat/send_file/receive_events` | Bản trên GitHub còn TODO |
| Codec, server/session/router, `ReliableUdpTransport`, `CongestionController` | Có khung API và TODO; chưa chạy mạng thật |
| Giao diện | Có HTML/JS với dữ liệu mẫu; HTTP bridge chưa triển khai |
| Mã hóa payload | Chưa triển khai |

**Tiến độ cục bộ do Khoa cung cấp:** đã làm và chạy thử `FileReceiver`, gửi/nhận
chat qua mock và hàng đợi sự kiện, nhưng chưa push đầy đủ. Các phần này cần được
đồng bộ rồi kiểm tra lại trước khi đánh dấu hoàn thành trên GitHub.

Demo chia file hiện tại ghi nối tiếp theo thứ tự; nó **không gọi
`FileReceiver`**. Demo mock tạo và chuyển `Packet` trực tiếp; nó **không gọi
`ClientService`**, không mở socket và không kiểm tra mất gói UDP.

## Yêu cầu và hướng triển khai

Theo đề BTL và phản hồi giảng viên do nhóm cung cấp:

- **UDP là nền tảng truyền dữ liệu chat/file.** Nhóm tự bổ sung reliability,
  flow control và congestion control.
- Có xử lý nhiều client bằng async I/O hoặc thread; tách network, xử lý dữ liệu
  và giao diện. Mục tiêu nghiệm thu theo đề là 10–20 client đồng thời.
- **Cần triển khai HTTP và giao diện JavaScript.** Không coi CLI hoặc trang
  HTML tĩnh có dữ liệu mẫu là đã hoàn thành phần này.
- Không dùng Flask/Django hoặc WebSockets để thay phần networking phải tự làm.
- Giảng viên cho phép không triển khai mã hóa payload, nhưng vẫn cần các biện
  pháp bảo mật khác. AES-GCM có thể để sau; xác thực, kiểm tra phiên/quyền gửi,
  giới hạn đầu vào và bảo vệ đường dẫn file vẫn cần được thiết kế.
- Ghi rõ thiết kế và phạm vi tính năng nhóm lựa chọn theo hướng dẫn giảng viên.

**Ba chi tiết vẫn cần chốt:**

1. Phạm vi HTTP: phương án HTTP server cục bộ nối UI với client UDP có đáp ứng
   cách triển khai mà giảng viên yêu cầu hay không.
2. Đề nêu BBR/CUBIC: cần chọn thuật toán và xác nhận mức triển khai. Cửa sổ cố
   định hoặc chỉ có retry chưa đủ để gọi là BBR/CUBIC.
3. Rubric còn bước handshake TCP: giữ riêng bước TCP này hay thay bằng handshake
   UDP cần được xác nhận. Khung TCP hiện tại là phương án dự kiến.

Các tài liệu TODO, protocol và UI trước đây còn giả định AES/TLS bắt buộc hoặc
HTTP chưa bắt buộc. Khi triển khai, cập nhật chúng theo phản hồi mới và quyết
định đã thống nhất; không coi mọi lựa chọn trong starter là yêu cầu của đề.

## Kiến trúc dự kiến

| Thành phần | Vai trò |
|---|---|
| `common/` | Mô hình packet, cấu hình, codec và giao diện transport dùng chung |
| `server/` | Quản lý người dùng/phiên và chuyển tiếp chat/file tới người nhận |
| `transport/` | UDP tin cậy, ACK/retry, cửa sổ nhận/gửi và congestion control |
| `client/` | Xử lý chat, điều phối truyền file, chia/ghép dữ liệu và phát sự kiện |
| `ui/` | Hiển thị trạng thái, tin nhắn, online và tiến độ; kết nối HTTP với client |
| `security/` | Khung mã hóa tùy theo thiết kế bảo mật được chọn |
| `mocks/`, `scripts/` | Hỗ trợ phát triển và demo cục bộ trước khi ghép mạng thật |
| `docs/`, `reports/` | Đặc tả, API, tiêu chí nghiệm thu và dàn ý báo cáo |
| `data/` | File chạy thử cục bộ; không commit dữ liệu nhận hoặc thông tin bí mật |

Client gửi packet đến server; server tra người nhận rồi chuyển tiếp.
`recipient="*"` là quy ước chat chung trong đặc tả.

Khoa phát triển dựa trên `Transport` trong
[common/contracts.py](common/contracts.py). Hải triển khai
`ReliableUdpTransport` theo cùng API. Khi ghép, thay transport và bổ sung cấu
hình phiên; kiểm tra lại toàn bộ luồng chat/file trên socket thật.

Trước khi ghép cần thống nhất:

- Trường/loại gói báo dung lượng còn trống của bên nhận để thực hiện flow control;
  `Packet` hiện chưa có trường receiver window riêng.
- Sequence, dedup và bộ đệm theo từng stream; ngân sách truyền tổng hợp cho các
  stream dùng chung đường truyền.
- Một vòng nhận phân phối sự kiện; không để file đang chờ ACK chặn việc nhận chat.
- `send()` của transport thật chờ ACK chặng kế tiếp; gọi tuần tự từng chunk sẽ
  thành stop-and-wait, nên cần số tác vụ gửi đồng thời có giới hạn.
- ACK của server chỉ xác nhận chặng đến server. Chỉ báo file hoàn tất sau khi
  người nhận kiểm tra dữ liệu và trả `FILE_COMPLETE`.

## Cài đặt và chạy phần hiện có

Yêu cầu: **Python 3.11+** và Git. Trên macOS/Linux:

```bash
git clone https://github.com/nvkhoa1207/udp-stream-collab.git
cd udp-stream-collab
python3 -m venv .venv
source .venv/bin/activate
```

Trên Windows PowerShell, tạo môi trường bằng `py -3 -m venv .venv`, sau đó
kích hoạt bằng `.venv\Scripts\Activate.ps1`.

Nếu đã có repo trên máy, dùng thư mục hiện có. Commit hoặc stash phần đang sửa
trước khi đồng bộ; không clone đè hoặc reset để lấy README mới.

### 1. Demo chuyển packet trong bộ nhớ

```bash
python -m scripts.demo_mock
```

Kết quả: server giả nhận packet và chuyển đến client giả; chương trình in
`OK hợp đồng mock`. Đây là minh họa API transport, chưa phải chat qua LAN.

### 2. Demo chia file và kiểm tra SHA-256

```bash
python -m scripts.demo_file_chunks
```

Demo tạo/ghi đè hai file thử `data/demo_original.bin` và
`data/demo_copy.bin`. Với dữ liệu 1.000 byte, các phần có kích thước
**480, 480 và 40 byte**; kết quả mong đợi là `Hash giống nhau: True`.

480 byte là giới hạn payload hiện tại của nhóm trong
[common/config.py](common/config.py), không phải kích thước bắt buộc của UDP.
Demo in thứ tự chunk từ 1; trường `chunk_index` của giao thức bắt đầu từ 0.

### 3. Xem tham số khởi chạy

```bash
python -m server.main --help
python -m client.main --help
```

Hai lệnh `--help` chạy được. Chạy server/client thật hiện vẫn kết thúc với thông
báo chưa triển khai. Cổng TCP 9000 và UDP 9001 là cấu hình dự kiến của nhóm.

Các demo trên chỉ cần thư viện chuẩn. `requirements.txt` hiện có
`cryptography`; chỉ cần cài khi triển khai phần dùng thư viện này:

```bash
python -m pip install -r requirements.txt
```

## Kết quả kiểm tra hiện tại

Trên bản code GitHub được kiểm tra ngày 19/10/2026:

- Biên dịch kiểm tra cú pháp Python: thành công.
- Hai demo và hai lệnh `--help` ở trên: chạy thành công.
- Chia/ghép byte và kiểm tra metadata/hash với file 0, 1, 479, 480, 481, 960,
  1.000 và 4.097 byte: đạt.
- Các giá trị chunk size sai kiểu hoặc ngoài giới hạn: bị từ chối.

Các kết quả này chỉ xác nhận phần đã triển khai. Chưa nghiệm thu socket UDP,
mất DATA/ACK, gửi lại, flow/congestion control, nhiều client hay HTTP UI thật.

## Công việc tiếp theo

1. Khoa push phần client/file và các demo cục bộ đã làm.
2. Cả hai chốt codec, cấu hình phiên và thông tin flow control trong giao thức.
3. Hải hoàn thiện server/transport; Khoa nối `send_file()` và xử lý sự kiện.
4. Ghép hai client qua server, thử chat và truyền file; sau đó thử mất gói,
   ACK mất/trùng, sai thứ tự, ngắt kết nối và nhiều stream đồng thời.
5. Tích hợp HTTP UI theo phương án được chấp thuận; hoàn thiện bảo mật,
   đo congestion control và thử 10–20 client.
6. Cập nhật hướng dẫn chạy thật, kết quả nghiệm thu và báo cáo theo code đã có.

Lịch [TODO_5_NGAY.md](TODO_5_NGAY.md) là kế hoạch ban đầu, không phải cam kết
tiến độ hoặc bằng chứng chức năng đã hoàn thành.

## Tài liệu

- [Đặc tả giao thức](docs/01_PROTOCOL.md)
- [API dùng chung](docs/02_API.md)
- [Quy trình Git](docs/03_GIT_WORKFLOW.md)
- [Tiêu chí nghiệm thu](docs/04_ACCEPTANCE.md)
- [Định hướng UI](ui/README.md)
- [Dàn ý báo cáo](reports/REPORT_OUTLINE.md)


Khi nộp: chuẩn bị mã nguồn, README chạy được từ đầu và báo cáo PDF theo đề.
Đề cho phép nộp ZIP hoặc liên kết repo GitHub private; kiểm tra lại hình thức
nộp và trạng thái repo trước hạn.
