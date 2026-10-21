# UDP Stream Collab

Bài tập lớn Mạng máy tính: **Distributed and Streaming-based Client-Server
Collaboration Application**.

Ứng dụng hướng đến chat chung, chat riêng, danh sách người dùng online và truyền
file tin cậy qua UDP. Nhóm tự xây giao thức ứng dụng, cơ chế xác nhận/gửi lại,
điều khiển luồng và điều khiển tắc nghẽn.

**Thành viên: Nguyễn Việt Khoa và Hải.**

**Cập nhật yêu cầu: 21/10/2026**, theo phản hồi giảng viên do Khoa cung cấp.

> Dự án đang phát triển. Bảng tiến độ GitHub bên dưới là kết quả kiểm tra ngày
> 19/10/2026; chưa đối chiếu lại repository trong lần cập nhật README này.
> Code cục bộ có thể mới hơn. Kiến trúc dự kiến không đồng nghĩa với tính năng
> đã triển khai hoặc đã nghiệm thu.

## Phân công

| Thành viên | Phần phụ trách chính |
|---|---|
| **Hải** | Codec chung; server TCP/UDP; đăng nhập/session/online; định tuyến; sequence, ACK, timeout, gửi lại; flow control và congestion control đơn giản hóa |
| **Khoa** | Client service; chat; chia/ghép file, metadata và SHA-256; hàng đợi sự kiện; giao diện HTTP/JavaScript; phối hợp phần bảo mật |
| **Cả hai** | Thống nhất giao thức/API; ghép module; kiểm thử mạng; báo cáo và demo |

Trong các TODO và comment cũ, **A / Bạn A là Hải**, còn **K / Khoa là Khoa**.
File [TODO_BAN_A.md](TODO_BAN_A.md) vẫn giữ tên hiện tại để các liên kết không bị
đứt. Danh sách của Khoa nằm ở [TODO_KHOA.md](TODO_KHOA.md).

## Tiến độ tại lần kiểm tra GitHub gần nhất

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

**Tiến độ cục bộ:** Khoa đã cung cấp code `FileReceiver` gồm `begin()`,
`accept_chunk()` và `finish()`, có ghi file tạm, theo dõi chunk trùng và kiểm tra
kích thước/hash. Theo thông tin Khoa cung cấp, phần này cùng chat qua mock và
hàng đợi sự kiện đã được chạy thử cục bộ, nhưng chưa push đầy đủ. Cần đồng bộ
code và kết quả chạy trước khi cập nhật trạng thái trên GitHub.

Demo chia file hiện tại ghi nối tiếp theo thứ tự; nó **không gọi
`FileReceiver`**. Demo mock tạo và chuyển `Packet` trực tiếp; nó **không gọi
`ClientService`**, không mở socket và không kiểm tra mất gói UDP.

## Yêu cầu đã xác nhận với giảng viên

Phần này ghi nhận phản hồi giảng viên do Khoa cung cấp; cách chia vai trò cho
từng giao thức nằm ở mục kiến trúc bên dưới.

- **Phải sử dụng cả TCP và UDP.** Phần chat/truyền file trên UDP vẫn cần tự bổ
  sung reliability, flow control và congestion control theo yêu cầu BTL.
- **Giao diện được dùng thư viện JavaScript tùy chọn.** Giảng viên không đặt
  nặng hình thức; ưu tiên thao tác được và hiển thị đúng trạng thái. Vẫn giữ
  giao diện HTTP/JavaScript theo yêu cầu đã trao đổi trước đó.
- **Python được dùng thư viện, nhưng không dùng framework.** Nhóm ưu tiên
  `socket`, `asyncio`, `json`, `pathlib`, `hashlib` và các thư viện cần thiết;
  không dùng Flask, Django hoặc FastAPI.
- **BBR/CUBIC được triển khai ở mức đơn giản.** Cần mô tả đúng thuật toán và
  phần giản lược; không ghi là bản đầy đủ khi chưa triển khai đầy đủ.
- Có xử lý nhiều client bằng async I/O hoặc thread; tách network, xử lý dữ liệu
  và giao diện. Mục tiêu nghiệm thu theo đề là 10–20 client đồng thời.
- Không dùng WebSockets theo giới hạn networking trong đề. Thư viện giao diện
  không thay thế phần truyền UDP và cơ chế tin cậy nhóm phải tự viết.
- Giảng viên cho phép không triển khai mã hóa payload, nhưng vẫn cần các biện
  pháp bảo mật khác. AES-GCM có thể để sau; xác thực, kiểm tra phiên/quyền gửi,
  giới hạn đầu vào và bảo vệ đường dẫn file vẫn cần được thiết kế.
- Ghi rõ thiết kế và phạm vi tính năng nhóm lựa chọn theo hướng dẫn giảng viên.

Các tài liệu TODO, protocol và UI cũ có thể còn ghi TCP chưa chốt, giao diện
đang chờ xác nhận hoặc AES/TLS bắt buộc. Khi triển khai, cập nhật các nội dung
đó theo yêu cầu mới ở đây và lựa chọn thiết kế cụ thể của nhóm.

## Kiến trúc dự kiến

**Phương án triển khai đề xuất:** TCP cho kết nối ban đầu/đăng nhập, UDP cho
chat và file, HTTP cho giao diện. Đây là cách nhóm phân vai trò dựa trên yêu
cầu có cả TCP và UDP, không phải lời giảng viên quy định chi tiết từng kênh.

| Kênh | Giao thức và mục đích |
|---|---|
| Client Python ↔ server | TCP: handshake ứng dụng, đăng nhập và thiết lập phiên; cổng dự kiến 9000 |
| Client Python ↔ server | UDP: chat, file, ACK, heartbeat và cập nhật online; cổng dự kiến 9001 |
| Trình duyệt ↔ client Python cục bộ | HTTP trên TCP: phục vụ giao diện và gọi chức năng client; bridge chỉ nghe trên localhost, cổng riêng sẽ được cấu hình khi triển khai |

Luồng sử dụng dự kiến:

1. Trình duyệt mở giao diện do HTTP bridge cục bộ phục vụ.
2. Giao diện gửi yêu cầu đăng nhập đến bridge; client trao đổi với server qua TCP.
3. Sau khi xác thực, client gắn UDP endpoint với phiên theo giao thức chung.
4. Chat và chunk file được truyền qua UDP; transport xử lý ACK, gửi lại và cửa sổ.
5. Client nhận phân phối sự kiện để giao diện hiển thị tin nhắn, online, tiến độ
   và lỗi. Có thể lấy sự kiện qua HTTP polling hoặc long-polling.

UI ban đầu dùng HTML, CSS và JavaScript thuần để dễ đọc và dễ sửa. Có thể bổ
sung thư viện JavaScript khi cần. Ưu tiên đăng nhập, chat chung/riêng, danh sách
online, chọn file, tiến độ và trạng thái lỗi; phần trang trí làm sau.

| Thành phần | Vai trò |
|---|---|
| `common/` | Mô hình packet, cấu hình, codec và giao diện transport dùng chung |
| `server/` | Nhận kết nối TCP, quản lý người dùng/phiên và chuyển tiếp chat/file qua UDP |
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

## Congestion control ở mức đơn giản

Hướng đề xuất là **CUBIC đơn giản hóa**; Hải phụ trách chính, Khoa phối hợp
ghép client và đo kết quả. Chưa coi thuật toán này là đã có trong code.

- Theo dõi cửa sổ tắc nghẽn `cwnd` và lượng dữ liệu đang chờ ACK.
- Giữ ý tưởng tăng cửa sổ theo hàm bậc ba của thời gian và giảm khi phát hiện mất
  gói; có giới hạn cửa sổ và xử lý ACK trùng để tránh tính tăng nhiều lần.
- Phối hợp với giới hạn nhận do flow control cung cấp. Nhiều stream dùng chung
  đường truyền phải cùng chịu ngân sách gửi phù hợp.
- Thử dưới các mức mất gói/độ trễ, ghi log cửa sổ, số lần gửi lại và thời gian
  hoàn tất; báo cáo rõ phần đã làm và phần giản lược.

Cửa sổ cố định, chỉ có timeout/retry hoặc chỉ tăng tuyến tính rồi giảm một nửa
không đủ để gọi là CUBIC. Mức triển khai đơn giản giúp giảm phạm vi, nhưng vẫn
cần thể hiện cơ chế điều chỉnh lượng dữ liệu gửi theo tình trạng truyền.

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

Ở bản code được kiểm tra ngày 19/10/2026, hai lệnh `--help` chạy được, còn chạy
server/client thật kết thúc với thông báo chưa triển khai. Cổng TCP 9000 và UDP
9001 là cấu hình dự kiến của nhóm; sẽ bổ sung lệnh chạy thật sau khi ghép xong.

Các demo trên chỉ cần thư viện chuẩn. `requirements.txt` hiện có
`cryptography`; chỉ cần cài khi triển khai phần dùng thư viện này:

```bash
python -m pip install -r requirements.txt
```

## Kết quả kiểm tra đã ghi nhận

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
5. Tích hợp HTTP UI đơn giản; hoàn thiện các kiểm tra bảo mật, đo congestion
   control đơn giản hóa và thử 10–20 client.
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

Bài tham khảo **“Implement non-blocking HTTP server and chat application”**
(tháng 04/2026, nhóm 2, lớp CC02) hữu ích cho HTTP request/response, routing,
session và async I/O. Bài đó mô tả chat P2P qua **TCP**, không phải cách triển
khai UDP tin cậy của dự án này. Dùng báo cáo đó để học cách tổ chức HTTP và
ứng dụng; yêu cầu của nhóm được đối chiếu với đề và phản hồi giảng viên ở trên.

Khi nộp: chuẩn bị mã nguồn, README chạy được từ đầu và báo cáo PDF theo đề.
Đề cho phép nộp ZIP hoặc liên kết repo GitHub private; kiểm tra lại hình thức
nộp và trạng thái repo trước hạn.
