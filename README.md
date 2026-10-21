# UDP Stream Collab

Bài tập lớn **Distributed and Streaming-based Client-Server Collaboration
Application**: đăng nhập, xem người đang online, chat chung, chat riêng và
truyền file tin cậy qua UDP trong mạng LAN.

**Thành viên:** Nguyễn Việt Khoa và Hải.

**Cập nhật:** 21/10/2026.

**Định hướng:** làm đơn giản, bám đề và phản hồi giảng viên; ưu tiên chạy đúng,
kiểm thử được và giải thích được code.

> **Dự án đang phát triển.** Đã có model, mock, chia file và một số chức năng
> client/file local. Server TCP/UDP, transport tin cậy và HTTP bridge còn TODO.
> Hiện chạy được các demo bên dưới; chưa có ứng dụng chat/file hoàn chỉnh qua
> mạng thật. Phần “thiết kế” không đồng nghĩa với phần “đã triển khai”.

## 1. Yêu cầu BTL và phạm vi thực hiện

Nguồn: [đề BTL, 4 trang](<Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application (1).pdf>).
Các điều chỉnh từ giảng viên dưới đây là phản hồi do Khoa cung cấp, đã ghi nhận
trong README ngày 21/10/2026.

| Yêu cầu | Căn cứ trong đề | Cách thực hiện của nhóm |
|---|---|---|
| Handshake TCP | Bước 2 | Client kết nối TCP và nhận phản hồi server |
| Đăng nhập, quản lý online | Mục 4; bước 3 | Tài khoản demo tạo trước, session và địa chỉ client |
| Chat chung và chat riêng | Mục 4 nêu broadcast/global chat và unicast/private messaging; bước 5 nêu routing | Server chuyển tiếp đến các client online hoặc một người cụ thể |
| Truyền file qua UDP | Bước 6 | Chia chunk, truyền, ghép lại và kiểm tra toàn vẹn |
| Sequence, ACK, gửi lại, sliding window, flow control | Mục 6 | Tự triển khai trong transport dùng chung |
| Congestion control | Mục 6 | Chọn CUBIC giản lược theo phạm vi được cho phép |
| Đồng thời, tránh chặn giữa các stream | Mục 6–7; bước 4 | Async I/O, stream riêng và hàng đợi có giới hạn |
| 10–20 client đồng thời | Mục 7 | Kiểm thử online/chat và hoạt động đồng thời |
| Timeout, ngắt kết nối và lỗi mạng | Mục 4; bước 7 | Báo lỗi, dọn tài nguyên, không làm server crash |
| Bảo mật | Mục 4, 6; bước 8 | Theo phạm vi điều chỉnh; vẫn kiểm tra đăng nhập, phiên và đầu vào |
| HTTP/JavaScript UI | Bước 9 | HTML, CSS, JavaScript thuần và HTTP bridge cục bộ |
| Kiểm thử, chất lượng code, báo cáo | Bước 10; mục 10 | Kết quả thật, README chạy đúng và báo cáo PDF |

### Phản hồi giảng viên đã ghi nhận

- **Dùng cả TCP và UDP.** Nhóm chọn TCP cho handshake/đăng nhập, UDP cho chat/file.
- **Python được dùng thư viện nhưng không dùng framework.** Ưu tiên socket và
  thư viện chuẩn; không dùng Flask, Django hoặc FastAPI.
- **Không dùng WebSocket** theo giới hạn networking của đề.
- UI được dùng thư viện JavaScript; nhóm chọn **HTML/CSS/JavaScript thuần** để
  dễ làm và giải thích. Không cần trang trí cầu kỳ.
- **BBR/CUBIC được giản lược**, nhưng phải mô tả đúng cơ chế đã làm và đo kết quả.
- **Có thể không mã hóa payload**, nhưng vẫn cần các biện pháp bảo mật khác.
  Không suy ra rằng được bỏ toàn bộ bảo mật hoặc tự khẳng định đạt điểm bước 8.

Đề có chỗ mô tả TCP/UDP và CLI/GUI chưa thống nhất. Nhóm theo phản hồi đã ghi
nhận và các mốc chấm điểm: có handshake TCP, file qua UDP và giao diện HTTP/JS.
Việc phân TCP cho đăng nhập, UDP cho chat/file là thiết kế của nhóm.

### Phạm vi đơn giản

- Một server trung tâm; người dùng nhập địa chỉ server. Server cung cấp danh
  sách online, chưa thêm dò tìm server bằng broadcast LAN.
- Tài khoản demo tạo trước; không làm đăng ký, quên mật khẩu hoặc quản trị user.
- Có chat chung và chat riêng; không thêm phòng chat, đã đọc, đang gõ hoặc
  lịch sử được lưu sau khi đóng chương trình.
- Gửi file cho **một người nhận đang online**. Client nhận file **tự động** nếu
  metadata hợp lệ và có khả năng lưu; không cần người dùng bấm Accept/Reject.
- Giữ `FILE_ACCEPT` làm xác nhận kỹ thuật **“client đã sẵn sàng nhận”**, do
  chương trình tự gửi. Đây không phải bước xin phép người dùng.
- Session lưu trong RAM, file nhận lưu trên đĩa; không thêm database, cloud,
  audio/video hoặc resume file sau restart.
- Không bắt buộc thêm `MSG_DELIVERED`, API `Transport.release()` hay các lớp
  quản lý mới chỉ vì chúng từng xuất hiện trong bản đề xuất chi tiết.

**Đề yêu cầu chat chung; đề không yêu cầu nút chấp nhận file.** Tên các gói file,
SHA-256, cổng và kích thước chunk là lựa chọn triển khai của nhóm.

## 2. Kiến trúc tổng thể

```mermaid
flowchart LR
    UA["HTML/CSS/JS trên máy A"] <-->|HTTP localhost| CA["Client Python A"]
    CA <-->|"TCP: handshake, đăng nhập"| S["Server Python"]
    CA <-->|"UDP: chat, file, ACK"| S
    S <-->|"TCP: handshake, đăng nhập"| CB["Client Python B"]
    S <-->|"UDP: chat, file, ACK"| CB
    CB <-->|HTTP localhost| UB["HTML/CSS/JS trên máy B"]
```

| Thành phần/kênh | Vai trò |
|---|---|
| Server Python | Quản lý user/session/online và chuyển tiếp dữ liệu |
| Client Python | Kết nối server, xử lý chat/file, dùng transport, phát event cho UI |
| Browser | Hiển thị và nhận thao tác; không trực tiếp mở socket UDP |
| TCP server, mặc định 9000 | Handshake, đăng nhập, thiết lập phiên |
| UDP server, mặc định 9001 | Chat, file, ACK, heartbeat, cập nhật online |
| HTTP bridge, chỉ nghe `127.0.0.1` | Phục vụ UI và gọi client core; cổng sẽ cấu hình khi triển khai |

Mỗi người dùng chạy client Python và browser trên cùng máy. HTTP là cầu nối
cục bộ; client Python vẫn dùng TCP/UDP để trao đổi với server. Server chuyển
các chunk, không cần lưu nguyên file. Thử nhiều client trên một máy thì mỗi
client dùng cổng HTTP riêng.

UI dùng **HTTP polling** để nhận event trong bản đầu, chưa cần long-polling.
Network, xử lý dữ liệu và UI được tách riêng. Đọc/ghi file hoặc đợi ACK không
được làm đứng giao diện hay chặn các cuộc trò chuyện khác.

## 3. Cấu trúc code và phân công

| Thư mục | Trách nhiệm | Phụ trách chính |
|---|---|---|
| `common/` | Packet, codec, cấu hình và hợp đồng Transport | Hải; hai người thống nhất API |
| `server/` | TCP, auth, session, heartbeat, online, routing | Hải |
| `transport/` | UDP, sequence, ACK, retry, flow/congestion control | Hải |
| `client/` | Login, chat, chia/ghép file và event | Khoa |
| `ui/` | HTTP bridge, HTML/CSS/JavaScript | Khoa |
| `security/` | Khung bảo mật theo phạm vi thực tế đã chọn | Cả hai phối hợp |
| `mocks/`, `scripts/` | Mock và demo phát triển | Cả hai |
| `docs/`, `reports/` | Đặc tả, kiểm thử và báo cáo | Cả hai |
| `data/` | Dữ liệu thử và file nhận | Dữ liệu cục bộ |

Trong comment/TODO cũ: **A / Bạn A là Hải**, **K / Khoa là Khoa**. Giữ cấu trúc
hiện có khi vẫn đủ rõ; không cần tách thêm nhiều module chỉ để hoàn thiện sơ đồ.
Hai người dùng chung [Packet](common/models.py) và [Transport](common/contracts.py).

| API Transport | Hợp đồng |
|---|---|
| `await start()` | Mở tài nguyên nhận/gửi |
| `await send(peer, packet)` | Bản thật chờ ACK chặng kế tiếp hoặc báo lỗi; mock chỉ đưa vào queue |
| `await receive()` | Trả ReceivedPacket; transport tự xử lý ACK, dedup và thứ tự |
| `await close()` | Đóng tài nguyên, phối hợp dừng task nhận/gửi |

`peer` là địa chỉ socket chặng kế tiếp, còn `packet.recipient` là người nhận
nghiệp vụ. Client gửi đến server; server tra phiên để chọn đích chuyển tiếp.

## 4. Luồng hoạt động

Các luồng dưới đây là hướng triển khai; mục 7 ghi riêng phần đã có code.

### Đăng nhập và online

1. Người dùng nhập server, username và password trên UI.
2. Client kết nối TCP, handshake và gửi yêu cầu đăng nhập.
3. Server kiểm tra tài khoản, từ chối thông tin sai hoặc username đang sử dụng.
4. Server tạo session; client dùng `BIND` để gắn UDP endpoint với phiên hợp lệ.
5. Sau khi thiết lập thành công, client xuất hiện trong danh sách online và
   được gửi/nhận dữ liệu nghiệp vụ.
6. Heartbeat cập nhật hoạt động. Logout hoặc hết timeout làm server xóa phiên
   và gửi danh sách online mới cho các client còn lại.

TCP dùng prefix độ dài 4 byte big-endian và body JSON UTF-8. Phải đọc đủ prefix
và body, giới hạn kích thước và xử lý EOF; không coi một lần `recv()` là một
thông điệp hoàn chỉnh.

### Chat

1. UI gửi `{recipient, text}` cho client Python.
2. Client kiểm tra nội dung, tạo `MSG` và gửi đến server.
3. Server xác định sender từ phiên hợp lệ, kiểm tra người nhận rồi chuyển tiếp.
4. `recipient="*"`: gửi đến các client online khác; người gửi hiển thị bản tin
   của mình. Username cụ thể: chỉ gửi đến người đó.
5. Client nhận phát event `message`; UI hiển thị nội dung bằng `textContent`.

Tin nhắn giới hạn **480 byte UTF-8**, không phải 480 ký tự. ACK từ server chỉ
xác nhận chặng đến server, không chứng minh người nhận đã nhận/đọc. Nếu đích
không tồn tại hoặc chuyển tiếp thất bại, server báo lỗi về người gửi. Một đích
chậm không được chặn việc chuyển tin đến các đích khác.

### File tự động nhận

```text
Client gửi                 Server                  Client nhận
    |--- FILE_META ---------->|--- FILE_META ---------->|
    |                         |          kiểm tra metadata,
    |                         |          chuẩn bị file .part
    |<-- FILE_ACCEPT ---------|<-- FILE_ACCEPT ---------|
    |--- FILE_CHUNK ---------->|--- FILE_CHUNK --------->|
    |          nhiều chunk; mỗi chặng có ACK/retry      |
    |--- FILE_END ----------->|--- FILE_END ----------->|
    |                         |          kiểm tra size/hash,
    |                         |          đổi tên file tạm
    |<-- FILE_COMPLETE -------|<-- FILE_COMPLETE -------|
```

- Metadata gồm tên, số byte, SHA-256, kích thước chunk và tổng số chunk.
- Client nhận kiểm tra và chuẩn bị nơi lưu rồi **tự trả FILE_ACCEPT**. Metadata
  sai, vượt giới hạn hoặc không lưu được thì trả `ERROR`; không hiện hộp Accept.
- Mỗi file có transfer ID riêng; chunk đánh chỉ số từ 0.
- Ghi vào `.part`, xử lý chunk trùng/sai thứ tự; chỉ đổi tên khi đủ dữ liệu và
  size/hash đúng. File rỗng có 0 chunk nhưng vẫn có META/ACCEPT/END/COMPLETE.
- Sender chỉ báo **hoàn tất** sau `FILE_COMPLETE` hợp lệ từ người nhận. Đủ ACK
  đến server thì vẫn đang chờ xác nhận cuối; tiến độ phải phản ánh đúng ý nghĩa đó.
- Lỗi/timeout phải báo thất bại, giải phóng tác vụ và dọn file tạm theo chính
  sách; không báo thành công giả hoặc chờ vô hạn.

## 5. UDP tin cậy và xử lý đồng thời

Đây là phần cốt lõi cần giữ đầy đủ dù nghiệp vụ và giao diện được giản lược.

| Cơ chế | Cần đạt |
|---|---|
| Sequence | Theo từng chặng, phiên, hướng và stream; không dùng một dãy cho tất cả |
| ACK | Xác nhận gói được nhận/giữ hợp lệ; ACK không đòi ACK tiếp |
| Retry | Gửi lại gói chưa ACK khi timeout, có giới hạn số lần |
| Dedup/order | ACK lại gói trùng nhưng chỉ giao nghiệp vụ một lần; giữ thứ tự trong stream |
| Sliding window | Cho phép nhiều gói đang chờ ACK, số lượng hữu hạn |
| Flow control | Sender tuân khả năng nhận để không làm tràn buffer |
| Congestion control | Điều chỉnh lượng dữ liệu gửi theo tình trạng đường truyền |
| Stream riêng | Chat, điều khiển và mỗi file không phải đợi sequence của nhau |

Chọn hướng **Selective Repeat** cho gửi lại và **CUBIC giản lược** cho congestion.
CUBIC cần thể hiện cửa sổ tăng theo hàm bậc ba của thời gian, phản ứng loss và
xử lý timeout. Fixed window hoặc chỉ retry chưa phải CUBIC. Ghi rõ phần giản
lược và đo kết quả; tham khảo [RFC 9438](https://www.rfc-editor.org/rfc/rfc9438.html).

Các điểm cần chốt khi ghép:

- `Packet` hiện chưa có trường receiver window. Cần thống nhất thông tin báo
  khả năng nhận và cách phục hồi khi bản cập nhật cửa sổ bị mất; không bỏ flow control.
- Một vòng nhận ở mỗi client dispatch chat/user/file/error. Không để từng tác
  vụ chờ file tự tranh `receive()` rồi lấy mất gói của nhau.
- `send()` thật chờ ACK; gọi tuần tự từng chunk sẽ thành stop-and-wait. Dùng số
  tác vụ gửi có giới hạn, không tạo task cho toàn bộ file lớn.
- Đọc/ghi/băm file, queue UI và queue chuyển tiếp không được chặn vòng nhận mạng.
  Giới hạn buffer/task và có cách xử lý khi đầy; không ACK dữ liệu đã bị bỏ.
- Nhiều stream cùng peer dùng chung ngân sách congestion phù hợp. ACK trùng
  không làm tăng progress hoặc cửa sổ nhiều lần.

Cấu hình trong [common/config.py](common/config.py) là giá trị khởi đầu của nhóm,
chưa được tối ưu qua đo và không phải ngưỡng bắt buộc của đề:

| Cấu hình | Giá trị |
|---|---|
| TCP / UDP server | 9000 / 9001 |
| Datagram tối đa | 1200 byte |
| Body trước envelope/mã hóa dự kiến | 1100 byte |
| Payload/chunk tối đa | 480 byte |
| Record TCP tối đa | 64 KiB |
| Heartbeat / offline timeout | 3 giây / 10 giây |
| Retry timeout ban đầu / retry tối đa | 0,5 giây / 8 lần |
| Cửa sổ khởi đầu | 8 packet |

Codec phải đo byte sau serialize và phần bao ngoài thực tế. Payload nhỏ không
tự bảo đảm cả datagram vừa giới hạn. Metadata, user list và lỗi cũng phải vừa
gói; danh sách online cần chia trang khi vượt giới hạn.

## 6. Bảo mật trong phạm vi đơn giản

Mã hóa payload được để ngoài phạm vi đầu theo phản hồi đã ghi nhận. Các phương
án TLS/HMAC/AES-GCM trong tài liệu đề xuất trước đây **không tự trở thành yêu cầu
bắt buộc** của bản đơn giản này. Nếu chọn thêm thì phải triển khai, kiểm thử và
mô tả đúng phạm vi bảo vệ.

Những việc vẫn cần làm:

- Kiểm tra tài khoản; không lưu mật khẩu plaintext trong file tài khoản, dùng
  hàm băm mật khẩu có salt bằng thư viện phù hợp. Không log password/khóa.
- Session ID khó đoán, có vòng đời; gắn UDP endpoint với phiên đăng nhập hợp lệ.
  Server kiểm tra phiên/quyền gửi, không tin `sender` tự khai.
- Giới hạn kích thước, kiểm tra kiểu/loại packet, recipient và lượng tài nguyên.
- Tên file không được ghi ra ngoài thư mục nhận hoặc ghi đè file có sẵn.
- HTTP bridge chỉ nghe localhost, kiểm tra truy cập API cục bộ và giới hạn upload.
- Lỗi ghi, hash sai hoặc timeout phải có thông báo và giải phóng tài nguyên.

Session ID, kiểm tra endpoint và SHA-256 **không thay thế mã hóa/xác thực mật mã**.
Nếu chưa bảo vệ kênh đăng nhập hoặc xác thực packet bằng mật mã thì ghi rõ giới
hạn; không tuyên bố chống nghe lén hay giả mạo chủ động. Phần bảo mật hiện chưa
được nghiệm thu.

## 7. Tiến độ thực tế

Đối chiếu ngày **21/10/2026**, GitHub `main` ở commit
[`463c8b3`](https://github.com/nvkhoa1207/udp-stream-collab/commit/463c8b30ab245cd0b7b7049450d639e5708a9a73).
Bản local có các thay đổi chưa được commit; clone GitHub tại mốc này chưa có
đầy đủ code trên máy Khoa.

| Thành phần | GitHub tại commit trên | Local đã kiểm tra |
|---|---|---|
| Packet, MessageType, ReceivedPacket, Transport | Có | Có |
| MemoryHub, MemoryTransport | Có | Có |
| `iter_chunks()`, `build_metadata()` | Có | Có |
| FileReceiver begin/accept_chunk/finish | TODO | Đã ghép chunk, bỏ trùng, kiểm tra size/hash |
| ClientService send_chat | TODO | Đã tạo/gửi MSG qua transport |
| ClientService receive_events | TODO | Đã xử lý MSG/event queue; chưa đủ user/file/error |
| ClientService login/send_file | TODO | TODO |
| Codec, server/auth/session/router | TODO | TODO |
| ReliableUdpTransport, CongestionController | TODO | TODO |
| `ui/index.html`, `ui/app.js` | Fixture, nút disabled | Fixture, nút disabled |
| HTTP bridge, PayloadCipher | TODO | TODO |
| demo_mock, demo_file_chunks | Có | Có |
| demo_client_chat, demo_file_receiver | Chưa có | Có, chưa commit |

FileReceiver/chat local mới chứng minh xử lý cơ bản, chưa đủ để đánh dấu hoàn
tất toàn bộ K2/K3/K4. Bản xem thử giao diện trong `output/` cũng không phải UI
đã nối vào ứng dụng.

## 8. Cài đặt và chạy phần hiện có

Yêu cầu **Python 3.11+** và Git. Chạy lệnh ở thư mục gốc project. Các demo hiện
có chỉ dùng thư viện chuẩn, chưa cần `cryptography`.

### Lấy code và tạo môi trường

Nếu chưa có repo:

```bash
git clone https://github.com/nvkhoa1207/udp-stream-collab.git
cd udp-stream-collab
```

Nếu đã có repo, mở terminal ở thư mục hiện có; không clone đè hoặc reset phần
đang làm để lấy tài liệu mới.

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python --version
```

Windows PowerShell:

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python --version
```

Nếu PowerShell không cho chạy script activate, dùng trực tiếp
`.venv\Scripts\python.exe` thay cho `python` trong các lệnh demo.

`requirements.txt` hiện có `cryptography` cho cipher dự kiến. Chỉ cài khi triển
khai/chạy chức năng sử dụng thư viện đó:

```bash
python -m pip install -r requirements.txt
```

### Demo có trong commit GitHub đã kiểm tra

```bash
python -m scripts.demo_mock
python -m scripts.demo_file_chunks
```

| Demo | Kết quả mong đợi | Phạm vi |
|---|---|---|
| demo_mock | In `OK hợp đồng mock` | Chuyển Packet qua queue trong một process; không gọi ClientService |
| demo_file_chunks | Chunk 480, 480, 40 byte; `Hash giống nhau: True` | Chia/ghi nối tiếp file 1000 byte; không gọi FileReceiver |

`demo_file_chunks` tạo/ghi đè `data/demo_original.bin` và `data/demo_copy.bin`.
Đây là file thử của demo, không dùng chúng để giữ dữ liệu riêng.

### Demo bổ sung ở local

Chỉ chạy khi có hai script và phần client/file tương ứng. Tại mốc GitHub ở
mục 7, hai script này chưa được commit nên chưa có khi clone repo.

```bash
python -m scripts.demo_client_chat
python -m scripts.demo_file_receiver
```

| Demo | Kết quả mong đợi | Phạm vi |
|---|---|---|
| demo_client_chat | friend nhận tin từ khoa | Gọi send_chat, receive_events và event queue; server vẫn chuyển tiếp giả |
| demo_file_receiver | Thứ tự `2, 0, 0, 1`, có 3 chunk khác nhau; hash khớp | Dùng FileReceiver để thử chunk trùng/sai thứ tự trên đĩa |

`demo_file_receiver` tạo/ghi đè `data/demo_original.bin`, lưu kết quả ở
`data/received/`, mỗi lần chạy dùng transfer ID mới. Cả bốn demo **không mở
socket TCP/UDP**, không kiểm tra mất gói hoặc chứng minh truyền qua LAN.

### Entrypoint server/client

Hiện chỉ xem được tham số:

```bash
python -m server.main --help
python -m client.main --help
```

Chạy server hoặc client thật hiện kết thúc với exit code 2 và thông báo chưa
triển khai. Chưa có tài khoản đăng nhập thật, HTTP bridge đang nghe hoặc lệnh
chạy ứng dụng hoàn chỉnh để hướng dẫn. Khi ghép xong, mục này phải được thay
bằng lệnh chạy thật, cách tạo tài khoản demo, địa chỉ UI và cách thử hai máy LAN
đã được kiểm chứng.

## 9. Kiểm thử và nghiệm thu

### Kết quả đã kiểm tra ngày 21/10/2026

- Cú pháp 28 file Python: đạt.
- Bốn demo local ở mục 8: chạy thành công.
- File 1000 byte chia 480/480/40 byte: hash khớp.
- FileReceiver nhận `2, 0, 0, 1`: có 3 chunk khác nhau, hash khớp.
- Hai lệnh `--help`: chạy được; hai entrypoint thực tế vẫn báo chưa triển khai.

Đây là kiểm tra local trên macOS, không phải nghiệm thu mạng hoặc xác nhận
mọi hệ điều hành. Không suy rộng thành đã đạt tất cả kiểm tra đầu vào, bảo mật
hoặc file lớn.

### Những kiểm thử cần hoàn thành

| Nhóm | Tình huống | Kết quả cần đạt |
|---|---|---|
| Đăng nhập/phiên | Đúng/sai mật khẩu, trùng username, ngắt kết nối | Trạng thái/lỗi đúng; server tiếp tục chạy |
| Online/chat | 3 client, chat chung/riêng | Online cập nhật, private không đến người thứ ba, không nhân đôi tin |
| File | File rỗng, nhị phân, tên tiếng Việt, 1 KiB/1 MiB/10 MiB | Nhận tự động, đúng byte/size/hash, chỉ hoàn tất sau xác nhận cuối |
| Reliability | Mất DATA và ACK, gói trùng/sai thứ tự | Retry đúng, không hỏng dữ liệu, hết retry có lỗi rõ |
| Flow/congestion | Receiver chậm, loss/delay, ACK trùng | Buffer hữu hạn, cửa sổ đúng, log thể hiện thuật toán đã chọn |
| Đồng thời | Chat khi file gặp loss; 10 rồi 20 client | Stream tiếp tục hoạt động, đo latency và tài nguyên thực tế |
| Lỗi/bảo mật | Session sai, gói quá lớn, tên chứa `../`, thiếu chunk/hash sai, lỗi ghi | Từ chối phù hợp; không ghi ra ngoài hoặc báo thành công giả |
| UI | Login, online, chat, chọn file, progress và lỗi | Thao tác được, phản ánh core, không đứng khi truyền file |
| Cài đặt | Chạy từ README trên máy còn lại | Làm được từ đầu, không cần chỉ dẫn ngoài README |

Mất gói 5–10%, độ trễ và các kích thước file trên là kịch bản nhóm chọn, không
phải ngưỡng điểm của đề. Dùng socket thật, fault injection có seed; lưu cấu hình,
hash, log và PASS/FAIL. Cập nhật [bảng nghiệm thu](docs/04_ACCEPTANCE.md) bằng kết
quả đã chạy. Các mục AES/tag chỉ áp dụng khi có cơ chế tương ứng, không tự biến
phần mã hóa đã được giản lược thành bắt buộc trở lại.

## 10. Thứ tự triển khai tiếp theo

1. Đồng bộ client/file và hai demo local để cả hai dùng cùng bản nền.
2. Chốt codec, session/BIND, thông tin cửa sổ nhận và API chung; ưu tiên giữ
   cấu trúc hiện có, chỉ thêm trường/hàm khi cơ chế thực tế cần.
3. Ghép handshake TCP, đăng nhập, online và chat chung/riêng qua UDP thật.
4. Hoàn thiện ACK/retry/window và file tự động từ META đến COMPLETE.
5. Ghép flow/congestion control, các stream độc lập và HTTP UI đơn giản.
6. Nghiệm thu, sửa lỗi, đo 10–20 client, hoàn thiện hướng dẫn chạy và báo cáo.

Đây là thứ tự phát triển, không thay thế yêu cầu cuối cùng. UDP chưa có đủ
reliability/flow/congestion control chỉ là bản trung gian, chưa phải bản đủ để nộp.

## 11. Một số tình huống khi chạy demo

| Hiện tượng | Cách kiểm tra |
|---|---|
| `No module named scripts...` | Chạy ở thư mục gốc bằng `python -m ...`; kiểm tra hai demo local đã được đồng bộ chưa |
| `NotImplementedError` | Đã gọi phần TODO; xem tiến độ, không coi là lỗi cài thư viện |
| Server/client thoát ngay | Entrypoint chưa triển khai; `--help` chỉ hiển thị tham số |
| UI có dữ liệu mẫu, nút disabled | Fixture chưa có bridge và chưa nối core |
| File thử thay đổi sau demo | Hai demo file có tạo/ghi đè demo_original.bin, như ghi ở mục 8 |

Không cần mở firewall hoặc đổi thiết lập mạng để chạy demo bộ nhớ. Hướng dẫn
cổng/firewall cho LAN sẽ bổ sung cùng lệnh chạy mạng thật sau kiểm thử.

## 12. Tài liệu và bản nộp

| Tài liệu | Nội dung |
|---|---|
| [Giao thức](docs/01_PROTOCOL.md) | Packet và luồng truyền đề xuất |
| [API chung](docs/02_API.md) | Hợp đồng client/server/transport |
| [Quy trình Git](docs/03_GIT_WORKFLOW.md) | Nhánh và cách ghép code |
| [Nghiệm thu](docs/04_ACCEPTANCE.md) | Các trường hợp kiểm tra và bằng chứng |
| [TODO Hải](TODO_BAN_A.md) | Server và transport |
| [TODO Khoa](TODO_KHOA.md) | Client, file và UI |
| [Kế hoạch ban đầu](TODO_5_NGAY.md) | Lịch nội bộ, không phải thời hạn đề quy định |
| [Định hướng UI](ui/README.md) | HTTP bridge và giao diện |
| [Dàn ý báo cáo](reports/REPORT_OUTLINE.md) | Nội dung báo cáo PDF |

**Phạm vi trong README này là định hướng mới nhất của nhóm.** Các TODO, protocol,
tài liệu UI và bản đề xuất `docs/05_THIET_KE_TONG_THE.md` có thể còn nội dung cũ
hoặc mở rộng: Accept/Reject thủ công, TLS/HMAC/AES bắt buộc, receipt chat hoặc
module/API bổ sung. Những phần đó chưa được coi là quyết định triển khai. Khi
sửa module liên quan, cập nhật tài liệu theo phạm vi ở đây; `FILE_ACCEPT` được
hiểu là xác nhận tự động của chương trình.

Theo đề, nộp ZIP hoặc liên kết GitHub private, chứa:

1. Source client/server và các thành phần cần để chạy.
2. README hướng dẫn cài đặt, chạy server, kết nối client và demo chức năng.
3. Báo cáo PDF: kiến trúc, đặc tả giao thức, sequence diagram cho chat/file và
   khoảng một trang **Challenges & Solutions**. Bổ sung phân công và kết quả
   kiểm thử thật để giải thích phần đã làm.

Trước khi nộp, thử lại trên máy còn lại và kiểm tra hình thức nộp/trạng thái repo.
Không đưa `.venv`, dữ liệu cá nhân, mật khẩu, private key hoặc khóa phiên vào
bản nộp. Báo cáo ghi đúng tính năng đã chạy và giới hạn còn lại.
