# BTL Mạng máy tính — bộ khung làm song song trong 5 ngày

**Đây là bộ khung để hai bạn tự code, chưa phải ứng dụng hoàn chỉnh để nộp.**
Đã có: chia việc, lịch 5 ngày, đặc tả nháp, kiểu dữ liệu/API chung, demo truyền
trong bộ nhớ và vị trí TODO. Chưa có: socket thật, đăng nhập thật, UDP tin cậy,
truyền file thật, mã hóa, giao diện kết nối mạng hay kết quả kiểm thử mạng.

## Bắt đầu trong 30 phút

1. Đọc `TODO_5_NGAY.md`, cùng xem `docs/01_PROTOCOL.md` và `docs/02_API.md`.
2. Tạm chọn **Python 3.11+**. Bạn A làm server/transport; **Khoa** làm
   client/file/security/UI. Có thể đổi người, giữ nguyên ranh giới module.
3. Bạn A mở `TODO_BAN_A.md`. Khoa mở `TODO_KHOA.md`.
4. Mở cả thư mục này trong VS Code. Tìm `TODO[` để đến từng vị trí cần code.
5. Chạy demo giả dưới đây để hiểu giao diện gọi. Không cần đợi server hoàn tất.

## Chuẩn bị và chạy phần đã có

Thực hiện tại thư mục chứa README này, trên macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m scripts.demo_mock
python -m server.main --help
python -m client.main --help
```

Windows PowerShell: kích hoạt bằng `.venv\Scripts\Activate.ps1`.
Demo hiện tại chỉ dùng thư viện chuẩn. Khi bắt đầu phần mật mã:

```bash
python -m pip install -r requirements.txt
```

`requirements.txt` mới liệt kê thư viện cần dùng; sau khi hai máy cài/chạy thành
công, nhóm thống nhất và khóa phiên bản đã kiểm tra. Không cài Flask, Django,
Socket.IO hoặc WebSockets cho bài này.

## Thư mục và người phụ trách

| Đường dẫn | Nội dung | Owner |
|---|---|---|
| `common/` | Kiểu gói, cấu hình, codec, giao diện transport | Chốt chung ngày 1; A làm codec |
| `server/` | Kết nối, đăng nhập, session, online, định tuyến | A |
| `transport/` | UDP, sequence, ACK, retransmit, window, congestion | A |
| `client/` | Kết nối client, chat, chia/ghép file, sự kiện UI | Khoa |
| `security/` | Mã hóa/giải mã; phối hợp A về TLS và cấp khóa | Khoa |
| `ui/` | Giao diện web/cầu nối, chờ xác nhận cách triển khai | Khoa |
| `mocks/` | Transport trong bộ nhớ để làm song song | Dùng chung |
| `docs/` | Giao thức, API, cách ghép, tiêu chí nghiệm thu | Cả hai |
| `reports/` | Dàn ý báo cáo và chỗ lưu bằng chứng | Cả hai |
| `data/` | Dữ liệu chạy cục bộ, không commit file nhận/khóa/mật khẩu | Mỗi máy |

## Làm song song như thế nào?

Khoa viết chức năng dựa trên `Transport` trong `common/contracts.py`; trước mắt
truyền vào `MemoryTransport`. A triển khai `ReliableUdpTransport` với **cùng
API**. Khi ghép, đổi transport được truyền vào, giữ phần xử lý chat/file.

Mock hiện tại chỉ thay đường vận chuyển dữ liệu, không giả vờ cung cấp đăng nhập
hay tự hoàn thiện service. Khoa cần tự tạo fixture/sự kiện để phát triển UI.
`scripts/demo_mock.py` minh họa hai client đi qua server giả, không mở cổng mạng.

## Ba điều phải xác nhận với giảng viên ngay ngày 1

- TCP cho handshake/đăng nhập, UDP cho chat/file có phù hợp không?
- Mục 9 có bắt buộc HTTP UI với JavaScript không? Có được dùng chương trình
  Python cục bộ làm cầu nối HTTP–UDP, không dùng framework/WebSocket không?
- BBR/CUBIC cần triển khai đến mức nào? Nhóm có thể chọn một thuật toán không?

Trong lúc chờ trả lời vẫn làm common, core client/server, truyền file và kiểm thử.
Không tự bỏ UI web hoặc congestion control rồi coi là hoàn thành đủ yêu cầu.
5 ngày là lịch gấp: ưu tiên luồng hoạt động đầu-cuối; chỉ giảm phần trang trí và
tính năng ngoài đề. Không có cam kết đủ điểm trước khi chạy nghiệm thu thực tế.

## Các lệnh dự kiến sau khi tự hoàn thiện TODO

```bash
python -m server.main --host 0.0.0.0 --tcp-port 9000 --udp-port 9001
python -m client.main --host 127.0.0.1 --tcp-port 9000 --udp-port 9001 --username khoa
```

Hiện tại các lệnh này báo rõ chưa triển khai; chỉ `--help` và demo giả chạy được.
Không đưa mật khẩu vào tham số dòng lệnh. Khi thử LAN, thay host phía client bằng
IP máy server. Các cổng ở trên là lựa chọn của nhóm, không phải quy định của đề.

## Nộp bài

Nộp mã nguồn, README chạy thật và PDF theo `reports/REPORT_OUTLINE.md`.
ZIP này là starter, chưa phải bản nộp. Đối chiếu `docs/04_ACCEPTANCE.md` trước
khi chốt. Đề gốc: “Assignment 1 - Distributed Streaming-based Client-Server
Collaboration Application”, 4 trang; các quyết định trong docs là đề xuất của
nhóm và phải cập nhật theo phản hồi giảng viên.
