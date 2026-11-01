# Mã hóa TCP/UDP bằng AES-256-GCM

Cập nhật 01/11/2026. Phần này thay thế mô tả “chưa mã hóa” trong các báo cáo
bàn giao ngày 28–29/10. Mục tiêu: thực hiện bước 8 của đề bằng thư viện mật mã
chuẩn, giữ nguyên giao diện, Packet, ACK/retry, routing và xử lý file.

## 1. Cách chạy trên macOS/Linux

Trong thư mục dự án, chuẩn bị một lần:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m security.cipher --generate-key shared.key
```

Nếu đã có `.venv`/`shared.key`, dùng lại. Lệnh tạo khóa từ chối ghi đè. Khóa là
32 byte nhị phân ngẫu nhiên, không phải mật khẩu đăng nhập. Không sửa file bằng
editor, không đưa lên Git, ảnh chụp, log hoặc báo cáo. `.gitignore` và
`.dockerignore` loại `*.key`; image Docker không chứa khóa.

Mở ba Terminal trong thư mục repo:

```bash
# Terminal 1: đặt mật khẩu demo cho hai/khoa khi được hỏi.
.venv/bin/python -m server.main --host 127.0.0.1 --tcp-port 9000 --udp-port 9001
```

```bash
# Terminal 2
.venv/bin/python -m client.main --http-port 8000 --received-dir data/received-hai
```

```bash
# Terminal 3
.venv/bin/python -m client.main --http-port 8001 --received-dir data/received-khoa
```

Browser mở http://127.0.0.1:8000 và http://127.0.0.1:8001. Form vẫn nhập server
127.0.0.1, TCP 9000 và mật khẩu đã đặt. Không nhập khóa mã hóa trong browser.
Không mở cổng TCP 9000 bằng browser.

Mặc định chương trình đọc `shared.key` trong thư mục làm việc. Có thể đặt
`BTL_KEY_FILE=/đường/dẫn/shared.key` cho từng tiến trình. Khóa được cache trong
RAM; đổi khóa phải dừng và khởi động lại server/các client.

## 2. Windows và LAN hai máy

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m security.cipher --generate-key shared.key
```

Thay `.venv/bin/python` bằng `.\.venv\Scripts\python.exe` trong các lệnh chạy.
Nếu cần đường dẫn khác: `$env:BTL_KEY_FILE = 'C:\duong-dan\shared.key'`.

LAN: chỉ tạo khóa một lần rồi chuyển **cùng file khóa** riêng cho máy còn lại
qua kênh tin cậy (ví dụ USB). Không tạo khóa độc lập trên mỗi máy. Server chạy
`--host 0.0.0.0`; client nhập IP LAN của server. Cần cùng phiên bản code mới.
Bản cũ gửi plaintext không tương thích và bị từ chối.

Thiếu khóa hoặc file không đúng 32 byte: CLI báo lỗi trước khi chạy. Sai khóa
32 byte: giải mã/xác thực thất bại, không đăng nhập được. Kiểm tra cùng file và
khởi động lại các tiến trình; không bật fallback plaintext để xử lý lỗi.

## 3. Docker và netem

Tạo `shared.key` trên host trước. Compose mount read-only file này vào server,
không copy vào image. Dừng server cũ đang chiếm cổng 9000/9001 trước khi chạy:

```bash
docker compose --profile test config --quiet
docker compose build
docker compose run --rm --service-ports --name btl-server server
```

Hai client trên host dùng cùng `shared.key`, đăng nhập vào 127.0.0.1:9000.
Compose vẫn chỉ publish localhost, không tự mở server Docker ra LAN.

```bash
docker compose --profile test run --rm netem
```

Netem tự tạo khóa thử tạm trong container, dùng cho server/client của harness;
không dùng hoặc thay khóa thật trên host. Chỉ áp lỗi lên interface lo của container.

## 4. Luồng mã hóa và định dạng

```
Ứng dụng → serialize JSON → AES-GCM encrypt → socket
socket → AES-GCM decrypt/verify → parse/validate JSON → ứng dụng
```

- `security/cipher.py`: gọi `AESGCM.encrypt/decrypt` của cryptography; chuyển
  `InvalidTag` thành lỗi bị lớp nhận từ chối. Không tự triển khai AES.
- `security/wire.py`: nạp khóa, tạo nonce và bọc/mở envelope.
- UDP: mã hóa **toàn bộ JSON packet**, bao gồm payload và header bên trong,
  cả BIND, ACK, heartbeat, chat, file và control. Không để payload base64 rõ
  trên đường truyền. Server giải mã rồi mã hóa lại khi relay.
- TCP: mã hóa mọi JSON record, kể cả HELLO, AUTH/password và AUTH_OK/session.
  `client/auth.write_record()` dùng cùng codec với server.
- HTTP browser–bridge vẫn chỉ localhost; lớp mã hóa này áp dụng client–server.

Envelope nhị phân:

| Thành phần | Số byte |
|---|---:|
| Magic `BTL1` | 4 |
| Nonce ngẫu nhiên từ `os.urandom` | 12 |
| Ciphertext | Bằng độ dài plaintext |
| Authentication tag | 16 |

AAD là `BTL1UDP` hoặc `BTL1TCP`, ngăn một ciphertext UDP được chấp nhận như TCP.
AAD được xác thực nhưng không cần giữ bí mật. Nonce không bí mật, gửi cùng gói;
khóa phải giữ bí mật. Nonce ngẫu nhiên 96-bit làm xác suất trùng rất nhỏ trong
phạm vi lab, không phải chứng minh tuyệt đối không trùng. Tạo nonce mới cả khi
retry; không dùng seq làm nonce vì seq lặp giữa phiên/luồng/hướng/client.

Overhead là 32 byte. UDP JSON tối đa 1100 → envelope tối đa 1132, dưới 1200.
CUBIC tính cả overhead. Payload/chunk vẫn tối đa 480 byte. TCP prefix 4 byte
big-endian ghi độ dài **envelope mã hóa**; envelope tối đa 65536, JSON tối đa
65504 byte. TCP vẫn đọc đủ prefix/body và có timeout trước khi giải mã.

Sai khóa, sai tag, thay nonce/ciphertext/AAD hoặc gói plaintext đều bị từ chối
trước dispatch. Không có khóa mặc định hard-code hoặc chế độ không mã hóa.
Mock MemoryTransport không mở socket nên không đi qua lớp mã hóa.

## 5. Giới hạn phải trình bày khi bảo vệ

- Khóa chia sẻ trước dành cho nhóm tin cậy. Ai có khóa đều có khả năng giải mã
  lưu lượng thu được và tạo ciphertext hợp lệ. Không khẳng định bảo vệ trước
  thành viên nội bộ biết khóa hoặc xác thực danh tính riêng bằng AES-GCM.
- Server thấy dữ liệu để relay; đây không phải mã hóa đầu-cuối (E2EE).
- Không có TLS, trao đổi/luân chuyển khóa tự động hoặc forward secrecy. Lộ khóa
  có thể làm lộ cả bản ghi lưu lượng cũ; phải phân phối khóa mới ngoài ứng dụng.
- AES-GCM tự nó không chống replay. Chưa thêm replay cache mật mã; vẫn dùng
  session, endpoint, sequence/dedup và trạng thái giao thức hiện có. Không
  khẳng định chống phát lại toàn diện, đặc biệt với record TCP/unsequenced.
- Không che địa chỉ IP, cổng, độ dài/timing gói; không mã hóa file đã lưu trên đĩa.
- PBKDF2+salt vẫn phục vụ mật khẩu lưu trữ; SHA-256 vẫn kiểm tra file hoàn chỉnh.
  Hai cơ chế đó không thay AES-GCM và không bị bỏ khi thêm mã hóa.

## 6. Kiểm thử

Sau khi cài dependency và tạo khóa thử:

```bash
BTL_NETWORK_TEST=1 .venv/bin/python -m unittest discover -s tests -q
.venv/bin/python -m scripts.verify_demos
.venv/bin/python -m scripts.network_acceptance --clients 20 --sizes 0,1024,1048576,10485760,16777216 --streams 140
.venv/bin/python -m scripts.network_acceptance --clients 3 --sizes 65536 --streams 2 --loss 0.07 --delay 0.006 --seed 7
```

`tests/test_encryption.py` kiểm tra socket write nhận ciphertext, nonce retry
khác nhau, round-trip file/password, giới hạn kích thước, thiếu/sai khóa,
sửa tag/nonce/ciphertext, AAD khác kênh và từ chối plaintext trước dispatch.
Bộ E2E cũ tiếp tục kiểm tra chat/file/hash/cancel/logout/reconnect trên socket
thật, nay tất cả đều chạy qua lớp mã hóa.

Nguồn API: https://cryptography.io/en/stable/hazmat/primitives/aead/

## Kết quả kiểm tra 01/11/2026 — mã hóa bật

Môi trường: macOS, Python 3.13.3, cryptography 46.0.7; Docker Desktop Linux
container cho netem. Đây là kết quả chạy thật trong lượt bổ sung mã hóa,
không suy ra đã nghiệm thu LAN hai máy.

| Kiểm tra | Kết quả |
|---|---|
| Full suite, `BTL_NETWORK_TEST=1` | 230 test, OK, không skip; 40,724 giây |
| Test mã hóa riêng | 8 test, OK: sai khóa/tag, plaintext, nonce, TCP/UDP và giới hạn |
| Demo mock/đĩa | 11 lượt PASS; sửa assertion demo chat cũ để kiểm cả message_id |
| 20 client, file 0/1 KiB/1 MiB/10 MiB/16 MiB, 140 file nối tiếp | PASS, 83,965 giây; pending/relay transfer/relay failure = 0 |
| Python fault: 3 client, 64 KiB, loss 7%, delay 6 ms, seed 7 | PASS, 4,292 giây; 96 drop, 29 duplicate được bơm |
| Docker Compose config/build | PASS; server đọc khóa mount read-only thành công |
| Docker/netem: 10 client, 64 KiB, 2 file nối tiếp | PASS, 11,514 giây; `tc -s`: 165 dropped; pending/relay failure = 0 |

Netem áp delay 20±5 ms, loss 7%, duplicate 1%, reorder 25%/50%, seed 7 lên
lo trong container. `injected_drops=0` ở harness là đúng vì kernel tạo lỗi;
không bật fault Python cùng lượt này. Số đo là một lượt, không phải benchmark
thống kê. Chưa chạy lại browser bằng tay hoặc LAN hai máy sau thay đổi này;
HTTP E2E trong full suite đã chạy trên backend/socket thật.
