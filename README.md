# UDP Stream Collab

Bài tập lớn ứng dụng client–server: đăng nhập, xem người online, chat chung/riêng
và truyền file tin cậy qua UDP. **Thành viên:** Nguyễn Việt Khoa và Hải.

- TCP: handshake và đăng nhập. UDP: chat, file, heartbeat và ACK.
- UDP tin cậy: sequence, ACK/retry, sliding window, flow control và CUBIC giản lược.
- AES-256-GCM mã hóa bản tin TCP/UDP; SHA-256 kiểm tra file nhận.
- Giao diện HTML/CSS/JavaScript dùng HTTP polling qua client Python cục bộ.

```text
Browser A ↔ HTTP ↔ Client Python A ↔ TCP/UDP ↔ Server ↔ TCP/UDP ↔ Client Python B ↔ HTTP ↔ Browser B
```

## 1. Chuẩn bị

Yêu cầu **Python 3.11+**. Docker Desktop chỉ cần khi chạy server trong container.
Chạy mọi lệnh từ thư mục gốc dự án.

Nếu chưa có mã nguồn:

```bash
git clone https://github.com/nvkhoa1207/udp-stream-collab.git
cd udp-stream-collab
```

**macOS/Linux:**

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m security.cipher --generate-key shared.key
```

**Windows PowerShell:**

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m security.cipher --generate-key shared.key
```

Nếu đã có `.venv`, dùng lại và cài cập nhật requirements. **Nếu đã có
`shared.key`, bỏ qua lệnh tạo khóa.** Server và mọi client phải dùng cùng file
khóa; không tạo khóa riêng cho từng máy, không commit hoặc công khai khóa.
Khóa mã hóa khác với mật khẩu tài khoản. Có thể đặt biến môi trường
`BTL_KEY_FILE` để dùng đường dẫn khóa khác.

## 2. Chạy thử trên một máy

Mở **ba Terminal/PowerShell**, mỗi cửa sổ đều ở thư mục dự án.
Giữ cả ba chạy trong lúc sử dụng.

**Terminal 1 — server:**

```bash
.venv/bin/python -m server.main --host 127.0.0.1 --tcp-port 9000 --udp-port 9001
```

Windows:

```powershell
.\.venv\Scripts\python.exe -m server.main --host 127.0.0.1 --tcp-port 9000 --udp-port 9001
```

Server hỏi mật khẩu lần lượt cho `hai`, `khoa`, `user3`. Tự đặt mật khẩu cho mỗi tài khoản;
khi gõ không hiện ký tự. Mỗi lần khởi động server sẽ đặt lại mật khẩu demo.

**Terminal 2 — client Hải:**

```bash
.venv/bin/python -m client.main --http-port 8000 --received-dir data/received-hai
```

Windows:

```powershell
.\.venv\Scripts\python.exe -m client.main --http-port 8000 --received-dir data/received-hai
```

**Terminal 3 — client Khoa:**

```bash
.venv/bin/python -m client.main --http-port 8001 --received-dir data/received-khoa
```

Windows:

```powershell
.\.venv\Scripts\python.exe -m client.main --http-port 8001 --received-dir data/received-khoa
```

Mở browser và đăng nhập:

| Thông tin | Hải | Khoa |
|---|---|---|
| Trang giao diện | [127.0.0.1:8000](http://127.0.0.1:8000) | [127.0.0.1:8001](http://127.0.0.1:8001) |
| Server | `127.0.0.1` | `127.0.0.1` |
| TCP port | `9000` | `9000` |
| Username | `hai` | `khoa` |
| Password | Mật khẩu Hải vừa đặt | Mật khẩu Khoa vừa đặt |

Để chạy thêm client thứ ba trên Mac, mở terminal mới:

```bash
.venv/bin/python -m client.main --http-port 8002 --received-dir data/received-user3
```

Mở http://127.0.0.1:8002, đăng nhập `user3` bằng mật khẩu đã đặt trên server;
Server là `127.0.0.1`, TCP port `9000`.

**Không mở cổng `9000` bằng browser:** đó là cổng giao thức TCP của server.

- Chọn **Everyone** để chat chung hoặc một username để chat riêng/gửi file.
- Tin nhắn tối đa **480 byte UTF-8**; file tối đa **16 MiB**, tự nhận vào thư mục đã cấu hình.
- **Chỉ người gửi chủ động Cancel.** Người nhận xử lý yêu cầu hủy và dọn file tạm.
- Chờ **Completed** để xác nhận file hoàn tất; 100% byte gửi chưa đủ kết luận.
- Server/TCP port được nhớ qua F5 bằng `localStorage`; ứng dụng không lưu mật khẩu trong đó.
- Đóng tab không dừng client Python. Kết thúc bằng **Log out**, rồi `Ctrl+C` ở các client và server.

## 3. Chạy LAN hai máy

1. Hai máy dùng cùng bản code, cài dependency và cùng file `shared.key`
   (chuyển khóa qua kênh tin cậy, chẳng hạn USB).
2. Trên máy server, chạy lệnh server ở mục 2 với **`--host 0.0.0.0`**.
3. Mỗi máy chạy một client; cả hai có thể dùng `--http-port 8000` vì là hai máy riêng.
4. Browser trên từng máy mở `http://127.0.0.1:8000`. Trong form, nhập **IP LAN
   của máy server** và TCP `9000`; không nhập `0.0.0.0` hoặc localhost của máy khác.
5. Nếu bị chặn, kiểm tra firewall cho TCP `9000`/UDP `9001` và Wi-Fi có cô lập client không.

Hai máy ở hai nhà khác nhau không tự thuộc cùng LAN; cần mạng/VPN cho phép
kết nối giữa chúng. HTTP bridge chỉ nghe localhost, không cần mở cổng HTTP ra LAN.

## 4. Chạy server bằng Docker

Mở Docker Desktop, đợi Engine hoạt động. Chuẩn bị `shared.key` trên host theo
mục 1 và dừng server cũ đang chiếm cổng `9000/9001`.

```bash
docker compose build
docker compose run --rm --service-ports --name btl-server server
```

Đặt mật khẩu khi được hỏi. Client vẫn chạy trên máy host như mục 2 và đăng nhập
vào `127.0.0.1:9000`. Compose mount khóa read-only; image không chứa khóa.
Cổng Docker hiện chỉ publish localhost, không dùng trực tiếp cho LAN hai máy.

## 5. Lỗi thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| Thiếu `cryptography` | Cài requirements và chạy bằng Python trong `.venv` |
| Thiếu/sai khóa, đăng nhập đóng kết nối | Kiểm tra cùng file `shared.key` ở mọi bên; khởi động lại tiến trình sau khi đổi khóa |
| Cổng đang được dùng | Dừng instance cũ; không chạy server Python và Docker cùng cổng |
| Login bị từ chối | Kiểm tra mật khẩu hoặc username đang online; phiên mất đột ngột cần khoảng 10 giây để hết hạn |
| BIND timeout | Kiểm tra IP server, UDP 9001 và firewall |
| `Failed to fetch` | Chạy lại client Python rồi reload browser để lấy token mới |
| Server mất kết nối | Chạy lại server, chờ client báo mất phiên rồi bấm Log in; ứng dụng không tự nối lại phiên cũ |
| `window stalled` khi đang truyền | Kiểm tra server/client nhận và đường mạng; khôi phục kết nối rồi gửi lại file |

## 6. Cấu trúc mã nguồn

| Thư mục | Nội dung | Phụ trách |
|---|---|---|
| `client/`, `ui/` | Client, xử lý file, HTTP bridge và giao diện | Khoa |
| `server/`, `transport/`, `common/` | Server, giao thức, UDP tin cậy và CUBIC | Hải; hai người thống nhất API |
| `security/` | AES-GCM và quản lý file khóa | Cả hai |

**Giới hạn:** khóa AES dùng chung trong nhóm tin cậy; server vẫn giải mã để
chuyển tiếp, không phải E2EE. Chưa có TLS, trao đổi khóa tự động, forward secrecy
hoặc lớp chống replay mật mã riêng. File trên đĩa không được mã hóa bởi lớp này.
Phiên/lịch sử hoạt động nằm trong RAM; chưa có đăng ký tài khoản hoặc tiếp tục
file sau restart.

Repository hiện chỉ giữ mã chạy và cấu hình. Test, demo/netem, báo cáo và tài
liệu phát triển đã được gỡ; có thể xem lại trong lịch sử Git tại commit `445e7af`.
