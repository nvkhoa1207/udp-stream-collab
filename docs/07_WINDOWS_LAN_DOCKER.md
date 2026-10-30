# Chạy Windows, LAN và Docker/netem

Cập nhật 29/10/2026. Windows loopback đã chạy; LAN hai máy và Docker Engine
chưa chạy trong phiên tích hợp này. Không dùng kết quả fault injection Python
để khẳng định đã chạy netem.

## Windows: server và hai client

Chạy trong repo, Python 3.11+. Không cần activate nếu PowerShell chặn script:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m server.main --host 127.0.0.1 --tcp-port 9000 --udp-port 9001
```

Server hỏi mật khẩu demo cho hai/khoa. Nhập 1–128 ký tự; ghi nhớ để dùng trong
form client. Không có mật khẩu mặc định trong server production; restart tạo
hash/salt mới trong RAM. Username server là tên dành riêng.

Terminal thứ hai:

```powershell
.\.venv\Scripts\python.exe -m client.main --http-port 8000 --received-dir data/received-hai
```

Terminal thứ ba:

```powershell
.\.venv\Scripts\python.exe -m client.main --http-port 8001 --received-dir data/received-khoa
```

Mở http://127.0.0.1:8000 và http://127.0.0.1:8001. Cả hai nhập server
127.0.0.1/TCP 9000. Không nhập cổng UDP vào UI; AUTH_OK cấp cổng UDP 9001 và
client BIND từ socket của phiên. Chọn username đích để gửi file; Tất cả chỉ
dùng chat chung. Receiver tự nhận file, lưu tên `<transfer_id>_<name>`.

Sau khi bridge restart, reload browser để lấy token HTTP mới. Ctrl+C đóng từng
client trước rồi server. Username cũ có thể bị giữ tối đa 10 giây khi tắt đột ngột.

## Kiểm tra LAN hai máy — người vận hành còn cần chạy

1. Máy A chạy server với `--host 0.0.0.0`; ghi IP LAN từ `ipconfig`.
2. Client trên cả A và B nhập cùng IP LAN của A, TCP 9000. Bridge trên mỗi máy
   vẫn chỉ 127.0.0.1; không mở HTTP 8000/8001 ra LAN.
3. Nếu cần, cho phép chương trình server nhận TCP 9000 và UDP 9001 trên profile
   Private của Windows Firewall. Không cần tắt firewall; cấu hình này do người
   vận hành làm, phiên tích hợp không tự thay thiết lập hệ thống.
4. Thử hai user online; chat chung và riêng; file rỗng/binary/tên tiếng Việt,
   1 MiB rồi 10 MiB, hủy từ cả hai phía, tắt một client giữa file và login lại.
5. So hash file gốc và file nhận bằng lệnh sau trên từng máy:

```powershell
Get-FileHash -Algorithm SHA256 .\duong-dan-file
```

Ghi commit, Python/OS, IP/cổng dùng trong lab, size/hash, thời gian, PASS/FAIL
và ảnh vào báo cáo. Không ghi password/session. T16 chỉ hoàn thành sau khi máy
còn lại tự cài/chạy từ README thành công.

## Bộ nghiệm thu loopback có thể chạy lại

```powershell
$env:BTL_NETWORK_TEST = '1'
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
.\.venv\Scripts\python.exe -m scripts.verify_demos
.\.venv\Scripts\python.exe -m scripts.network_acceptance --clients 20 --sizes 1024,1048576,10485760,16777216 --streams 140
.\.venv\Scripts\python.exe -m scripts.network_acceptance --clients 3 --sizes 65536 --streams 2 --loss 0.07 --delay 0.006 --seed 7
.\.venv\Scripts\python.exe -m scripts.network_acceptance --clients 2 --sizes 0 --streams 4095
```

Harness dùng tài khoản test không bí mật và file tạm tự dọn, không chỉnh tài
khoản server production. Các lệnh network_acceptance dùng socket TCP/UDP thật.
Loss/delay/duplicate được bơm trong chương trình trước socket write; seed ổn
định bộ sinh lỗi, nhưng lịch chạy OS có thể làm số packet/thời gian khác lần trước.

Fixture browser riêng, chỉ localhost, tài khoản test hai/khoa theo docstring:

```powershell
.\.venv\Scripts\python.exe -m scripts.demo_network_ui --seconds 300
```

Mở HTTP 18000/18001, nhập TCP 19000. Có `--slow-file-seconds 15` để giữ giai
đoạn send chunk đủ lâu khi thử nút Hủy; delay này chỉ ở fixture. Fixture tự dọn
sau deadline, không thay cấu hình ứng dụng chính.

## Docker và netem — chuẩn bị, chưa chạy

Đã chạy `docker compose --profile test config --quiet`: PASS. `docker info`
thất bại vì pipe dockerDesktopLinuxEngine không tồn tại; Engine chưa hoạt động.
Vì vậy chưa build image, chưa chạy container, chưa xác minh kernel hỗ trợ netem.

Sau khi bật Docker Desktop với Linux Engine:

```powershell
docker info
docker compose build
docker compose run --rm --service-ports --name btl-server server
```

Server hỏi hai mật khẩu demo như bản Windows. Hai client Windows kết nối
127.0.0.1:9000; Compose publish TCP 9000 và UDP 9001 riêng, chỉ trên localhost.
Ctrl+C dừng server foreground. Không bật server Windows cùng cổng cùng lúc.

Kiểm thử netem trong container riêng:

```powershell
docker compose --profile test run --rm netem
```

Service test có capability NET_ADMIN; `scripts/netem_acceptance.sh` áp qdisc
lên **lo trong container**, vì client và server của harness cùng container.
Cấu hình: limit 1024, delay 20±5 ms, loss 7%, duplicate 1%, reorder 25%/50%,
seed 7. Harness không bật fault Python ở lượt này. Script in `tc -s`, chạy
10 client/file 64 KiB/2 file nối tiếp rồi gỡ qdisc trong trap. Không dùng host
network và không tác động qdisc của Windows/host.

Nếu báo thiếu NET_ADMIN, netem kernel module, hoặc iproute2 chưa hỗ trợ seed,
ghi lỗi môi trường; không đổi kết quả thành PASS. Khi iproute2 cũ, bỏ `seed 7`
khỏi lệnh tc và ghi rõ netem không tái lập seed; fault Python vẫn có seed riêng.

Nguồn cú pháp và phạm vi capability:
[Docker run/capabilities](https://docs.docker.com/engine/containers/run/),
[tc-netem manual](https://man7.org/linux/man-pages/man8/tc-netem.8.html).
