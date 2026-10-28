# Tiến độ và kết quả theo phiên bản

## Commit K2: danh sách online — 28/10/2026

- Ghép USER_LIST theo revision, kiểm tra schema/username, bỏ trang trùng/cũ.
- Dispatcher vẫn nhận chat khi danh sách chưa đầy đủ.
- Kiểm tra snapshot commit: 27 test local; demo_client_users và demo_client_chat.
- Client file dispatcher/login/UI vẫn cần triển khai ở các commit sau.

## Bổ sung theo review của Hải — 24/10/2026

- FileReceiver đã validate schema/hash/tên/ID, quota size/tổng byte/transfer/chunk,
  cancel/expire/close, dọn lỗi và publish không ghi đè đích. Lỗi dọn giữ quota để
  retry; không tự có timer hoặc resume sau restart. API chi tiết trong docs/02_API.
- Vòng đời stream ACTIVE/DRAINING/CLOSED: trả buffer khi đóng; cache kết quả
  120s và giữ dấu CLOSED nhỏ đến hết phiên, quota 4096/phiên. Hải triển khai,
  kiểm thử >128 stream nối tiếp và packet đến muộn còn TODO.
- Comment login dùng peer/session. Server, codec, transport vẫn do Hải triển khai.
- Regression local: `python -m unittest discover -s tests -v` (16 test);
  bốn demo tiếp tục chạy. Đây không phải kiểm thử UDP/flow/CUBIC.

## Snapshot đồng bộ ngày 23/10/2026

Nhánh: `codex/align-protocol-docs`, phát triển từ local HEAD `80d1e0e`.
Kết quả snapshot dưới đây gắn với commit `c1494d9`, không khẳng định main hoặc máy Hải đã có thay đổi. Xem snapshot bằng
`git show c1494d9:docs/06_TIEN_DO.md`; lấy nhánh theo
[quy trình Git](03_GIT_WORKFLOW.md).

Phạm vi sửa theo [cuộc trò chuyện](https://chatgpt.com/share/6adb5bb8-0280-83ec-9f78-4044c8b17269):

- Đồng bộ README/protocol/API/TODO/UI/thiết kế và comment code. Giữ bridge
  localhost + polling, TCP login/UDP chat-file, file nhận tự động, CUBIC giản lược.
- Bỏ `tx_key/rx_key` khỏi chữ ký register_peer; không tạo khóa giả. Hàm vẫn TODO.
- Thêm recv_limit, WINDOW_UPDATE/PROBE và cấu hình; quy định cửa sổ theo packet,
  giới hạn tăng dần, probe/deadline khi mất cập nhật. Xử lý UDP thực tế vẫn TODO.
- Đưa phần client/file local và hai demo bổ sung vào cùng cây nguồn để Hải lấy
  về. Giữ cấu trúc code; thêm assertion demo và không nhận bool làm chunk_size.
- Tách lịch sử local/GitHub khỏi README; thêm bố trí demo hai máy. Chưa có lệnh
  chạy ứng dụng mạng hoàn chỉnh và chưa merge nhánh này vào main.

## Kết quả local ngày 23/10/2026

Môi trường: macOS, Python 3.13.3. Chạy demo trong thư mục tạm với PYTHONPATH trỏ
repo để tránh ghi đè file thử đang có của người dùng. Ở thư mục gốc, các lệnh
`python -m scripts...` tương đương tạo file dưới data/ như README mô tả.

| Kiểm tra | Kết quả | Giới hạn bằng chứng |
|---|---|---|
| Parse cú pháp 28 file Python bằng ast.parse | PASS | Không chứng minh thực thi các stub |
| `python -m scripts.demo_mock` | PASS; packet/message_id/peer đúng | Queue trong một process |
| `python -m scripts.demo_file_chunks` | PASS; 1000 byte → 480/480/40, bytes/hash đúng | Chia/ghép local |
| `python -m scripts.demo_client_chat` | PASS; event friend nhận đúng sender/recipient/text | Forward bằng mock |
| `python -m scripts.demo_file_receiver` | PASS; 2,0,0,1 → 3 chunk duy nhất, bytes/hash đúng, không còn .part | FileReceiver trên đĩa local |
| `python -m server.main --help`, `python -m client.main --help` | PASS, exit 0 | Chỉ parser tham số |
| `python -m server.main`, `python -m client.main --username khoa` | Exit 2 với thông báo chưa triển khai, đúng trạng thái dự kiến | Chưa mở server/client thật |
| Kiểm tra trực tiếp file rỗng, binary/tên tiếng Việt, reorder/dedup | PASS; đúng byte, state và .part được dọn sau finish thành công | Script kiểm tra tạm, chưa có regression suite trong repo |
| Kích thước chunk bool/âm/quá lớn, index sai, path traversal, thiếu chunk/hash sai | PASS; từ chối và không tạo file hoàn chỉnh sai | Các ca đã thử, không bao phủ mọi metadata/lỗi đĩa |
| Chat 480/483 byte UTF-8, rỗng/recipient sai, sai peer/đích, UTF-8 hỏng | PASS; gửi/loại/event lỗi đúng | ClientService với MemoryTransport |
| Chữ ký register_peer, Packet bất biến/replace, enum window | PASS; không cần khóa, recv_limit None khác 0 | Chỉ model/API, không phải flow control mạng |

Ba demo file/chat có assertion để exit khác 0 khi kết quả sai. Không dùng
`python -O` khi chạy kiểm tra vì chế độ đó bỏ assert.

## Việc còn thiếu trước khi ghép và nộp

- Codec chưa encode/decode/validate packet mới; Transport.register_peer trong
  lớp thật chưa lưu mapping, ReliableUdpTransport/CongestionController vẫn stub.
- Auth/session/BIND/router/heartbeat/online và socket TCP/UDP chưa triển khai.
- Client login/send_file, USER_LIST/file/error dispatch chưa hoàn chỉnh.
- FileReceiver đã được bổ sung theo review ở đầu file; còn nối dispatcher,
  timer expire, cancel/error và shutdown của ClientService. Chưa xong K4/K5.
- Queue event hữu hạn đã có nhưng chính sách backpressure/gộp progress và tách
  worker đĩa chưa được ghép/test với mạng; UI/bridge vẫn TODO/fixture.
- T01–T22 trong [nghiệm thu](04_ACCEPTANCE.md) **chưa chạy trên ứng dụng thật**.
  Không suy từ mock ra đã đạt loss/ACK/retry/window/CUBIC, 10–20 client hoặc bảo mật.

## Lịch sử được chuyển từ README cũ

Các dòng dưới là ghi nhận cũ ngày **21/10/2026**, không phải lần kiểm tra GitHub
mới trong ngày 23/10 và không mô tả main hiện tại:

| Mốc | Ghi nhận trong README cũ |
|---|---|
| GitHub `463c8b3` | Có model/mock/chia file; FileReceiver và ClientService còn TODO; chưa có hai demo bổ sung |
| Local ngày 21/10 | Có FileReceiver begin/accept_chunk/finish và ClientService send_chat/event MSG; chưa commit |
| Kiểm tra local ngày 21/10 | Cú pháp 28 file, bốn demo, hash file 1000 byte và receiver 2,0,0,1 đạt; --help chạy, entrypoint mạng exit 2 |

Bảng này được giữ để giải thích nhận xét “code mới chỉ ở máy Khoa”. README chính
hiện mô tả source đi cùng phiên bản tài liệu, không dùng mốc 463c8b3 làm tiến độ
mới. Chỉ sau review/merge mới cập nhật main và xác nhận Hải đã lấy đúng commit.
