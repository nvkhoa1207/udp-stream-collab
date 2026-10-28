# Tiến độ và kết quả theo phiên bản

## Chốt phần code Khoa và bàn giao — 28/10/2026

- K1–K6 có code client/UI; K8 có báo cáo phần Khoa, README và ảnh browser.
- Suite 119 ca: 118 PASS, 1 TCP opt-in skip; chạy riêng TCP loopback thật PASS.
  Thêm runtime upload qua hai ClientService với relay mock, hash/cleanup đúng.
- K7 mạng thật còn chờ server/transport/codec của Hải; chưa đánh dấu đạt LAN,
  loss/ACK/window/CUBIC hoặc UI chat/file mạng thật. Xem reports/KHOA_REPORT.md.
- Commit/push từng phần trên codex/align-protocol-docs, chưa merge main.


## Người nhận chủ động hủy file — 28/10/2026

- Thêm cancel_receive_file(id), xử lý qua cùng worker/queue hữu hạn. Dọn .part,
  trả quota rồi báo CANCEL control về sender; không xóa file đã hoàn tất.
- Snapshot có cancel_delivery; lỗi gửi không dừng các lượt khác, lỗi dọn giữ
  cleanup_pending/quota. Shutdown không bỏ người gọi chờ vô hạn.
- 107 test PASS (6 mới), gồm hủy/ID sai, file hoàn tất, gửi CANCEL timeout,
  unlink lỗi, queue đầy và shutdown giữa thao tác đĩa. Demo file_control với
  --receiver-cancel PASS: người nhận hủy tại 1920/5120 byte, sender dừng,
  không còn .part/quota, file hoàn tất trước đó vẫn nguyên vẹn.
- Còn login/logout, HTTP/UI và ghép transport/server thật với Hải. Đây là
  kiểm thử mock/local, chưa nghiệm thu mạng thật.

## Báo hết hạn chủ động về sender — 28/10/2026

- Khi expire dọn thành công, ERROR FILE_EXPIRED đi control, liên hệ META gốc.
  Cũng xử lý expire bên trong begin file mới; mỗi thông báo được lấy gửi một lần.
- Cache ERROR để END muộn nhận lại cùng kết quả; giữ dấu ID chống mở lại.
  Gửi thông báo có deadline 10s; lỗi vẫn truyền ra caller và cleanup worker.
- 101 test PASS, gồm thông báo đúng META/người nhận, không phát lặp, expire
  ngầm khi mở file, shutdown và lỗi gửi thông báo. Demo_client_receive_status
  PASS với ERROR FILE_EXPIRED; vẫn mock/đồng hồ giả, chưa UDP thật.
- Còn login/logout, nút hủy nhận local, HTTP/UI và tích hợp mạng với Hải.

## Đóng gói các bài học thành commit — 28/10/2026

- USER_LIST: commit `bab2dd0`, snapshot riêng qua 27 test cùng demo_client_users
  và demo_client_chat; đã push lên `codex/align-protocol-docs`.
- Commit chứa mục nhật ký này đóng gói luồng gửi/nhận file, worker, progress,
  CANCEL, timeout, test/demo và API tương ứng. Các thành phần đang phụ thuộc
  chung dispatcher được giữ cùng nhau để commit chạy độc lập. Kiểm tra: 97 test,
  demo_client_file_control và demo_client_receive_status; chỉ chứng minh mock/local.
- Từ đây, mỗi phần mới phải kiểm tra → commit → push trước khi sang phần khác,
  theo docs/03_GIT_WORKFLOW.md. Không tự merge main. `output/` giữ local.
- Các ghi chú “chưa commit/push” trong mục bài học bên dưới mô tả thời điểm học,
  trước đợt đóng gói này; không phải trạng thái của phiên bản đã được commit.

## Phần Khoa: trạng thái và tiến độ phía nhận — 28/10/2026

- poll_file_updates trả cả send/receive; giới hạn chung 100 record, khóa theo
  chiều + ID và lock ngắn giữa worker đĩa với event loop. Không bỏ kết quả cuối
  chưa đọc; đầy thì từ chối META trước tạo .part bằng FILE_STATUS_LIMIT.
- Nhận chunk mới ghi thành công mới cộng received_bytes. Xuất receiving,
  verifying, completed, failed, cancelled, expired; thông báo lỗi có code/message
  an toàn, không lộ path. Chunk trùng/sai sender không tăng tiến độ.
- Expire định kỳ và expire trong begin đồng bộ trạng thái; lỗi dọn giữ snapshot
  cleanup_pending/quota, retry thành công cập nhật cờ. COMPLETE gửi lỗi hoặc
  shutdown sau finish vẫn giữ file và completed, confirmation_delivery=failed.
- 97 test PASS (13 mới), gồm trùng/sai thứ tự, hash sai, hết hạn, quota snapshot,
  retry cleanup, hai chiều cùng ID và hủy worker trong lúc finish. Demo
  demo_client_receive_status PASS: 0/40/520/1000 byte, completed; expired và failed
  với sai hash, chỉ file đúng còn lại, không còn .part/quota.
- Còn login/logout, HTTP/UI, nút hủy nhận local, timeout báo chủ động về sender
  và tích hợp mạng/stream với Hải. Test dùng mock/đồng hồ giả, chưa UDP thật.
  Thay đổi bài học vẫn local, chưa commit/push.

## Phần Khoa: tiến độ phía gửi và FILE_CANCEL — 28/10/2026

- Thêm poll_file_updates: snapshot byte đã được send xác nhận và trạng thái;
  đủ byte vẫn verifying, chỉ COMPLETE hợp lệ mới completed. Gộp snapshot theo
  ID, tối đa 100 record; giữ kết quả cuối tới khi đọc, đầy thì từ chối lượt mới.
- Thêm cancel_file(id), dừng/đợi tác vụ con rồi gửi CANCEL control với deadline
  3s. Lỗi/timeout sau META cũng thử báo CANCEL. cancel_delivery phân biệt kết
  quả gửi tới server với việc receiver đã dọn; lỗi gửi hủy không che lỗi gốc.
- Worker nhận CANCEL kiểm tra chủ lượt truyền, dọn .part/quota, giữ ID chống
  META đến sau. CANCEL lặp không gia hạn cache; file đã COMPLETE được giữ nguyên.
- 84 test PASS (14 mới): tiến độ không tăng khi send còn chờ; 100% chưa hoàn
  tất; cancel API/remote/task, cancel timeout, cancel hai lần, cleanup qua relay,
  CANCEL trước META/sau COMPLETE/sai chủ, lỗi unlink và giới hạn snapshot/quota.
- Demo_client_file_control PASS: 5120/5120 verifying rồi completed; lượt tiếp
  hủy tại 1920/5120, receiver dọn .part/quota và giữ nguyên file đầu tiên.
- Vẫn là mock, chưa có ACK/UDP thật. Còn login/logout, HTTP/UI, snapshot phía
  nhận, nút hủy lượt nhận local và tích hợp vòng đời stream với Hải. Chưa commit/push.

## Phần Khoa: gửi file tự động — 28/10/2026

- Thêm FileSender, nối ClientService.send_file và dispatcher ACCEPT/COMPLETE/ERROR.
  META → chờ ACCEPT → nhóm tối đa 8 CHUNK → END → COMPLETE đúng size/hash.
  Một file gửi mỗi client; tối đa 16 MiB và 4096 lần xử lý gửi mỗi phiên.
- Hash/đọc nguồn chạy trong thread, phát hiện nguồn đổi trước END. Deadline
  send 45s, chờ ACCEPT/COMPLETE 30s; lỗi/hủy dừng và đợi các task con, đóng nguồn
  và giải phóng lượt gửi. ERROR đối chiếu message_id đã gửi, kể cả chunk cũ.
- Bộ test local: 70 PASS (14 test mới); gồm gửi/nhận cùng nhau, file rỗng,
  phản hồi sai/sớm, thiếu ACCEPT/COMPLETE, lỗi liên quan chunk cũ, giới hạn
  concurrency, hủy, send timeout, vòng nhận dừng và nguồn bị thay đổi.
- Demo_client_file_send PASS: hai ClientService qua relay bộ nhớ truyền 5120
  byte, file đích khớp nội dung/SHA-256, quota nhận được trả lại.
- Chưa có FILE_CANCEL qua mạng, progress/status, login/logout/UI; chưa thử
  socket thật. Hủy local chưa báo receiver ngay, receiver dùng timeout/close.
  Thay đổi bài học hiện vẫn local, chưa commit/push.

## Phần Khoa: ghi chunk và hoàn tất file — 28/10/2026

- Nối FILE_CHUNK vào accept_chunk và FILE_END vào finish trong worker hiện
  có của ClientService. Kiểm tra sender khớp chủ lượt nhận trước khi ghi/đóng.
- Chỉ trả COMPLETE sau khi đủ chunk, đúng size/SHA-256 và lưu/dọn .part xong.
  File rỗng được hỗ trợ. Lỗi trả ERROR và dọn lượt nhận; không xóa file hoàn
  chỉnh nếu lỗi gửi COMPLETE hoặc lỗi dọn .part sau khi đã lưu file.
- Cache kết quả COMPLETE/lỗi của CHUNK/END tối đa 120 giây, không gia hạn khi
  gói lặp; giữ dấu ID đã dùng. END lặp không tạo lại file, chunk muộn sau khi
  hoàn tất không được ghi thêm. Chưa thay thế vòng đời stream của Hải.
- `python -m unittest discover -s tests -v`: 56 test PASS (13 test mới), gồm
  trùng/sai thứ tự, thiếu chunk, sai hash/sender, file rỗng, cache hết hạn, lỗi
  cleanup, hai lượt độc lập và chat trong lúc kiểm tra file.
- Demo client_file_receive, client_file_offer, client_users, client_chat PASS.
  Demo mới nhận đúng 1000 byte theo thứ tự 2,0,0,1, trả COMPLETE đúng size/hash,
  END lặp trả lại cùng phản hồi. Dùng mock/thư mục tạm, không mở TCP/UDP.
- send_file, xử lý ACCEPT/COMPLETE/ERROR phía gửi, CANCEL, tiến độ, login/logout
  đầy đủ và HTTP/UI vẫn TODO. Thay đổi local chưa xác nhận đã lên GitHub.

## Phần Khoa: chuẩn bị nhận file — 27/10/2026

- Nối FILE_META → FileReceiver.begin → FILE_ACCEPT/ERROR vào ClientService.
  Receiver được truyền bằng keyword, mặc định chưa bật nhận file. Ghi rõ
  stream_id = transfer_id UUID hex 32 ký tự cho luồng file trong protocol.
- Worker queue 8 META, I/O chạy trong thread theo thứ tự, không chặn chat khi
  chờ chuẩn bị/gửi phản hồi. Hủy vòng nhận đợi I/O xong rồi dọn .part/quota.
  Quét expire khi worker chạy; deadline gửi phản hồi 10s. ID đã dùng được giữ
  có giới hạn để META muộn không tạo lại file; không thay state transport.
- `python -m unittest discover -s tests -v`: 43 test PASS, thêm 16 test cho
  metadata/quota/ERROR, chat trong lúc chờ, queue hữu hạn, timeout, shutdown,
  lỗi cleanup và hủy giữa thao tác đĩa.
- Demo client_file_offer, client_users, client_chat PASS qua mock. Demo offer
  dùng thư mục tạm, kiểm tra .part trước ACCEPT, giữ lượt hợp lệ khi một META
  khác sai, dọn file và quota khi dừng. Không mở TCP/UDP, chưa gửi chunk.
- CHUNK/END/COMPLETE/CANCEL, nhận ERROR, tiến độ, send_file, login/logout đầy
  đủ và UI/bridge vẫn cần làm. Chưa xác nhận thay đổi local đã lên GitHub.

## Phần Khoa: nhận danh sách online — 27/10/2026

- Thêm `client/user_list.py`: kiểm tra payload/schema/username, gom các trang
  USER_LIST cùng revision, bỏ trang trùng/phiên bản cũ, giới hạn 128 trang và
  1024 username; chỉ phát danh sách khi đầy đủ. Trang lỗi giữ nguyên state tốt.
- Nối vào `ClientService.receive_events()`: kiểm tra peer/recipient/sender,
  phát event users hoặc lỗi dữ liệu, tiếp tục nhận chat khi danh sách chưa đủ.
- `python -m unittest discover -s tests -v`: 27 test PASS (11 test mới cho
  USER_LIST/dispatcher và 16 test FileReceiver).
- `python -m scripts.demo_client_users` và `python -m scripts.demo_client_chat`:
  PASS qua MemoryTransport. Demo online đưa trang 1 đến trước trang 0 và nhận
  chat ở giữa, không mở TCP/UDP.
- Đây là thay đổi local, chưa xác nhận GitHub/máy Hải đã có. Server phát
  USER_LIST, login/logout và reset assembler theo phiên mới, file dispatcher,
  bridge/UI vẫn cần ghép; chưa đạt nghiệm thu mạng thật.

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
- Client login chưa hoàn chỉnh. USER_LIST, nhận META/CHUNK/END/CANCEL,
  send_file với ACCEPT/COMPLETE/ERROR/CANCEL và snapshot hai chiều đã có theo
  các mục 27–28/10; server phát danh sách và vòng đời phiên chưa ghép.
- FileReceiver đã nối worker nhận, snapshot và expire/shutdown; đã có API hủy lượt nhận local; chưa ghép UI/mạng thật để nghiệm thu K4/K5.
- Queue event hữu hạn, snapshot progress gộp và worker đĩa đã có nhưng chưa
  được ghép/test với mạng; UI/bridge vẫn TODO/fixture.
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

## Commit login/lifecycle — 28/10/2026

ClientService.login có TCP HELLO/AUTH, BIND, heartbeat và logout. 112 test PASS
(5 mới cho record TCP, BIND và lifecycle). Chưa chạy mạng thật do transport/server
của Hải còn stub. Bridge/UI và entrypoint là commit tiếp theo.

## Commit HTTP bridge — 28/10/2026

Bridge/runtime có API phiên/chat/file/cancel/polling, Host/Origin/token, upload
16 MiB và 4 job, ring 512 event. 117 test PASS (5 mới: runtime, cleanup, HTTP
đầu vào/nguồn). UI thật và entrypoint được nối ở commit tiếp theo.

## Commit UI/entrypoint — 28/10/2026

Form login, online/chat, file/progress/cancel hai chiều nối HTTP API, không còn
fixture. client.main mở localhost UI; password chỉ nhập form. 117 test PASS,
node --check PASS. Browser localhost: form hoạt động, login báo đúng transport
stub (503), chat/file vẫn disabled; ảnh reports/client-ui-check.png. Chưa có
bằng chứng UI chat/file qua mạng thật vì phụ thuộc Hải.

## Hải — tích hợp A3 ngày 28/10/2026
Merge origin/codex/align-protocol-docs 2999d08 vào hai/server-transport. Client/server dùng chung strict TCP reader và giới hạn password 1–128 ký tự. Server quét phiên định kỳ và dọn khi close. Regression: 170 tests, 169 pass, 1 optional network test skipped; test_team_auth dùng TCP thật với server nhóm. Sandbox Windows kẹt socketpair nên test chạy ngoài sandbox.

