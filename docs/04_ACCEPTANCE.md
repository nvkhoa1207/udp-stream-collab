# Nghiệm thu — ghi kết quả thật

Cập nhật 29/10/2026: test server/client nhóm qua TCP/UDP thật và HTTP/browser
trên Windows loopback. 202/202 unittest PASS khi bật BTL_NETWORK_TEST=1;
11 lượt demo mock/local PASS riêng. LAN hai máy, Docker Engine/netem và T16
chưa chạy. Bảng ghi phạm vi từng bằng chứng, không coi unit/mock là mạng LAN.

| ID | Tình huống | Kết quả mong đợi | Kết quả thực tế / bằng chứng |
|---|---|---|---|
| T01 | Kết nối + đăng nhập đúng/sai | Phản hồi rõ; sai không tạo session | PASS TCP thật: test_team_auth, test_server_auth/handshake; đúng/sai, trùng, pending expiry, EOF/timeout |
| T02 | 3 client, chat chung | Các đích dự kiến nhận đúng một lần | PASS UDP thật: test_team_e2e ba client; network_acceptance broadcast đủ 20 client |
| T03 | A nhắn riêng B, có C | B nhận; C không nhận nội dung | PASS UDP thật ba client; nhiều sender cùng đích, private không tới client thứ ba |
| T04 | Gửi file rỗng, binary, tên tiếng Việt | Tự FILE_ACCEPT, không cần click nhận; đúng byte/size/hash; tên an toàn | PASS UDP thật: file rỗng/binary/tên tiếng Việt, kiểm byte/hash/.part và COMPLETE |
| T05 | File 1 KiB, 1 MiB và 10 MiB | Không hỏng; bộ nhớ có giới hạn; progress đúng | PASS harness 20 client: 1 KiB/1 MiB/10 MiB/16 MiB, đúng size/hash; report Hải có thời gian |
| T06 | Mô phỏng mất DATA và ACK 5–10% | Gửi lại, hoàn tất hoặc báo lỗi timeout rõ ràng | PASS UDP thật với drop DATA/ACK riêng; seed 7 loss 7% file 64 KiB, 96 injected drop; không phải netem |
| T07 | Gói trùng, sai thứ tự, ACK trùng | Không nhân đôi nội dung/progress; buffer hữu hạn | PASS UDP test reorder/dedup/ACK trùng; fault cuối có 29 duplicate, file đúng hash |
| T08 | Tắt một client khi gửi file | Server sống; báo thất bại, dọn trạng thái/tệp tạm theo chính sách | PASS receiver logout và sender mất socket/heartbeat expiry; dọn .part, thông báo bên còn lại; đã sửa race relay lỗi trước sweep |
| T09 | Một file bị mất gói trong lúc chat | Chat/luồng khác tiếp tục; đo độ trễ thực tế | PASS chat xen file 20 client: 10,87–19,23ms; fault 6 ms/loss 7%: 154,66ms, một lần đo |
| T10 | 10 rồi 20 client đồng thời | Auth/chat/online đúng; server không crash | PASS mức 10 và 20 client thật cùng process; TCP/UDP/user list/chat; không thay thử LAN nhiều máy |
| T11 | Session không tồn tại, sai endpoint, sender tự khai sai, packet sai schema/quá lớn | Từ chối sai phiên/nguồn/schema; sender lấy từ phiên; không tuyên bố chống giả mạo mật mã | PASS UDP thật: test_team_e2e giả sender/session/endpoint, malformed/oversize; bootstrap sai IP/endpoint, codec strict; không có xác thực mật mã |
| T12 | Filename chứa ../ hoặc path tuyệt đối | Không ghi ra ngoài thư mục nhận | PASS META qua UDP thật với ../escape.bin, không tạo file thoát thư mục; local test thêm path tuyệt đối/tên thiết bị |
| T13 | File thiếu chunk, hash sai, lỗi ghi | Không hiển thị thành công; không xuất file hoàn chỉnh sai | PASS missing chunk/hash sai qua UDP thật, không xuất file sai; lỗi ghi/unlink/cleanup được fault/unit test ở FileReceiver |
| T14 | HTTP bridge localhost + polling | Online/chat/progress/lỗi rõ, không block | PASS hai HTTP bridge thật login/chat/upload/poll/logout; hai browser chat/file completed và hủy hai phía, fixture chậm chunk để thao tác |
| T15 | CUBIC giản lược dưới loss/delay | Cửa sổ tăng bậc ba, giảm loss/timeout, ACK trùng không tăng; có số liệu | PASS clock unit + UDP delay/loss hai stream chung peer; fault cuối server 19 loss events; CUBIC rút gọn, không đầy đủ RFC9438 |
| T16 | Máy còn lại cài/chạy từ README | Làm được từng bước, không cần hướng dẫn miệng | CHƯA CHẠY: máy còn lại chưa cài/chạy từ README; hướng dẫn docs/07_WINDOWS_LAN_DOCKER.md |
| T17 | Receiver dừng consume, window cạn dù DATA đã ACK | Không gửi seq vượt recv_limit; buffer/queue/task hữu hạn | PASS UDP flow test: 8 ACK vẫn chặn seq 9 khi chưa consume, buffer có trần; chat stream khác tiếp tục |
| T18 | Mất WINDOW_UPDATE mở cửa sổ, mất cả probe/phản hồi đầu | Probe nhịp 1 giây khôi phục credit; timeout hữu hạn nếu không tăng limit | PASS UDP test mất UPDATE và probe/phản hồi đầu; retry nhịp rút gọn trong test, stall deadline, control độc lập |
| T19 | ACK/UPDATE trùng, đảo thứ tự, sai stream/session | Không cộng credit; limit chỉ tăng ở đúng stream/phiên/hướng; ACK không nhận dữ liệu giả | PASS logic transport/unit + UDP: max credit, sai stream/session, ACK cũ/trùng và ACK cho DATA chưa thực sự phát |
| T20 | Stream mới xin window lúc hết quota, rồi có chỗ; phiên kết thúc | Chưa cấp buffer thì limit=0; không ACK dữ liệu bỏ; giải phóng đúng quota/phiên | PASS UDP test quota rx hạ còn 2 để buộc zero-window; giải phóng rồi probe khôi phục; remove/close dọn phiên và waiter |
| T21 | Hơn 128 file nối tiếp trong một phiên, chat vẫn hoạt động | Slot ACTIVE được tái dùng sau mỗi file; CLOSED không giữ payload buffer | PASS 140 file đầu-cuối trong 20 client; thêm lượt 4096 file, không giữ buffer file CLOSED; chat vẫn hoạt động |
| T22 | Mất ACK COMPLETE, packet/probe cũ sau đóng, đủ 4096 CLOSED rồi reconnect | Không giao/tạo file lại; cache kết quả rồi dấu CLOSED; quota lịch sử hữu hạn, phiên mới hoạt động | PASS phần transport/app loopback: ACK COMPLETE mất, DATA/probe cũ, 4096 file thật/quota/control/logout/reconnect/file mới; TTL120s dùng clock unit, chưa đo LAN |

Sai tag/nonce/replay mật mã chỉ kiểm thử nếu bổ sung TLS/HMAC/AES sau này,
không phải tiêu chí bắt buộc của bản đầu. T11 không chứng minh chống nghe lén
hay giả mạo chủ động khi kênh chưa được bảo vệ. Kiểm tra local riêng được ghi
trong [tiến độ](06_TIEN_DO.md), không tự chuyển các hàng mạng trên thành PASS.

## Mô phỏng mạng

Có thể viết lớp fault-injection quanh socket send/receive để drop/duplicate/
delay có seed cố định. Chạy với dữ liệu thử trong localhost/LAN. Ghi seed,
loss rate, loại gói bị tác động, file size và cấu hình. Chỉ dùng memory queue
không kiểm tra được UDP thật. Mỗi kiểm thử lưu PASS/FAIL cùng bằng chứng, không
điền số thời gian/tốc độ chưa đo. Hai máy vật lý bổ sung cho thử nhiều terminal.

## Đối chiếu 10 mục chấm điểm

| Mục đề | Module/bằng chứng chính |
|---|---|
| 1. Protocol | docs/01_PROTOCOL + sơ đồ trạng thái, viết trước code |
| 2. Handshake | server/app + client/service; T01 |
| 3. Auth/state | server/auth, sessions; T01, T08 |
| 4. Concurrency | async lifecycle; T09, T10 |
| 5. Messaging | router, service; T02, T03 |
| 6. File UDP | transport + files; T04–T07, T13 |
| 7. Errors | timeout/disconnect/cleanup; T08, T13 |
| 8. Security | auth/session/endpoint, đầu vào/path/quota/localhost; T11–T13, ghi giới hạn chưa mã hóa |
| 9. UI | ui theo phản hồi giảng viên; T14 |
| 10. Quality/tests | code sạch, README, kết quả thật; T01–T22 |

Congestion và tránh HoL vẫn cần đối chiếu phần yêu cầu kỹ thuật dù không có
dòng điểm riêng. Không dùng bảng này để suy ra chắc chắn 10/10.

## Lệnh/bằng chứng bản tích hợp

- `BTL_NETWORK_TEST=1; python -m unittest discover -s tests -q`: 202 PASS, không skip.
- `python -m scripts.verify_demos`: 11 lượt mock/local PASS trong thư mục tạm.
- `python -m scripts.network_acceptance --clients 20 --sizes 1024,1048576,10485760,16777216 --streams 140`: PASS 152,723s; 0 pending/transfer/failure.
- `python -m scripts.network_acceptance --clients 10 --sizes 1024 --streams 1`: PASS 2,986s; 0 pending/transfer/failure.
- `python -m scripts.network_acceptance --clients 3 --sizes 65536 --streams 2 --loss 0.07 --delay 0.006 --seed 7`: PASS 5,285s; file3,119s/chat 154,66 ms; 96 drop/29 duplicate/1145 lượt ghi.
- `python -m scripts.network_acceptance --clients 2 --sizes 0 --streams 4095`: PASS 109,243s; đủ4096 file, app/transport từ chối lượt mới, control/logout vẫn được dùng; reconnect cả hai client rồi file mới PASS.
- Compose config và node --check PASS; Docker info báo Engine pipe không tồn tại. Chưa build/run/netem.

Chi tiết giới hạn và môi trường ở reports/HAI_REPORT.md. Số đo là một lần chạy,
seed không cố định lịch OS; chưa phải percentile hay số liệu LAN. Screenshot
browser là output local ngoài Git; báo cáo Khoa được giữ nguyên như bàn giao.


## Regression cho review f75894c — 29/10/2026

- Toàn bộ 208 test đạt với BTL_NETWORK_TEST=1 trên Windows (128,215 giây).
- R1: 20 client, 2 vòng x 380 tin riêng; đúng người nhận, không trùng,
  server giữ 40 stream nhận; file sau chat đúng nội dung; logout thu hồi peer.
- R2: CANCEL lặp sau COMPLETE/CLOSED giữ nguyên file, phiên và heartbeat;
  chat và file tiếp theo vẫn thành công. Test mất ACK COMPLETE cũ vẫn đạt.
- R3: bỏ BIND_OK đầu tiên, USER_LIST vẫn đầy đủ; thử cả 20 người/nhiều trang;
  kiểm tra chat đến sớm và giới hạn 100 gói chờ bootstrap.
- R4: thử vòng nhận bị hủy và server dừng tới heartbeat timeout; login lại
  trong cùng runtime dọn transport/task cũ, chat lại được. Trường hợp vòng
  nhận bị hủy còn kiểm tra truyền file sau reconnect. Chưa thử lại thao tác
  browser khi server mất kết nối trong đợt sửa này.
- Fault test: 3 client, 64 KiB, 2 file nối tiếp, loss 7%, delay 6 ms, seed 7:
  PASS 7,674 giây; 0 pending, 0 relay transfer/failure. Đây là loopback,
  không phải kết quả LAN hay netem.

Một lượt chạy trước có timeout ở test hủy file cũ khi đợi dọn file (5 giây).
Bộ E2E lúc đó bị nạp hai lần do import trong file test mới; đã sửa import,
không tăng timeout và không bỏ assertion. Lượt full suite sau đó đạt.
Test hủy file được chạy riêng thêm 3 lần liên tiếp: đều đạt (6,365 giây tổng cộng).
