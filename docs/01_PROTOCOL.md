# Giao thức v1 — hợp đồng triển khai chung

Cập nhật 28/10/2026 theo phạm vi trong [README](../README.md) và
[cuộc trò chuyện đối chiếu](https://chatgpt.com/share/6adb5bb8-0280-83ec-9f78-4044c8b17269).
Đây là lựa chọn kỹ thuật của nhóm. **Model/API đã cập nhật; codec và transport
mạng vẫn TODO**, chưa phải giao thức đã chạy hoặc được nghiệm thu.

## 1. Kiến trúc đã chọn

- Mỗi máy: browser → HTTP localhost → Python client → server bằng TCP/UDP.
- TCP 9000: handshake và đăng nhập. UDP 9001: BIND, chat/file, ACK, flow control,
  heartbeat, online, lỗi và logout.
- HTTP bridge chỉ bind `127.0.0.1`, phục vụ HTML/CSS/JS và API cùng origin;
  JavaScript polling bằng `fetch()`. Python dùng thư viện, không dùng framework;
  không Flask, Django, FastAPI hoặc WebSocket.
- Bản đầu chưa mã hóa payload, chưa có TLS/HMAC/AES hoặc cấp khóa. Các cơ chế này
  là mở rộng tùy chọn; giới hạn bảo mật được ghi ở mục 8.

## 2. Handshake, đăng nhập và BIND

Mỗi record TCP là prefix độ dài 4 byte big-endian + JSON UTF-8, body tối đa
64 KiB. Đọc đủ prefix/body, xử lý EOF và timeout; không coi một `recv()` là đủ.

| Record | Trường ngoài `type` |
|---|---|
| `HELLO` | `version`, `client_name` |
| `HELLO_ACK` | `version`, `server_name` |
| `AUTH` | `username`, `password` |
| `AUTH_OK` | `session_id`, `udp_port` — không có `tx_key`/`rx_key` |
| `AUTH_ERROR` | `code`, `message` — không chứa bí mật |

Tài khoản demo tạo trước, mật khẩu lưu bằng hàm băm có salt. Username dài 1–32
ký tự ASCII `[A-Za-z0-9_-]`; `*` dành riêng cho chat chung. Từ chối username đang
sử dụng. Session ID ngẫu nhiên 128-bit, biểu diễn 32 ký tự hex, chỉ giữ trong RAM.

1. AUTH đúng tạo phiên đang chờ BIND; chưa đưa user vào danh sách online.
2. Client nhận AUTH_OK, đăng ký `(server_udp_peer, session_id)` với transport và
   gửi BIND từ socket UDP sẽ dùng suốt phiên.
3. Server nhận BIND ở đường bootstrap: kiểm tra phiên chờ còn hạn và IP nguồn
   khớp kết nối TCP. Lấy endpoint từ `recvfrom`, không lấy từ payload tự khai.
   Sau đó `SessionRegistry.bind_peer()` và `register_peer(peer, session_id)`.
4. Server gửi BIND_OK; client chỉ gửi nghiệp vụ sau BIND_OK. Server công bố online
   khi bind hợp lệ. BIND trùng cùng endpoint trả lại BIND_OK; endpoint khác bị từ chối.
5. Hết 10 giây chưa BIND, logout hoặc heartbeat timeout thì hủy phiên. Kết nối
   lại tạo session mới; không tự thay endpoint của phiên đã bind.

BIND/BIND_OK dùng `stream_id="control"`, `seq=0`, payload rỗng. Client retry BIND
với RTO/số lần hữu hạn cho đến BIND_OK; không cần cửa sổ dữ liệu hoặc ACK riêng.
Nhờ vậy không phải đăng ký khóa giả hoặc đợi flow control trước khi tạo phiên.

## 3. Packet UDP và giới hạn

Nguồn model: [common/models.py](../common/models.py). Wire là JSON UTF-8 với
payload base64. `Packet` bất biến; transport dùng bản sao khi gán session/seq.

| Field | Quy ước |
|---|---|
| `version` | 1 |
| `kind` | Giá trị `MessageType` |
| `session_id` | Phiên của chặng trực tiếp, transport gán |
| `message_id` | UUID hex; giữ khi retry/relay để đối chiếu |
| `sender`, `recipient` | Danh tính nghiệp vụ; server xác định sender từ phiên |
| `stream_id` | ID luồng tối đa 64 ký tự; mỗi file có ID riêng |
| `seq` | Reliable DATA bắt đầu từ 1 theo chặng/phiên/hướng/stream; ứng dụng để 0 |
| `ack` | Chỉ ACK dùng: một sequence DATA >= 1; loại khác là `None` |
| `chunk_index` | Chỉ FILE_CHUNK dùng: chỉ số từ 0; loại khác là `None` |
| `payload` | Bytes trong code, base64 trên wire |
| `recv_limit` | Bắt buộc trên ACK/WINDOW_UPDATE: số nguyên >= 0, sequence DATA tối đa bên kia được gửi; loại khác là `None` |

`recv_limit` tính bằng **packet**, là giới hạn tuyệt đối, không phải byte hoặc số
slot còn trống tại một thời điểm. `None` nghĩa là không quảng bá, khác với 0 là
chưa cấp quyền gửi. Codec phải loại bool, số âm và sai tổ hợp field/kind.

Datagram tối đa 1200 byte; body JSON tối đa 1100 byte (`MAX_PLAINTEXT_BYTES`, giữ
tên cũ để tương thích); payload thô tối đa 480 byte. Bản đầu không có envelope/tag
mật mã. Kiểm tra byte encode thực tế của cả packet, kể cả `recv_limit`; payload
480 byte không tự bảo đảm mọi header đều vừa gói. Không cắt dữ liệu âm thầm.

USER_LIST do server tạo (`sender="server"`), dùng
`{revision, page, pages, users:[...]}`. `users` là danh sách username dạng chuỗi
theo quy tắc ở mục 2, không trùng trong cùng danh sách. `revision` là số nguyên
không âm, tăng khi danh sách thay đổi; `pages >= 1`, `0 <= page < pages`.
Danh sách rỗng vẫn có một trang với `users=[]`. Mỗi revision có số trang và nội
dung cố định; chia trang theo kích thước encode và giới hạn payload.
Client chỉ thay danh sách sau khi đủ các trang cùng revision; bỏ trang trùng và
revision cũ, không trộn các phiên bản. Tên file, metadata và ERROR cũng phải
vừa giới hạn.

## 4. Loại gói và stream

| Loại | Payload / hành vi |
|---|---|
| `BIND`, `BIND_OK` | Bootstrap phiên như mục 2 |
| `HEARTBEAT` | Payload rỗng, seq=0, không ACK/retry; gửi lại ở nhịp kế tiếp |
| `USER_LIST` | JSON phân trang |
| `MSG` | Text UTF-8; recipient `*` hoặc username |
| `FILE_META` | JSON `name`, `size`, `sha256`, `chunk_size`, `total_chunks` |
| `FILE_ACCEPT` | Payload rỗng; chương trình nhận đã kiểm tra metadata và chuẩn bị nơi lưu |
| `FILE_CHUNK` | Bytes file; có chunk_index |
| `FILE_END` | Payload rỗng; hết dữ liệu gửi, chưa phải hoàn tất |
| `FILE_COMPLETE` | JSON `sha256`, `size`; file đã được xác minh và lưu |
| `FILE_CANCEL` | JSON đúng hai trường `transfer_id` (32 hex thường), `reason` (chuỗi 1–128 ký tự); đi control, chunk_index null; payload tối đa 480 byte |
| `ACK` | Payload rỗng, ack và recv_limit; chỉ xác nhận một DATA được giữ hợp lệ |
| `WINDOW_UPDATE` | Payload rỗng, recv_limit; không xác nhận DATA |
| `WINDOW_PROBE` | Payload rỗng; bên gửi DATA hỏi recv_limit của bên nhận, đi cùng chiều DATA |
| `ERROR` | JSON `code`, `message`, `related_message_id`; đi control |
| `LOGOUT` | Payload rỗng; dọn phiên, heartbeat timeout dự phòng khi mất gói |

ACK/WINDOW_UPDATE/WINDOW_PROBE có seq=0, không được ACK và không giao lên router
hay ClientService. Hướng của từng loại trong cùng stream/phiên:

- **WINDOW_PROBE đi cùng chiều DATA cần gửi.** Với DATA A→B, A gửi PROBE→B
  để hỏi khả năng nhận của B. B tra trạng thái nhận DATA A→B và trả UPDATE→A.
- **ACK/WINDOW_UPDATE đi ngược chiều DATA mà chúng tham chiếu.** Với DATA A→B,
  B gửi ACK/UPDATE→A; A cập nhật trạng thái gửi DATA A→B. UPDATE trả lời PROBE
  không cấp credit cho chiều DATA B→A và không xác nhận đã nhận DATA.

Chỉ chấp nhận control từ endpoint của phiên hợp lệ. BIND và heartbeat cũng
không chiếm sequence reliable. Các loại còn lại dùng sequence reliable >= 1.

Tách stream chat/control và từng file. Server khi relay giữ message_id nhưng
cấp session/seq của chặng đích; chat từ các sender khác nhau cần stream riêng
trên cùng chặng ra, ID sinh/ánh xạ không vượt 64 ký tự. ACK/window của A→server
không được chuyển nguyên sang chặng server→B.

## 5. ACK, retry và sliding window

- Selective Repeat: ACK riêng từng seq, giữ gói sai thứ tự trong buffer hữu hạn.
  Chỉ giao nghiệp vụ một lần và theo thứ tự trong stream; gói trùng được ACK lại.
- ACK sau khi giữ packet hợp lệ trong buffer. Không ACK packet bị loại do đầy,
  sai phiên, sai kích thước hoặc vượt cửa sổ. ACK không cam kết đã ghi ra đĩa.
- `send()` đợi ACK chặng kế tiếp hoặc raise lỗi. Caller dùng số task hữu hạn để
  tận dụng cửa sổ; không gửi tuần tự từng chunk thành stop-and-wait.
- RTO ban đầu 0,5 giây, tối đa 8 lần gửi lại; đo RTT trên gói chưa retransmit,
  backoff và timeout hữu hạn. Hết retry làm stream thất bại, không bỏ qua lỗ seq.
- ACK trùng/cũ không tăng progress/cwnd hoặc giải phóng bytes_in_flight lần hai.
- ACK từ server chỉ chứng minh chặng đầu; lỗi relay phải báo người gửi.
  Chat chưa có receipt “đã giao/đã đọc”. File chỉ xong sau FILE_COMPLETE hợp lệ.

## 6. Flow control đã chốt: recv_limit theo packet

Trạng thái riêng cho mỗi `(peer, session_id, hướng DATA, stream_id)`:

- Bên nhận có `W = RECEIVE_WINDOW_PACKETS = 8` slot, chứa cả reorder và hàng đợi
  sẵn sàng giao. `consumed_seq` là seq cuối đã giao **liên tiếp** qua `receive()`.
- Khi đã dành đủ W slot cho stream: `recv_limit = consumed_seq + W`. Không giảm
  giới hạn đã cấp trong phiên/stream. Bên gửi hợp nhất cập nhật bằng
  `peer_recv_limit = max(peer_recv_limit, received_limit)`; gói cũ không thu hồi
  credit và gói lặp không cấp thêm credit.
- Stream mới bắt đầu với `peer_recv_limit=0`; sender gửi WINDOW_PROBE. Receiver
  dành buffer rồi trả WINDOW_UPDATE(W). Khi chưa đủ quota trả limit=0, không ACK
  dữ liệu chưa giữ. Tối đa `MAX_RECEIVE_STREAMS=128` stream có buffer trên mỗi
  transport, kể cả control; dành slot cho control ngay khi BIND trước khi cấp
  buffer file/chat. Thiếu quota thì từ chối phiên/stream mới, không lấy phần
  control cho file. Giới hạn này là cấu hình khởi đầu, chưa đo tải.
- DATA mới chỉ gửi nếu `next_seq <= peer_recv_limit` **và** đủ ngân sách congestion.
  Mỗi seq mới dùng một slot. Retransmit cùng seq không dùng thêm slot, vẫn theo
  lịch retry/congestion. ACK giải phóng bytes đang bay, không tự tạo slot nhận.
- Khi `receive()` giao packet đúng thứ tự, nhả slot, tăng consumed_seq và gửi
  WINDOW_UPDATE với giới hạn mới. ACK cũng mang giới hạn hiện tại. Không cần thêm
  API `release()`; caller phải dùng queue/worker hữu hạn, không rút vô hạn từ receive.
- Mạng nhận/ACK/window chạy riêng với xử lý đĩa/UI. Receiver giữ lời hứa buffer
  cho credit đã cấp; không hạ limit hoặc thu hồi slot vì downstream tạm chậm.
  Stream đã kết thúc giữ trạng thái chống trùng cho các retry còn có thể đến,
  không tái sử dụng stream ID trong cùng phiên.

**Ví dụ:** W=8, receiver cấp limit=8. Sender gửi seq 1–8; dù cả 8 đã ACK, seq 9
vẫn phải chờ. Receiver giao seq 1–3 cho ứng dụng thì gửi limit=11: sender được
thêm seq 9–11. UPDATE cũ limit=8 hoặc ACK trùng không thay đổi limit=11.

**Mất thông báo mở cửa sổ:** khi còn dữ liệu cần gửi nhưng chưa có credit,
sender gửi WINDOW_PROBE tối đa mỗi 1 giây cho stream. Receiver trả WINDOW_UPDATE
hiện tại kể cả khi chưa có DATA. Các gói này đi ngoài cửa sổ DATA, được giới hạn
nhịp để không gây vòng ACK hoặc flood. Mất probe/update thì nhịp sau hỏi lại.
Sau `WINDOW_STALL_TIMEOUT_SECONDS=10` giây không tăng credit khi đang bị chặn,
báo timeout và kết thúc stream; phản hồi cùng limit không reset deadline.
Không ngừng retry DATA đã gửi chỉ vì đang hết credit cho DATA mới.

Đây mới là **hợp đồng và field trong model**. A6 còn phải triển khai state,
buffer, probe timer, kiểm tra quota và các kịch bản T17–T22 trong bảng nghiệm thu.

### 6.1. Thu hồi stream và chống trùng (Hải triển khai)

`MAX_RECEIVE_STREAMS=128` giới hạn **buffer ACTIVE/DRAINING**, không phải tổng
stream từng xuất hiện trong phiên. Tách ba loại trạng thái:

| Trạng thái | Giữ gì | Khi nào thu hồi |
|---|---|---|
| ACTIVE | W slot, reorder/ready queue, pending sends/timer | Chưa thu hồi chỉ vì queue đang rỗng; peer vẫn còn credit đã cấp |
| DRAINING | Buffer và các send còn đợi ACK | File hoàn tất: đợi xử lý/forward hết DATA và ACK của COMPLETE ở từng chặng; ngừng cấp DATA mới |
| CLOSED | Bản ghi nhỏ: ID/phiên/hướng, seq đã giao, kết quả success/failure | Thu hồi toàn bộ payload buffer, slot active và task/timer ngay khi đóng; giữ bản ghi đến hết phiên |

- File thành công: nghiệp vụ báo kết thúc cho transport của từng chặng sau khi
  COMPLETE đã được chuyển tiếp/xác nhận và không còn dữ liệu chờ xử lý. Server
  theo dõi riêng hai chặng, không đóng chặng ra chỉ vì chặng vào đã ACK.
- ERROR/CANCEL, hết retry hoặc timeout: kết thúc stream với lỗi, fail các send
  đang chờ, dừng worker liên quan và bỏ buffer; chuyển CLOSED ngay, không chờ ACK
  của chunk thiếu. Cancel/error đi control, không phụ thuộc cửa sổ stream file.
- Chat/control tái sử dụng stream trong suốt phiên; không tạo stream cho từng tin
  nhắn. Không đóng stream đang idle để thu hồi credit đã cấp. Logout/expire phiên
  hủy tất cả stream và xóa mapping/bản ghi của phiên đó; packet phiên cũ bị loại.
- CLOSED không nhận DATA mới, không cấp window mới và không quay lại ACTIVE do
  probe/META đến muộn. DATA đã giao có thể được ACK lại nhưng không giao lại;
  gói từng giữ rồi bị bỏ khi stream lỗi không được ACK như đã giao thành công.
  Phản hồi lỗi trạng thái đóng qua control có giới hạn nhịp, không hồi sinh stream.
- Cache kết quả FILE_COMPLETE/lỗi tối đa **120 giây**, để xử lý retry khi phản
  hồi cuối bị mất. RTO backoff cần chặn ở **4 giây**, tối đa 8 retry; sau deadline
  kết thúc thì sender không gửi DATA mới trên stream cũ. Hết 120 giây bỏ payload
  kết quả, vẫn giữ dấu CLOSED (ID + seq + trạng thái) cho đến hết phiên.
- Giới hạn riêng **4096 bản ghi CLOSED mỗi phiên**, tính cả stream kết thúc lỗi;
  dành chỗ bản ghi trước khi nhận stream mới. Chạm trần thì báo cần tạo phiên mới,
  không xóa dấu chống trùng của phiên còn sống. Giới hạn số phiên hoạt động của
  server cũng phải hữu hạn. Nhờ vậy có thể chạy hơn 128 file nối tiếp mà không
  giữ 128 buffer mãi; bộ nhớ lịch sử vẫn có trần và packet cũ không tái tạo file.

Đây là hợp đồng lifecycle, chưa có code transport. Khi ghép, Hải cần nối tín
hiệu kết thúc nghiệp vụ với state transport; không thêm API release từng packet.
Thử >128 file nối tiếp, mất ACK COMPLETE, DATA/META/probe đến muộn, đủ quota
CLOSED và session reconnect theo T21/T22 trước khi đánh dấu hoàn thành.

## 7. File tự động nhận

Mỗi lượt truyền dùng UUID hex thường 32 ký tự làm `transfer_id`; các gói
META/ACCEPT/CHUNK/END/COMPLETE dùng `stream_id = transfer_id`. Server giữ ID này
khi relay file; session/seq vẫn thuộc từng chặng. File chỉ gửi riêng đến một
username, không dùng recipient `*`. ERROR đi stream `control`, liên hệ gói gây
lỗi bằng `related_message_id`; CANCEL mang transfer_id trong payload như mục 4.

1. META với transfer ID mới → client nhận kiểm tra metadata/khả năng lưu, tạo
   `.part`, **tự gửi ACCEPT**. Lỗi thì ERROR; không có nút Accept/Reject thủ công.
2. Sender chờ ACCEPT rồi gửi chunk bằng concurrency hữu hạn. Tiến độ là byte
   duy nhất đã ACK **đến server**, không phải byte được receiver ghi xuống đĩa.
3. Receiver xử lý trùng/sai thứ tự; sender gửi END sau khi chunk đã ACK chặng đầu.
4. Receiver chỉ hoàn tất sau đủ chunk, đúng size/SHA-256 và lưu file thành công;
   gửi COMPLETE có đúng transfer/size/hash. Sender lúc đó mới báo hoàn tất.
5. File rỗng có 0 chunk nhưng vẫn META/ACCEPT/END/COMPLETE. Lỗi, timeout/cancel
   phải có thông báo và dọn tài nguyên; không chờ vô hạn hoặc báo thành công giả.

Tên file không chứa đường dẫn, không ghi ngoài thư mục nhận hoặc ghi đè file có
sẵn. Cần giới hạn tên, kích thước, số transfer và deadline. FileReceiver local
đã có kiểm tra metadata/quota và cancel/expire/close. ClientService đã nối
META/CHUNK/END qua worker, kiểm tra người gửi của lượt truyền, lưu file rồi trả
COMPLETE hoặc ERROR; quét expire và dọn khi dừng vòng nhận. Kết quả COMPLETE/lỗi
khi xử lý CHUNK/END được cache tối đa 120 giây, gói lặp không gia hạn cache;
ID đã dùng vẫn giữ tới hết phiên. Phía gửi tự động đã có send_file và dispatcher
ACCEPT/COMPLETE/ERROR; thử hai ClientService qua relay bộ nhớ thành công.
CANCEL qua giao thức và snapshot tiến độ hai chiều đã thử qua mock. Phía nhận
đếm byte chunk duy nhất đã ghi thành công; đủ byte chưa bỏ qua bước kiểm tra
size/hash khi END. Timer dọn thành công lượt hết hạn gửi ERROR FILE_EXPIRED
qua control, related_message_id là META gốc; cache kết quả 120s để trả lại khi
END đến muộn. Sender vẫn giữ deadline riêng nếu thông báo không đến. Sender dừng
và đợi các send/I/O đang chạy trước khi gửi CANCEL trên control; nếu sau META
có lỗi/timeout hoặc hủy task thì cũng thử báo CANCEL, deadline 3s. ACK chặng
server của CANCEL không xác nhận bên nhận đã dọn xong. Nếu báo hủy thất bại,
receiver vẫn có timeout/close; caller nhận lỗi/status, không báo thành công giả.

Receiver kiểm tra chủ lượt truyền trước khi hủy, dọn .part và trả quota. CANCEL
có thể vượt META vì khác stream: giữ ID đã hủy tới hết phiên (trong quota 4096),
META muộn không được tạo file. Lỗi FILE_CANCELLED được cache 120s như lỗi file
khác; CANCEL lặp không kéo dài cache. Nếu đã COMPLETE, giữ nguyên file cuối và
có thể trả lại COMPLETE đang cache. CANCEL sai schema không thay đổi state;
khác chủ trả FILE_SENDER_MISMATCH. Lỗi dọn file giữ quota để expire/close thử lại.

Kiểm thử đầu-cuối qua mạng, HTTP/UI và xử lý stream/ACK của transport vẫn chưa
triển khai. Phần gửi/nhận CANCEL không tự thay thế hook đóng
stream mà Hải phải nối vào transport.

## 8. Bảo mật và CUBIC trong phạm vi hiện tại

Không bắt buộc TLS/HMAC/AES hoặc tx_key/rx_key trong bản đầu. Vẫn cần băm mật khẩu
có salt, session khó đoán, kiểm tra endpoint/quyền gửi/đầu vào, quota, tên file an
toàn, HTTP localhost và không log bí mật. Băm mật khẩu lưu trữ không che password
trong AUTH qua TCP. Session/endpoint và SHA-256 không bảo vệ trước nghe lén hoặc
giả mạo chủ động; không tuyên bố packet đã được xác thực bằng mật mã.

Chọn **CUBIC giản lược**, không còn chờ chọn BBR hay CUBIC. Controller dùng chung
cho các stream trên một peer/hướng, trả ngân sách byte; flow control ở mục 6 tính
packet, phải thỏa cả hai điều kiện khi gửi. Cần cửa sổ tăng theo hàm bậc ba của
thời gian, giảm khi loss, xử lý timeout và tránh tăng do ACK trùng; fixed window
và retry đơn thuần chưa phải CUBIC. Đo loss/delay và ghi rõ phần giản lược, không
khẳng định tuân thủ đầy đủ [RFC 9438](https://www.rfc-editor.org/rfc/rfc9438.html).
