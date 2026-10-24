# API để code song song

Cập nhật 24/10/2026 theo [README](../README.md) và [giao thức](01_PROTOCOL.md).
Nguồn code: `common/contracts.py`, `common/models.py`. Các hàm còn
`NotImplementedError` là TODO, không phải tính năng đã chạy.

## Transport chung

| API | Hợp đồng |
|---|---|
| `await start()` | Mở tài nguyên nhận/gửi; không tự đăng nhập |
| `await send(peer, packet)` | Gửi đến chặng kế tiếp; bản thật đợi ACK DATA hoặc raise lỗi |
| `await receive()` | Trả ReceivedPacket đã kiểm tra phiên/endpoint, dedup/order theo stream; nhả slot nhận |
| `await close()` | Dừng transport, giải phóng task/timer/buffer/phiên; caller dừng vòng nhận nền |

`peer` là `(host, port)` của socket kế tiếp, `packet.recipient` là người nhận
nghiệp vụ. Client gửi đến server; server lookup endpoint người nhận. Packet
frozen; transport dùng `dataclasses.replace` để gán session/seq/recv_limit.

Không thêm `release()`. `receive()` chỉ nhả slot khi giao gói cho caller; caller
phải xử lý hoặc đưa vào queue/worker hữu hạn trước khi nhận tiếp. Không spawn task
vô hạn để né backpressure; việc ghi đĩa/UI không chạy trong vòng nhận UDP.
ACK và WINDOW_UPDATE/PROBE được transport xử lý nội bộ, không phát event nghiệp vụ.

## Đăng ký peer/session trong transport thật

```python
transport.register_peer(peer, session_id)
```

Chữ ký trong `transport/reliable_udp.py` **không có tx_key/rx_key**, không tạo
khóa giả và không gọi PayloadCipher. Hàm hiện vẫn TODO, chưa lưu trạng thái thật.
Khi triển khai A3/A6 phải tuân:

- `peer`: địa chỉ UDP đã resolve, port hợp lệ; session_id hex 32 ký tự do server
  cấp sau AUTH. Đăng ký không mở socket, không tự làm user online.
- Client: nhận AUTH_OK → register_peer(server UDP, session_id) → gửi BIND →
  chờ BIND_OK → cho phép nghiệp vụ. Trước BIND_OK chỉ cho bootstrap BIND.
- Server: nhận BIND qua đường bootstrap, tra phiên AUTH đang chờ và kiểm tra
  nguồn như protocol → bind_peer → register_peer → gửi BIND_OK/online.
- Gọi lại cùng peer/session là idempotent; mapping xung đột phải báo lỗi. Endpoint
  đã bind không tự đổi. Hết phiên dọn mapping và state send/receive tương ứng;
  peer tái kết nối chỉ đăng ký session mới sau khi mapping cũ đã được dọn.
- Mapping phiên/endpoint không phải xác thực mật mã. Mọi packet sau BIND, kể cả
  ACK/window, phải đối chiếu mapping trước khi dùng.

Đây là API cấu hình riêng của ReliableUdpTransport, không thêm vào Transport
Protocol và MemoryTransport: mock không làm auth/BIND.

## Flow control giữa hai bên

`Packet.recv_limit: int | None` nằm cuối dataclass để giữ thứ tự tham số cũ.
ACK và WINDOW_UPDATE mang giới hạn sequence tuyệt đối của chiều DATA đối diện,
đơn vị packet. WINDOW_PROBE hỏi lại khi chưa có credit; cả ba loại dùng seq=0,
không ACK tiếp và không tranh cửa sổ DATA. `None` không có nghĩa là 0.

Cấu hình chung: RECEIVE_WINDOW_PACKETS=8, WINDOW_PROBE_SECONDS=1,
WINDOW_STALL_TIMEOUT_SECONDS=10, MAX_RECEIVE_STREAMS=128. Sender khởi đầu limit=0,
probe xin cửa sổ; hợp nhất cập nhật bằng max, không cộng credit mỗi ACK. Receiver
chỉ tăng limit khi giao packet đúng thứ tự qua receive(). Chi tiết quota, mất
UPDATE, retry và timeout ở [protocol mục 6](01_PROTOCOL.md#6-flow-control-đã-chốt-recv_limit-theo-packet).
CongestionController trả ngân sách **byte** riêng; send phải tuân cả hai.

Model/enum/config đã có, codec và hành vi flow control vẫn A1/A6/A7. Không đánh
dấu flow control hoàn thành chỉ vì constructor Packet nhận được field mới.

## Client, file và UI

- `ClientService(transport, server_peer, username)` giữ constructor hiện tại.
  Cấu hình TCP/login thực tế còn K1; `server_peer` là endpoint UDP.
- `send_chat(recipient, text)` đã kiểm tra recipient, rỗng và giới hạn 480 byte
  UTF-8, tạo MSG rồi gọi transport. Mock gửi được; chưa chứng minh chat mạng thật.
- `receive_events()` hiện nhận MSG hợp lệ về endpoint/recipient, giải mã UTF-8
  và đưa vào `events` (queue tối đa 100). USER_LIST/file/error còn TODO.
- Một consumer gọi transport.receive() rồi dispatch. Không để mỗi transfer gọi
  receive riêng và lấy mất gói của nhau. Event đầy phải có backpressure/gộp tiến
  độ phù hợp; vòng UDP vẫn cần tiếp tục xử lý ACK/window độc lập.
- Event hiện có: `{type: "message", sender, recipient, text}` và lỗi UTF-8.
  Event users/progress/status/error nghiệp vụ sẽ được bổ sung khi ghép chức năng.
- `FileReceiver.begin/accept_chunk/finish` đã xử lý file local, trùng/sai thứ tự,
  size/hash, quota và dọn file (chi tiết bên dưới). `send_file()` và luồng META → ACCEPT tự động → CHUNK → END → COMPLETE
  chưa triển khai; hash local không chứng minh truyền file qua mạng.
- HTTP localhost + polling đã chọn. Bridge gọi ClientService; browser không mở
  socket mạng hay gọi PayloadCipher. Xem [UI](../ui/README.md).

## Ghép mock và mạng thật

Chạy bốn demo trong README ở cùng bản code. Mock chỉ đưa Packet vào queue trong
một process, không encode, auth, ACK, loss, window hay CUBIC. Khi transport thật
có lifecycle và phiên đúng, truyền vào ClientService để kiểm thử lại toàn luồng.

Hải phụ trách server/transport/common; Khoa phụ trách client/file/UI và phối hợp
an toàn đầu vào. Đổi API thì cập nhật caller/model/docs cùng thay đổi; không tự
thêm mã hóa, MSG_DELIVERED hay module mới để đáp ứng một bản đề xuất cũ.

## FileReceiver: giới hạn và lifecycle local

Constructor giữ `FileReceiver(output_directory)`, thêm cấu hình keyword:
`max_file_bytes=16 MiB`, `max_total_bytes=64 MiB`, `max_transfers=4`,
`max_chunks=65536`, `timeout_seconds=45`, `clock=time.monotonic`.
Các quota áp dụng cho file đang nhận trên instance; không phải quota ổ đĩa cho
các file đã hoàn tất. `reserved_bytes` là tổng size META đã dành, không phải số
byte đã ghi. Chỉ trả quota sau khi dọn `.part` thành công.

META phải có đúng name/size/sha256/chunk_size/total_chunks; strict type (loại
bool), hash hex 64 ký tự, tên UTF-8 tối đa 128 byte, không path/ký tự điều khiển/
tên thiết bị, số chunk khớp size. Transfer ID hex thường 32 ký tự. Hash file rỗng
được kiểm tra ngay; SHA-256 nội dung luôn kiểm tra khi finish.

- `begin`: validate/quota rồi tạo `.part` độc quyền; thất bại không có transfer
  mới. META trùng ID đang nhận bị từ chối, không xóa transfer cũ.
- `accept_chunk`: giữ dedup/reorder; lỗi dữ liệu/index/ghi làm hủy và dọn transfer.
  Chunk mới ghi thành công mới cập nhật last_progress; chunk trùng không kéo timeout.
- `finish`: thiếu chunk/sai size/hash/lỗi publish thì dọn transfer và raise.
  Tạo tên đích bằng hard link cùng filesystem để không ghi đè file xuất hiện
  đồng thời; filesystem không hỗ trợ thì báo lỗi, không fallback rename ghi đè.
- `cancel(id) -> bool`: idempotent, xóa file tạm và trả quota; không xóa file
  hoàn chỉnh hoặc file không thuộc instance. False nếu ID không hoạt động.
- `expire() -> list[str]`: dùng monotonic, dọn transfer không tiến triển >=45s
  và retry các cleanup đã lỗi. Caller cần timer định kỳ, đề xuất mỗi giây.
- `close()`: dọn tất cả transfer khi logout/shutdown, vẫn thử các transfer khác
  nếu một file không xóa được; trả ExceptionGroup nếu còn lỗi dọn.

Không có timer/thread tự mở trong FileReceiver. Caller tuần tự hóa mọi lời gọi
trên instance trong worker riêng để I/O/hash không chặn vòng mạng. `begin` tự
quét expire; accept/finish cũng từ chối transfer hết hạn trước khi xử lý.
Lỗi unlink giữ state/quota để thử dọn lại; không báo cleanup thành công giả.
Nếu file đã publish đúng hash nhưng unlink `.part` lỗi, giữ file đích và state;
caller có thể retry finish hoặc cancel để dọn, không báo FILE_COMPLETE trước khi
finish thành công. Thư mục nhận là thư mục cục bộ tin cậy của client; chưa có
phục hồi transfer hoặc tự xóa file mồ côi của phiên/process trước sau restart.

Quota FileReceiver và quota stream độc lập. Hải triển khai thu hồi buffer khi
stream CLOSED, giữ dấu chống trùng nhỏ đến hết phiên theo protocol mục 6.1.
Client dispatcher/timer chưa ghép; các API local này chưa chứng minh file mạng.
