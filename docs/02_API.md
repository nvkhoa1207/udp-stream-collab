# API để code song song

Cập nhật 28/10/2026 theo [README](../README.md) và [giao thức](01_PROTOCOL.md).
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
đơn vị packet. WINDOW_PROBE đi cùng chiều DATA cần gửi để hỏi khả năng nhận:
với DATA A→B, A gửi PROBE→B; B tra trạng thái nhận A→B rồi gửi UPDATE→A.
ACK cũng đi B→A; A dùng ACK/UPDATE để cập nhật trạng thái gửi A→B, không phải
trạng thái nhận B→A. Cả ba loại dùng seq=0, không ACK tiếp và không tranh cửa sổ
DATA. `None` không có nghĩa là 0.

Cấu hình chung: RECEIVE_WINDOW_PACKETS=8, WINDOW_PROBE_SECONDS=1,
WINDOW_STALL_TIMEOUT_SECONDS=10, MAX_RECEIVE_STREAMS=128. Sender khởi đầu limit=0,
probe xin cửa sổ; hợp nhất cập nhật bằng max, không cộng credit mỗi ACK. Receiver
chỉ tăng limit khi giao packet đúng thứ tự qua receive(). Chi tiết quota, mất
UPDATE, retry và timeout ở [protocol mục 6](01_PROTOCOL.md#6-flow-control-đã-chốt-recv_limit-theo-packet).
CongestionController trả ngân sách **byte** riêng; send phải tuân cả hai.

Model/enum/config đã có, codec và hành vi flow control vẫn A1/A6/A7. Không đánh
dấu flow control hoàn thành chỉ vì constructor Packet nhận được field mới.

## Client, file và UI

- `ClientService(transport, server_peer, username, *, file_receiver=None)` giữ
  ba tham số vị trí cũ; có thể truyền FileReceiver riêng bằng keyword để bật
  nhận file. Cấu hình TCP/login còn K1; `server_peer` là endpoint UDP.
- `send_chat(recipient, text)` đã kiểm tra recipient, rỗng và giới hạn 480 byte
  UTF-8, tạo MSG rồi gọi transport. Mock gửi được; chưa chứng minh chat mạng thật.
- `receive_events()` nhận MSG hợp lệ về endpoint/recipient, giải mã UTF-8
  và đưa vào `events` (queue tối đa 100). USER_LIST từ sender `server` được gom
  đủ trang cùng revision rồi phát event. FILE_META/CHUNK/END gửi riêng được
  chuyển vào worker ghi/kiểm tra file và trả ACCEPT/COMPLETE/ERROR. Nhận
  ACCEPT/COMPLETE/ERROR được chuyển tới lượt gửi đang chờ. CANCEL đúng lượt gửi
  làm dừng sender; CANCEL khác được đưa vào cùng worker nhận để xử lý tuần tự.
- Một consumer gọi transport.receive() rồi dispatch. Không để mỗi transfer gọi
  receive riêng và lấy mất gói của nhau. Event đầy phải có backpressure/gộp tiến
  độ phù hợp; vòng UDP vẫn cần tiếp tục xử lý ACK/window độc lập.
- Event hiện có: `{type: "message", sender, recipient, text}`,
  `{type: "users", revision, users}` và `{type: "error", message}` cho lỗi
  UTF-8/USER_LIST. Snapshot file hai chiều lấy riêng qua poll_file_updates để
  không nhồi hàng nghìn progress vào queue chat. ERROR của chat còn TODO.
- `UserListAssembler` chỉ giữ một revision đang gom, tối đa 128 trang và 1024
  username phía client. Trang lỗi không phá state hợp lệ; trang trùng/revision
  cũ không phát lại event. Khi bắt đầu phiên mới phải dùng assembler mới để
  không mang revision cũ sang phiên khác; login/logout tích hợp vẫn TODO.
- `FileReceiver.begin/accept_chunk/finish` đã xử lý file local, trùng/sai thứ tự,
  size/hash, quota và dọn file (chi tiết bên dưới). Phía nhận đã nối META →
  ACCEPT, CHUNK → END → COMPLETE qua mock; `send_file()` đã chạy cùng bên nhận. Kiểm tra
  bằng mock không chứng minh truyền file qua mạng.
- HTTP localhost + polling đã chọn. Bridge gọi ClientService; browser không mở
  socket mạng hay gọi PayloadCipher. Xem [UI](../ui/README.md).

### Gửi file bằng ClientService.send_file

- Chạy receive_events nền trước khi gọi `await send_file(recipient, Path(...))`.
  Chỉ một consumer nhận gói; send_file không gọi transport.receive riêng.
- Mỗi client gửi tối đa một file cùng lúc, vẫn nhận chat/file. Mỗi phiên tối đa
  4096 lần gọi gửi được nhận vào xử lý, kể cả lần lỗi ở bước chuẩn bị. Đây là
  quota client, chưa thay thế quota/vòng đời stream của transport.
- File thông thường tối đa 16 MiB. Tính hash và đọc file theo chunk trong thread;
  không đọc toàn file vào RAM. Metadata UTF-8 tối đa 480 byte. Bên nhận vẫn kiểm
  tra tên file và quota của chính nó trước ACCEPT.
- Đăng ký lượt gửi trước khi gửi META; chờ ACCEPT rỗng từ đúng người nhận/ID,
  rồi gửi từng nhóm tối đa 8 chunk bằng TaskGroup. Đợi cả nhóm xong mới đọc nhóm
  tiếp. Đây là giới hạn tác vụ ứng dụng; ACK/window/CUBIC vẫn thuộc transport.
- Lưu message_id đã gửi trong lượt để nhận ERROR liên quan, kể cả lỗi chunk đến
  sau ACK chặng đầu. ERROR chỉ nhận từ server hoặc người nhận, stream control;
  lỗi sai JSON/không liên quan được bỏ qua. Bộ ID có giới hạn bởi size/chunk và
  được xóa khi lượt gửi kết thúc. ERROR của chat chưa được xử lý tại đây.
- Gửi END sau khi mọi send chunk hoàn thành và hash/size đọc lần gửi còn khớp
  metadata. Chỉ nhận COMPLETE trong giai đoạn chờ cuối, đúng người gửi, transfer,
  size (int, không bool), SHA-256 và schema. COMPLETE sớm không được giữ lại.
- Deadline mỗi transport.send là 45s; chờ ACCEPT và COMPLETE mỗi bước 30s sau
  khi send META/END hoàn thành. Đây là cấu hình ban đầu, chưa đo mạng thật. RTO,
  retry/ACK do transport xử lý; ứng dụng không tự gửi lại META với ID mới.
- Trả None chỉ sau COMPLETE hợp lệ. Lỗi phản hồi/I/O/timeout/transport được
  raise; lỗi trong nhóm chunk có thể là ExceptionGroup. Hủy task dừng và đợi
  nhóm send/I/O hiện tại, đóng nguồn file và giải phóng lượt gửi local. Vòng
  nhận dừng cũng làm lượt gửi lỗi. Caller phải await các task trước close transport.
- Sau lỗi/hủy từ khi bắt đầu gửi META, sender dừng các tác vụ rồi thử gửi
  FILE_CANCEL qua control, deadline 3s. Không gửi CANCEL sau completed, trước
  META, hoặc đáp lại một CANCEL hợp lệ từ receiver. Timeout COMPLETE không
  chứng minh bên nhận chưa lưu file thành công.
- Demo `python -m scripts.demo_client_file_send` dùng hai client và relay mock;
  không xác nhận có ACK, truyền qua UDP hoặc đóng stream thật.

### Tiến độ và hủy lượt gửi

- `poll_file_updates() -> list[dict]`: gọi trên event loop của ClientService.
  Mỗi lượt chỉ giữ snapshot mới nhất; không đảm bảo UI thấy mọi trạng thái trung
  gian. Record phía gửi có `type="file"`, `direction="send"`, `transfer_id`,
  `recipient`, `status`, `acknowledged_bytes`, `total_bytes`, `cancel_delivery`.
- `status`: preparing → waiting_accept → sending → verifying → completed;
  đường lỗi kết thúc failed/cancelled. total_bytes là None khi chuẩn bị, sau đó
  là số byte, có thể bằng 0. UI không chia cho 0 khi file rỗng.
- acknowledged_bytes chỉ tăng sau mỗi send chunk thành công, không tính META/END
  hay cộng lại retry. Transport thật xác nhận ACK chặng server; MemoryTransport
  chỉ enqueue nên số liệu mock không phải chứng cứ ACK. Đủ byte vẫn verifying;
  completed chỉ sau COMPLETE đúng size/hash. send_file vẫn raise lỗi cho caller.
- Tối đa 100 record chưa được thu hồi, dùng chung cho cả hai chiều, khóa gồm
  (direction, transfer_id). Poll trả bản sao, xóa record đã kết thúc khi không
  còn chờ cleanup hoặc send COMPLETE, giữ chỗ cho lượt đang chạy. Nếu đầy,
  từ chối lượt gửi mới trước META hoặc từ chối META nhận trước khi tạo .part;
  không xóa kết quả cuối chưa đọc hoặc chặn send để đợi UI. Queue chat vẫn có
  backpressure riêng; HTTP bridge/UI chưa nối vào các API này.
- `await cancel_file(transfer_id) -> bool`: False nếu không có lượt gửi tương
  ứng đang chạy; True khi đã yêu cầu dừng và đợi cleanup local. Caller vẫn phải
  await task send_file, bắt FileTransferCancelled. Hủy trực tiếp task gửi cũng
  cleanup và phát CANCEL, rồi tiếp tục raise asyncio.CancelledError.
- cancel_delivery là not_needed, sent_to_server hoặc failed. sent_to_server
  chỉ nói send CANCEL đã trả về, không xác nhận receiver dọn thành công. Nếu
  CANCEL lỗi/timeout, không che lỗi gốc; receiver vẫn dựa vào expire/close.
- Receiver xử lý CANCEL cùng worker đĩa: đúng chủ mới dọn, giữ ID chống META
  muộn, không xóa file đã hoàn tất. Có thể trả ERROR FILE_CANCELLED hoặc kết quả
  COMPLETE đã cache. Hủy và hoàn tất có thể chạy đua; cancelled ở sender không
  có nghĩa file hoàn chỉnh trên máy nhận đã bị xóa.
- Chưa có API nút hủy lượt nhận ở local. Snapshot nhận mô tả ở phần dưới.
  Demo `python -m scripts.demo_client_file_control` chứng minh tiến độ/hủy qua mock.

### Tiến độ và lỗi phía nhận

- `FileOfferReceiver(..., updates=None)` có thể nhận FileUpdates để xuất trạng
  thái. ClientService truyền buffer chung; worker cập nhật trong thread, polling
  đọc qua lock ngắn. Lock không bao quanh thao tác đĩa/hash/mạng.
- Record có `type="file"`, `direction="receive"`, `transfer_id`, `sender`,
  `status`, `received_bytes`, `total_bytes`, `cleanup_pending`,
  `confirmation_delivery`; thêm name chỉ sau khi metadata đã được kiểm tra.
  received_bytes chỉ tăng sau ghi/đóng chunk mới thành công, không cộng chunk
  trùng, không dùng kích thước .part vì file có thể được ghi lệch thứ tự.
- Trạng thái: preparing → receiving → verifying → completed; hoặc failed,
  cancelled, expired. Đủ received_bytes vẫn receiving khi chưa có END. File rỗng
  có total_bytes=0. completed chỉ sau finish kiểm tra size/hash và lưu/dọn xong.
- Lỗi có code/message an toàn (không chứa đường dẫn hệ thống); thiếu/sai chunk
  hoặc hash dùng FILE_DATA_ERROR. META có header/ID hợp lệ nhưng dữ liệu sai
  cũng có snapshot failed. Sai sender, META trùng, packet không hợp lệ không
  được thay đổi snapshot của lượt đang chạy.
- Hết hạn không có chunk mới trong 45s (mặc định) → expired/FILE_EXPIRED sau khi
  dọn thành công. Đồng bộ cả expire định kỳ và expire bên trong begin file mới.
  Chunk trùng không kéo deadline. Sau dọn thành công, worker gửi ERROR FILE_EXPIRED
  qua control với related_message_id của META gốc. Sender nhận đúng ID sẽ dừng
  lượt gửi; vẫn có deadline riêng nếu thông báo mất hoặc không tới.
- Hủy từ xa → cancelled/FILE_CANCELLED; đóng client khi còn nhận dở →
  cancelled/CLIENT_CLOSED. Lỗi unlink → failed/FILE_STORAGE_ERROR và
  cleanup_pending=true; giữ snapshot/quota để retry. Sau expire/close thành công,
  phát cleanup_pending=false, giữ nguyên trạng thái lỗi ban đầu. Retry cleanup
  qua controller FileOfferReceiver sau khi worker dừng để cập nhật cả snapshot.
- confirmation_delivery: not_needed trước khi hoàn tất, pending khi đã lưu
  file và sắp gửi COMPLETE, sent_to_server khi send trả về, failed nếu send lỗi
  hoặc worker bị hủy trước khi gửi. completed vẫn đúng nếu file đã lưu mà gửi
  COMPLETE thất bại; không xóa file cuối. ACK chặng server không xác nhận UI bên
  gửi đã thấy kết quả. Khi dùng controller trực tiếp, gọi note_reply(reply, bool)
  sau gửi; ClientService đã nối hook này và xử lý lúc shutdown.
- Poll không xóa record terminal còn cleanup_pending hoặc confirmation pending.
  END lặp sau khi UI đã đọc kết quả không tạo lại snapshot. FILE_STATUS_LIMIT
  từ chối META khi buffer 100 record đã đầy; không tạo .part và không ghi đè
  kết quả chưa đọc. META đó vẫn giữ dấu seen, thử lượt mới cần ID mới.
- Demo `python -m scripts.demo_client_receive_status` thử nhận trùng/sai thứ tự,
  hoàn tất, hết hạn với đồng hồ giả và sai hash qua mock.

## Ghép mock và mạng thật

Chạy các demo trong README ở cùng bản code. Mock chỉ đưa Packet vào queue trong
một process, không encode, auth, ACK, loss, window hay CUBIC. Khi transport thật
có lifecycle và phiên đúng, truyền vào ClientService để kiểm thử lại toàn luồng.

Hải phụ trách server/transport/common; Khoa phụ trách client/file/UI và phối hợp
an toàn đầu vào. Đổi API thì cập nhật caller/model/docs cùng thay đổi; không tự
thêm mã hóa, MSG_DELIVERED hay module mới để đáp ứng một bản đề xuất cũ.

## Nhận META/CHUNK/END ở client

- `client/file_offers.py` kiểm tra META, gọi `FileReceiver.begin()` trước khi
  tạo FILE_ACCEPT, trả ERROR nếu dữ liệu/quota/lưu trữ không phù hợp. Không có
  receiver cấu hình thì trả FILE_RECEIVE_DISABLED, không tự tạo thư mục nhận.
- FILE_ACCEPT về người gửi nghiệp vụ, cùng stream ID; gửi qua `server_peer`.
  ERROR đi control, gồm code/message/related_message_id, không lộ đường dẫn máy.
  Các code hiện có: INVALID_FILE_META, FILE_REJECTED, FILE_STORAGE_ERROR,
  FILE_RECEIVE_DISABLED, FILE_ID_REUSED, FILE_SESSION_LIMIT, FILE_STATUS_LIMIT.
- `FileOfferReceiver.handle()` chuyển CHUNK vào `accept_chunk()` và END vào
  `finish()`, sau khi kiểm tra sender khớp META đã chấp nhận. CHUNK thành công
  không trả ACK ở tầng này; ACK vẫn thuộc transport. END chỉ trả COMPLETE
  chứa size/sha256 sau khi finish thành công, cùng transfer ID trong stream_id.
- Lỗi chunk/END dọn lượt nhận và trả ERROR. Các code bổ sung: FILE_NOT_ACTIVE,
  FILE_SENDER_MISMATCH, FILE_DATA_ERROR, FILE_EXPIRED. Sai sender không được
  ghi, hủy hoặc lấy kết quả của lượt khác. Lỗi dọn giữ state/quota để thử lại.
- Cache kết quả COMPLETE hoặc lỗi của lượt đã nhận tối đa 120s; END lặp đúng
  sender trả cùng Packet/message_id, không ghi file lần nữa. Kết quả lỗi giữ
  related_message_id của gói gây lỗi đầu tiên. Chunk muộn sau COMPLETE bị bỏ.
  Gói lặp không kéo dài TTL; hết cache vẫn giữ seen_ids và không mở lại lượt cũ.
  Cache bị chặn bởi quota 4096 ID/phiên, không thay dedup/ACK/CLOSED ở transport.
- Một worker với queue tối đa 8 gói file gọi I/O trong thread theo thứ tự. Vòng
  nhận chat/USER_LIST tiếp tục khi worker bận; queue đầy áp dụng backpressure.
  Không gọi trực tiếp FileReceiver từ worker khác khi nó thuộc ClientService.
- Worker quét expire trước mỗi lượt xử lý và sau tối đa 1 giây chờ queue rỗng;
  lúc đang xử lý/gửi phản hồi, lần quét kế tiếp chờ thao tác đó kết thúc. Deadline
  gửi ACCEPT/COMPLETE/ERROR là 10 giây. Timer cập nhật snapshot hết hạn local
  và chủ động báo ERROR về sender, kể cả lượt được dọn khi begin file mới.
  take_notifications() lấy thông báo một lần trên worker; retry thuộc transport.
  Hàng đợi được chặn bởi quota 4096 ID/phiên; worker xả sau expire/handle. Kết
  quả lỗi cache 120s, không gia hạn vì END lặp; dấu ID giữ tới hết phiên. Nếu
  gửi thông báo timeout lỗi/quá hạn, vòng nhận fail và cleanup như lỗi gửi phản
  hồi khác; nếu bản thân cleanup lỗi thì không khẳng định đã dọn/hết quota.
- `receive_events()` chạy một lần mỗi instance/phiên. Hủy và await task này
  sẽ dừng worker, chờ I/O đang chạy trước khi close FileReceiver, bỏ gói file còn
  trong queue. Caller đóng transport sau đó. Lỗi gửi phản hồi/cleanup làm vòng
  nhận thất bại qua TaskGroup/ExceptionGroup, không bị nuốt; nếu xóa file lỗi,
  receiver giữ state/quota để caller thử close lại sau khi worker đã dừng.
- Giữ tối đa 4096 ID META đã xử lý trong phiên, kể cả bị từ chối/hết hạn; không
  tạo lại file với ID cũ. META lặp đến nghiệp vụ bị trả FILE_ID_REUSED và không
  xóa lượt đang hoạt động. Retry của transport vẫn phải được dedup trước khi
  giao nghiệp vụ. Đây không thay thế bản ghi CLOSED của transport.
- Nếu gửi COMPLETE bị lỗi sau khi file đã lưu, không xóa file hoàn chỉnh;
  snapshot completed có confirmation_delivery=failed. Demo
  `scripts.demo_client_file_receive` chạy toàn nhánh nhận qua mock trong thư
  mục tạm; demo `scripts.demo_client_file_offer` chỉ thử bước META/ACCEPT.

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
