# Kiểm tra UI sau R1–R4 — 30/10/2026

R1–R4 được kiểm tra diff, commit riêng ở `42592ae` và push trước khi sửa UI.
Không chạy lại bài 20 client × hai vòng × 380 tin.

## Kết quả tái hiện và phân loại

| Trường hợp | Quan sát trên browser thật | Kết luận và cách xử lý |
| --- | --- | --- |
| Logout còn online cũ | Sau logout vẫn hiện số 1 và tài khoản hai, chat/file đã khóa | Lỗi UI xác nhận được. Xóa danh sách, số online và lựa chọn người nhận khi mất kết nối. Commit `ad51d95`. |
| Server tắt rồi bật lại | Sau heartbeat timeout, login lại được trong cùng tiến trình client; cũng thử logout rồi restart server và login | R4 đã xử lý phục hồi runtime. Browser được kiểm tra riêng; không suy ra từ runtime test. Logout vẫn dọn local nếu gửi LOGOUT bị timeout/lỗi mạng. |
| Lỗi cũ sau F5 | Login lại thành công nhưng F5 hiện đồng thời Connected as hai và Session ended | Lỗi xác nhận được: phát lại event lỗi cũ. Banner lấy snapshot lỗi hiện tại, có đóng theo ID, login thành công giải quyết lỗi phiên. Commit `e8d83ce`. |
| B gửi khi A verifying | Dùng fixture giữ bước xác minh tối đa 20 giây; A ở 100%/Verifying. B được UI báo Send requested, sau đó RuntimeError; A vẫn Completed ở cả hai browser | Giới hạn một file gửi là đúng thiết kế. UI nhận yêu cầu B như thành công là lỗi. Khóa Send file đến khi lượt cũ kết thúc, kiểm tra thêm ở runtime trước khi nhận job. Commit `6abea25`. |
| Cancelled/relay timeout khi gửi B | Không xuất hiện trong lần tái hiện A/B nêu trên | Chưa xác nhận B gây cancelled/relay timeout. Không tuyên bố đã sửa nguyên nhân của báo cáo này. Cần log/transfer ID hoặc bộ bước tái hiện gốc để đối chiếu. |

Không có ảnh báo lỗi gốc của người dùng trong ngữ cảnh đọc được của lượt làm này.
Các kết luận trên dựa vào thao tác và quan sát mới, không coi ảnh kiểm tra mới là ảnh báo lỗi gốc.

Trong code, B bị từ chối trước khi tạo FileSender mới nếu A còn hoạt động.
`RELAY_FAILED` được router phát khi chuyển tiếp tới đích bị lỗi hoặc vượt thời gian;
sender cũng có thời hạn chờ ACCEPT/COMPLETE riêng. Các nhánh này không đủ để kết luận
nguyên nhân sự cố cũ. Bản sửa giữ thông báo lỗi/timeout trên đúng transfer ID để lần
kiểm tra tiếp theo có thông tin rõ hơn thay vì chỉ RuntimeError chung.

## UX đã bổ sung theo lựa chọn của nhóm

- Chat vẫn có Everyone và gửi riêng; file chỉ gửi riêng, tự động nhận, không có Accept.
- Không cho chọn chính mình; ClientService và server đều từ chối self-send. Đây là lựa chọn của nhóm.
- Số online tính cả bản thân và có chú thích. Snapshot online/username tách khỏi lịch sử event.
- 100% byte vẫn hiển thị Verifying, chỉ Completed khi kết thúc kiểm tra/xác nhận.
- Thẻ file có chiều gửi/nhận, người bên kia, tên gốc, tiến độ và nguyên nhân lỗi.
- Cancel khóa khi đã kết thúc, đang xử lý yêu cầu hủy, hoặc bên nhận đã vào bước verifying không còn hủy kịp qua worker.
- Download chỉ hiện cho file nhận Completed. API kiểm tra token, chỉ nhận transfer ID đã đăng ký,
  giới hạn 16 MiB và không mở đường dẫn tùy ý. Không tải file đang verifying hoặc file gửi đi.
- Giữ cách lưu `ID_tên-gốc`; tải xuống dùng tên gốc. Giữ lịch sử và khả năng tải khi logout.
- Lỗi validation được xóa khi sửa input tương ứng; đóng banner không làm mất lỗi mới có ID khác.
  F5 vẫn hiển thị lỗi phiên chưa được giải quyết, không che lỗi đang xảy ra.

## Bằng chứng kiểm tra

Browser localhost chạy qua HTTP bridge và TCP/UDP thật, hai tài khoản thử hai/khoa:

1. Logout: online về 0, recipient về Everyone, chat/file bị khóa; lịch sử file còn.
2. Người nhận logout: browser bên còn lại bỏ tài khoản đó và reset recipient về Everyone.
3. Server restart: đăng nhập lại cùng tiến trình client, không cần khởi động lại bridge.
4. Login sai mật khẩu hiện Login was rejected; server tắt hiện Cannot reach the server;
   heartbeat timeout hiện Session ended. Login thành công rồi F5 không phát lại lỗi phiên cũ.
5. Dismiss lỗi rồi F5: lỗi đã đóng không trở lại. Khi lỗi phiên chưa giải quyết, F5 vẫn giữ lỗi đó.
6. A verifying: chọn B nhưng Send file bị khóa. A xong, gửi B và cả hai file Completed ở hai phía.
7. Everyone và private chat đều xuất hiện đúng nhãn tại browser người nhận.
8. Download thật hai file cùng tên A.txt, nội dung khác nhau; sau F5 vẫn có hai thẻ riêng.
   SHA-256 bản tải khớp từng bản gửi:
   - 24 byte: `edfe312dcdc57b7e59f7dab575d424091c90912c2fbc61892534f5f3d23dd0e6`
   - 41 byte: `0f8b1f39ed997158f377777fb3ad10ccb0a7dd787f0318872b1caeb5a4f15a47`

Hồi quy: **214 test pass trong 76.069 giây**, bật `BTL_NETWORK_TEST=1`, loại rõ
`test_twenty_clients_all_pairs_repeat_and_file` khỏi suite. Có test download/token,
path không hợp lệ, file chưa hoàn tất, file trùng tên, lỗi banner mới đến sau khi đóng,
snapshot mất kết nối, self-send và upload đồng thời. `node --check ui/app.js` và
`git diff --check` cũng pass. Kết quả test và bằng chứng browser được ghi riêng.

## Fixture để tái hiện

Chạy `python -m scripts.demo_network_ui --seconds 1800 --control-dir data/browser-review`
sau khi tạo thư mục điều khiển. Browser dùng HTTP 18000/18001, TCP 19000;
tài khoản hai/khoa dùng mật khẩu thử `test-password`.

- Tạo file rỗng `stop-server` trong thư mục điều khiển để tắt server; xóa file này để bật lại.
  Hai runtime và bridge tiếp tục chạy.
- Tạo `hold-verify` để giữ bước xác minh tối đa 20 giây; xóa để cho xác minh chạy tiếp.
  Đây là trì hoãn có chủ đích để quan sát, không phải chứng cứ lỗi mạng ngoài thực tế.
- `hold-complete` giữ phản hồi COMPLETE để thử timeout; không dùng trong phép thử A/B nêu trên.
- Dừng fixture khi thử xong. Dữ liệu nhận trong fixture là thư mục tạm và được dọn khi đóng.
