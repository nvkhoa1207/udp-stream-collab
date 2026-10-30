# Kiểm tra cuối trên hai/server-transport — 30/10/2026

Đợt này tiếp nối `b806598`, không merge main và không gỡ Download.
Kết quả cũ nằm ở [05-browser-ui-review.md](05-browser-ui-review.md).

## Lỗi xác nhận được và thay đổi

| Nhóm | Nguyên nhân hoặc yêu cầu | Thay đổi đã push |
| --- | --- | --- |
| Người nhận | Nhóm chọn hiển thị bản thân nhưng không cho tự gửi | `8c5c9c6`: hiện `(You)` và khóa dòng/option tương ứng. Số online tính cả bản thân. Backend vẫn chặn tự gửi. |
| Cancel và relay | Đóng stream khi Cancel đánh thức các lượt gửi đang chờ bằng ConnectionError. Router hiểu nhầm việc đóng chủ động này là lỗi relay; client còn đưa lỗi lên banner chung. | `238c12a`: dùng StreamClosedError và trạng thái lượt đã kết thúc để nhận biết đóng chủ động. Lỗi relay thật vẫn được ghi Failed với nguyên nhân trên đúng lượt. |
| Chat sau F5 | Tin gửi chỉ được thêm trực tiếp vào DOM; runtime chỉ giữ tin nhận nên F5 mất tin gửi. | `890ecc0`: cả hai chiều dùng chung hàng đợi event RAM, có message ID, sender, recipient và trạng thái. HTTP không thêm bản sao vào DOM. |

Lỗi Cancel được tái hiện riêng: trước sửa, bấm Cancel tạo card Cancelled kèm
banner relay timeout. Test UDP trước sửa ghi nhận 5 lỗi relay thừa từ các gói
đang chờ. Sau sửa, sender/receiver Cancel không tạo banner đó. Không bỏ qua mọi
exception: timeout thật vẫn tăng bộ đếm relay failure, dọn lượt và báo Failed.
Runtime lấy cập nhật file trước lỗi liên quan để card mới có sẵn khi gắn lỗi.

Báo cáo cũ “gửi B làm A cancelled khi A verifying” vẫn chưa xác nhận được nguyên
nhân. Không đồng nhất báo cáo đó với race Cancel đã tái hiện. Giới hạn một lượt
gửi file vẫn giữ: B bị chặn khi A còn hoạt động; 100% byte chưa phải Completed.

## Chính sách dữ liệu và trạng thái

- Lịch sử dùng hàng đợi sẵn có, tối đa 512 event, không thêm database hay lịch sử
  vĩnh viễn. F5 lấy lại event còn giữ. Hai tin cùng nội dung vẫn có ID khác nhau.
- Tin gửi hiện `Send requested`, không khẳng định đã giao/đã đọc. Lỗi gửi được
  ghép bằng ID và hiện Failed; cập nhật trạng thái không thêm bản sao tin nhắn.
- Logout rồi login cùng username có thể giữ lịch sử và Download trong tiến trình
  client. Login thành công bằng username khác xóa lịch sử RAM, lỗi và danh sách
  Download cũ. File trên đĩa không bị xóa. Hiện danh tính này dựa trên username.
- Download và registry giới hạn sẵn có được giữ nguyên: chỉ file nhận hoàn tất,
  tên lưu có ID để tránh ghi đè. Không thêm đường dẫn tùy ý hoặc preview.
- Lỗi phiên chưa giải quyết vẫn hiện sau F5; Dismiss theo ID và login thành công
  xử lý lỗi tương ứng. Không xóa banner thật chỉ để màn hình trông sạch.

## Browser thật

Dùng hai browser nối hai HTTP bridge trong cùng tiến trình thử nghiệm, với server
TCP/UDP trên loopback. Không suy ra các kết quả này từ mock hoặc runtime test.

| Thao tác | Kết quả |
| --- | --- |
| Hai tài khoản online | Số online là 2; bản thân có `(You)`, khóa ở list và dropdown. |
| Cancel phía gửi và phía nhận khi chunk đang chờ | Hai phía Cancelled, không có banner relay thừa; F5 không phát lại. Chat và file B tiếp theo hoạt động, B Completed. |
| Gửi xen kẽ HP1, KP1, HA1, KA1, HP2, KP2, HA2, KA2 | Cả hai browser có đủ 8 tin theo thứ tự; private/broadcast đúng. F5 hai lần mỗi bên vẫn đủ 8, không nhân đôi. |
| Gửi hai tin cùng nội dung | F5 vẫn giữ hai tin riêng, không gộp theo nội dung. |
| Tắt server rồi gửi chat | Tin chuyển Failed với UDP retry limit reached; F5 giữ Failed và banner phiên đã kết thúc. |
| Bật server lại | Login lại được trên cùng bridge/client. Logout khi server tắt vẫn dọn local, online 0, khóa chat/file. |
| Đổi tài khoản trên bridge đã có lịch sử | Chat/file card về 0; F5 vẫn không lộ lịch sử tài khoản trước. |
| Nhận B.txt, F5 rồi Download | Tải được 24 byte, SHA-256 khớp nguồn: `01e8d62f0b89ff07e99bbE6448877b4f44de9887daE0623c91ed6b899d0cd356`. |

Cancel dùng fixture `hold-relay` giữ chunk bằng cách bỏ lượt ghi UDP để tạo gói
đang chờ ACK; đây là fault injection có kiểm soát. Thử server tắt/bật dùng cờ
`stop-server`, giữ nguyên tiến trình client. Không gọi các phép thử này là LAN.

Ảnh bằng chứng lưu ngoài repo: `final-self-disabled.jpg`,
`final-cancel-no-relay.jpg`, `final-chat-history-f5.jpg` trong thư mục visualization
của phiên làm việc. Mapping nhiều file trùng tên đã kiểm tra browser ở đợt trước;
đợt này giữ regression tương ứng và kiểm tra thêm Download sau F5.

## Kiểm tra tự động cuối

```powershell
$env:BTL_NETWORK_TEST='1'
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
node --check ui/app.js
git diff --check
```

Kết quả cuối: **222 test pass, 79.617 giây**. Log ở
`data/browser-review/final-regression.log` (không đưa dữ liệu chạy thử vào git).
Kiểm tra cú pháp JavaScript và whitespace diff đều pass.

- Bộ đầy đủ có bài 20 client, hai vòng, mỗi vòng 380 tin gửi riêng. Kiểm tra đủ
  người gửi/người nhận/nội dung, không thiếu/trùng, replay không thêm tin; gửi
  file 1 KiB sau chat, so sánh byte và kiểm tra hoàn tất qua giao thức hash,
  rồi kiểm tra dọn phiên/stream.
- Cancel: cả hai phía, chunk đang chờ, Cancel muộn/lặp sau COMPLETE, giữ file đã
  hoàn tất, dọn part/quota, chat và gửi file mới. Timeout relay được bơm riêng
  phải Failed có lý do ở đúng lượt, không biến thành Cancelled.
- Lịch sử: thứ tự, ID riêng cho nội dung trùng, bounded history, lỗi gửi và lỗi
  server có liên hệ đúng tin. Đổi tài khoản dọn RAM nhưng giữ file vật lý.
- Giữ regression R1–R4, một file tại một thời điểm, Download đúng ID/trùng tên,
  file mất hoặc chưa hoàn tất, kiểm tra token/Host/Origin và chặn đường dẫn tùy ý.

Chưa kiểm tra LAN hai máy, Docker Engine hoặc netem. Không dùng kết quả loopback,
fault injection hay unit test để khẳng định các môi trường này đã hoạt động.
