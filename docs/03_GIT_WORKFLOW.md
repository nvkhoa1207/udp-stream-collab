# Ghép code giữa Khoa và Hải

Repository chung: [udp-stream-collab](https://github.com/nvkhoa1207/udp-stream-collab).
Đồng bộ ngày 23/10/2026 trên nhánh `codex/align-protocol-docs`: tài liệu, hợp đồng
peer/window, code client/file local và hai demo bổ sung. Đây là nhánh để review,
không mặc định đã merge vào `main`.

## Lấy nhánh đồng bộ để thử

Khi working tree của Hải đã sạch hoặc thay đổi đã lưu trên nhánh riêng:

```bash
git fetch origin
git switch --track origin/codex/align-protocol-docs
python -m scripts.demo_mock
python -m scripts.demo_file_chunks
python -m scripts.demo_client_chat
python -m scripts.demo_file_receiver
```

Nếu đã có nhánh local tương ứng thì dùng `git switch codex/align-protocol-docs`.
Không reset/checkout đè phần chưa commit để lấy nhánh. Bốn demo dùng bộ nhớ/đĩa
local; không chứng minh mạng thật. Xem [tiến độ](06_TIEN_DO.md) để biết giới hạn.

## Nhánh phát triển và commit

- Hải: `feature/server-transport`.
- Khoa: `feature/client-file-ui`.
- `main`: phiên bản hai người đã review, ghép và chạy thử.

Chọn đúng file, gồm cả script kiểm tra và thay đổi hợp đồng liên quan. Ví dụ khi
chỉ hoàn thiện client/file và demo:

```bash
git status
git add client/files.py client/service.py scripts/demo_client_chat.py scripts/demo_file_receiver.py TODO_KHOA.md
git diff --cached
git commit -m "Implement local client chat and file receiver demos"
git push -u origin feature/client-file-ui
```

Chạy trên nhánh `feature/client-file-ui` đã tạo/chuyển tới trước đó. Nếu sửa API,
đưa model/config, transport, caller và docs tương ứng vào cùng thay đổi. Không
chỉ push TODO mà bỏ sót code/demo; không dùng `git add .` khi có output/dữ liệu thử.

## Khi ghép

1. Kiểm tra diff, không đưa khóa/password, `.venv`, file nhận hoặc output thử vào commit.
2. Ghi nhánh/commit vào PR hoặc bàn giao; người còn lại chạy demo cùng phiên bản.
3. Merge qua PR hoặc cùng ghép local, không force-push nhánh chung.
4. Cập nhật `main` vào nhánh riêng khi đã lưu thay đổi:

```bash
git fetch origin
git merge origin/main
```

Conflict nội dung là README/docs/API chưa thống nhất; conflict Git là cùng dòng
khác nhau khi merge. Với `common/`, đối chiếu protocol và caller của cả hai, không
chọn toàn bộ ours/theirs chỉ để hết conflict. Giữ commit chạy được để quay lại.
