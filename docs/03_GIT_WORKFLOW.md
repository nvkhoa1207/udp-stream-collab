# Ghép code trong 5 ngày

Chỉ áp dụng khi nhóm đã tạo repository chung. Starter chưa khởi tạo hoặc push
bất kỳ repository nào. Một người đưa bộ khung lên trước, người kia clone cùng repo.

## Nhánh

- A: `feature/server-transport`.
- Khoa: `feature/client-file-ui`.
- `main`: phiên bản cả hai đã ghép và chạy thử.

Sau khi có commit nền, mỗi người tạo nhánh của mình, ví dụ:

```bash
git switch -c feature/client-file-ui
```

Khi hoàn thành một phần, chọn đúng file mình sửa:

```bash
git status
git add client/ security/ ui/ TODO_KHOA.md
git commit -m "Implement client chat using shared transport"
git push -u origin feature/client-file-ui
```

Thông điệp commit phải phản ánh việc thật đã làm. A thay bằng đường dẫn/nhánh của A.

## Cuối mỗi ngày

1. Commit phần đang làm; kiểm tra không thêm khóa, password, `.venv` hoặc file nhận.
2. Người còn lại review thay đổi API và chạy demo tương ứng.
3. Merge qua PR hoặc cùng ghép local theo quy trình nhóm; không force-push nhánh chung.
4. Mỗi người cập nhật `main` vào nhánh của mình trước khi làm tiếp:

```bash
git fetch origin
git merge origin/main
```

Nếu conflict ở `common/`, cả hai giải quyết cùng nhau. Không chọn toàn bộ ours/
theirs chỉ để mất conflict. Giữ lại một commit chạy được mỗi tối để quay lại khi cần.
