# Mở file nào để bắt đầu?

1. `README.md`: chạy bộ khung và hiểu phần nào đã/chưa có.
2. `TODO_5_NGAY.md`: kế hoạch chung và mốc ghép mỗi tối.
3. **Khoa:** `TODO_KHOA.md`, bắt đầu `client/service.py`.
4. **Bạn A:** `TODO_BAN_A.md`, bắt đầu `common/codec.py` + `server/app.py`.
5. **Cả hai:** chốt `docs/01_PROTOCOL.md` và `docs/02_API.md` ngày 1.

Tìm toàn project bằng `TODO[` trong VS Code. ID A1–A8 và K1–K8 liên kết trực
tiếp với checklist riêng. Chưa hoàn thành thì giữ ô trống; chạy được thật rồi
mới tick. Stub cố ý raise NotImplementedError, không trả kết quả thành công giả.

Chạy ngay để hiểu cách ghép mà không phải chờ server:

```bash
python -m scripts.demo_mock
```
