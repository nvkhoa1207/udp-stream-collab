// Fixture tĩnh để Khoa làm UI trước; không phải user/tin nhắn thật.
const fixtures = {
  users: ["khoa (giả)", "ban-a (giả)"],
  messages: ["Dữ liệu mẫu: hoàn thiện TODO K6 rồi nối ClientService qua bridge."],
};

function addTextItem(listId, text) {
  const item = document.createElement("li");
  item.textContent = text;
  document.getElementById(listId).appendChild(item);
}

fixtures.users.forEach((user) => addTextItem("users", user));
fixtures.messages.forEach((message) => addTextItem("messages", message));

// TODO[K6]: render event users/message/progress/status/error từ HTTP bridge.
// TODO[K6]: chỉ bật nút khi đã login/bind thật; xử lý fetch lỗi và logout.
// TODO[K6]: recipient private, giới hạn text theo byte, file/progress/FILE_COMPLETE.
// Không đưa file lớn thành base64 JSON khổng lồ; không dùng WebSocket.
