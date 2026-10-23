"""Owner A. Tài khoản demo cục bộ; không cần làm tính năng đăng ký ngoài đề."""


class AuthService:
    async def verify(self, username: str, password: str) -> bool:
        # TODO[A3]: kiểm tra credentials băm có salt, giới hạn input, không log secret.
        # Bản đầu AUTH qua TCP chưa TLS; ghi rõ giới hạn này. Không bỏ qua auth.
        raise NotImplementedError("TODO[A3]: AuthService.verify")
