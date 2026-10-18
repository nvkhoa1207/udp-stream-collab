"""Owner Khoa. Entrypoint có --help; client mạng thật chưa được viết."""

import argparse

from common.config import TCP_PORT, UDP_PORT


def main() -> None:
    parser = argparse.ArgumentParser(description="Client starter — cần làm K1–K7")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp-port", type=int, default=TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=UDP_PORT)
    parser.add_argument("--username", required=True)
    parser.parse_args()
    # TODO[K1]: tạo service, kết nối/login, nhận event nền; không truyền password qua CLI.
    parser.exit(2, "Chưa có client thật. Thực hiện TODO[K1] trong client/service.py.\n")


if __name__ == "__main__":
    main()
