"""Owner A. Entrypoint có --help; server thật chưa được viết."""

import argparse

from common.config import TCP_PORT, UDP_PORT


def main() -> None:
    parser = argparse.ArgumentParser(description="Server starter — cần làm A2–A7")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp-port", type=int, default=TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=UDP_PORT)
    parser.parse_args()
    # TODO[A2]: truyền args vào ServerApp, asyncio.run(app.run()), shutdown sạch.
    parser.exit(2, "Chưa có server thật. Thực hiện TODO[A2] trong server/app.py.\n")


if __name__ == "__main__":
    main()
