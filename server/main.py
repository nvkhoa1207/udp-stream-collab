"""Chạy TCP login và UDP relay; tài khoản demo nhập khi khởi động."""

import argparse
import asyncio
import getpass
import logging

from common.config import TCP_PORT, UDP_PORT, configure_console
from server.app import ServerApp
from server.auth import AuthService


def main() -> None:
    configure_console()
    parser = argparse.ArgumentParser(
        description="TCP login + reliable UDP chat/file relay"
    )

    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp-port", type=int, default=TCP_PORT)
    parser.add_argument(
        "--udp-port",
        type=int,
        default=UDP_PORT,
        help="Cổng UDP chat/file",
    )

    args = parser.parse_args()

    if not all(
        1 <= port <= 65535
        for port in (args.tcp_port, args.udp_port)
    ):
        parser.error("Cổng phải từ 1 đến 65535")

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    try:
        auth = AuthService()

        for username in ("hai", "khoa"):
            password = getpass.getpass(
                f"Mật khẩu demo cho {username}: "
            )
            auth.add_user(username, password)
            del password

        app = ServerApp(
            host=args.host,
            tcp_port=args.tcp_port,
            udp_port=args.udp_port,
            auth=auth,
            enable_udp=True,
        )
        asyncio.run(app.run())

    except (KeyboardInterrupt, EOFError):
        pass

    except (OSError, ValueError) as exc:
        parser.exit(1, f"Không thể chạy server: {exc}\n")


if __name__ == "__main__":
    main()
