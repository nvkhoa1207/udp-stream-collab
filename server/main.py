"""Entrypoint cho TCP handshake server."""

import argparse
import asyncio
import logging

from common.config import TCP_PORT, UDP_PORT
from server.app import ServerApp


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TCP handshake server (A2)"
    )

    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--tcp-port", type=int, default=TCP_PORT)
    parser.add_argument(
        "--udp-port",
        type=int,
        default=UDP_PORT,
        help="Dành cho bước sau; hiện chưa mở socket UDP",
    )

    args = parser.parse_args()

    if not 1 <= args.tcp_port <= 65535:
        parser.error("--tcp-port phải từ 1 đến 65535")

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    try:
        app = ServerApp(
            host=args.host,
            tcp_port=args.tcp_port,
        )
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        parser.exit(1, f"Không thể mở TCP server: {exc}\n")


if __name__ == "__main__":
    main()