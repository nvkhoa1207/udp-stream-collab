"""Mở HTTP UI localhost; đăng nhập trong browser, không đưa password qua CLI."""
import argparse
from pathlib import Path
from ui.bridge import run_bridge
from common.config import configure_console
from security.wire import get_cipher


def main():
    configure_console()
    parser = argparse.ArgumentParser(
        description='Open the local web client. Log in from the browser.'
    )
    parser.add_argument('--http-port', type=int, default=8000)
    parser.add_argument('--received-dir', type=Path, default=Path('data/received'))
    args = parser.parse_args()
    if not 1 <= args.http_port <= 65535:
        parser.error('HTTP port must be between 1 and 65535')
    try:
        get_cipher()
    except ValueError as exc:
        parser.error(str(exc))
    run_bridge(args.http_port, args.received_dir)


if __name__ == '__main__':
    main()
