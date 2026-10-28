"""Mở HTTP UI localhost; đăng nhập trong browser, không đưa password qua CLI."""
import argparse
from pathlib import Path
from ui.bridge import run_bridge


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--http-port', type=int, default=8000)
    parser.add_argument('--received-dir', type=Path, default=Path('data/received'))
    args = parser.parse_args()
    if not 1 <= args.http_port <= 65535:
        parser.error('Cổng HTTP phải từ 1 đến 65535')
    run_bridge(args.http_port, args.received_dir)


if __name__ == '__main__':
    main()
