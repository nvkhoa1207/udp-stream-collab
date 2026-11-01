#!/bin/sh
set -eu
export BTL_KEY_FILE=/tmp/netem-shared.key
python -m security.cipher --generate-key "$BTL_KEY_FILE"
# Client và server cùng container, chỉ tạo lỗi mạng trên lo của container.
tc qdisc replace dev lo root netem limit 1024 delay 20ms 5ms loss random 7% duplicate 1% reorder 25% 50% seed 7
trap 'tc qdisc del dev lo root 2>/dev/null || true' EXIT
tc -s qdisc show dev lo
python -m scripts.network_acceptance --clients 10 --sizes 65536 --streams 2
tc -s qdisc show dev lo
