#!/bin/sh
set -eu
# Test clients and server share this container; impair lo, not host networking.
tc qdisc replace dev lo root netem limit 1024 delay 20ms 5ms loss random 7% duplicate 1% reorder 25% 50% seed 7
trap 'tc qdisc del dev lo root 2>/dev/null || true' EXIT
tc -s qdisc show dev lo
python -m scripts.network_acceptance --clients 10 --sizes 65536 --streams 2
tc -s qdisc show dev lo
