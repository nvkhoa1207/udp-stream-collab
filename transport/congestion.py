"""Simplified CUBIC, byte budget shared by all streams of one peer/direction.

Uses RFC 9438's cubic target (C=.4, beta=.7), ACK-clocked growth, a small
Reno growth floor, bounded cwnd and one decrease per recovery flight.
No HyStart++, ECN, fast convergence or spurious-loss undo. Loss uses RTO.
"""
import time


class CongestionController:
    def __init__(self, *, clock=time.monotonic, mss=1200, initial_packets=8,
                 max_packets=256):
        self.clock = clock
        self.mss = mss
        self.cwnd = float(initial_packets * mss)
        self.maximum = max_packets * mss
        self.bytes_in_flight = 0
        self.rtt = 0.05
        self.epoch = clock()
        self.w_max = self.cwnd
        self.k = 0.0
        self.sent = 0
        self.recovery_until = 0
        self.loss_events = 0
        self.ack_events = 0
        self.next_send = 0.0

    def budget_bytes(self):
        return max(0, int(self.cwnd) - self.bytes_in_flight)

    def on_sent(self, size):
        if size > self.budget_bytes():
            raise ValueError('Congestion budget exceeded')
        self.bytes_in_flight += size
        self.sent += 1
        # One packet per pacing slot; no accumulated unlimited idle burst.
        self.next_send = max(self.next_send, self.clock()) + max(0.0001, self.rtt * size / self.cwnd)
        return self.sent

    def on_ack(self, acknowledged_bytes, rtt_seconds):
        if acknowledged_bytes <= 0 or acknowledged_bytes > self.bytes_in_flight:
            return
        self.bytes_in_flight -= acknowledged_bytes
        self.ack_events += 1
        if rtt_seconds > 0:
            self.rtt = 0.875 * self.rtt + 0.125 * rtt_seconds
        elapsed = max(0.0, self.clock() - self.epoch)
        target = (0.4 * (elapsed + self.rtt - self.k) ** 3 + self.w_max / self.mss) * self.mss
        reno = self.mss * acknowledged_bytes / self.cwnd
        cubic = max(0.0, target - self.cwnd) * acknowledged_bytes / self.cwnd
        self.cwnd = min(self.maximum, self.cwnd + min(self.mss, max(reno, cubic)))

    def on_loss(self, lost_bytes, *, ticket=None):
        # Bytes remain in flight during retries; a retransmit does not reserve again.
        ticket = self.sent if ticket is None else ticket
        if lost_bytes <= 0 or ticket <= self.recovery_until:
            return
        self.recovery_until = self.sent
        self.w_max = self.cwnd
        self.cwnd = max(2 * self.mss, 0.7 * self.cwnd)
        self.k = ((self.w_max - self.cwnd) / self.mss / 0.4) ** (1 / 3)
        self.epoch = self.clock()
        self.loss_events += 1

    def discard(self, size):
        self.bytes_in_flight = max(0, self.bytes_in_flight - size)
