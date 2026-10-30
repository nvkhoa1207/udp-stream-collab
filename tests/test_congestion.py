import unittest
import asyncio
from dataclasses import replace
from transport.congestion import CongestionController
from tests import test_udp_reliability as reliability
from common.models import MessageType as M


class CongestionTests(unittest.TestCase):
    def test_byte_budget_ack_loss_and_recovery_flight(self):
        now = [0.0]
        cc = CongestionController(clock=lambda: now[0])
        initial = cc.cwnd
        ticket = cc.on_sent(1000)
        other = cc.on_sent(500)
        self.assertEqual(cc.budget_bytes(), int(initial) - 1500)
        cc.on_loss(1000, ticket=ticket)
        self.assertEqual(cc.cwnd, initial * 0.7)
        cc.on_loss(500, ticket=other)
        self.assertEqual(cc.loss_events, 1)
        cc.on_ack(1000, 0.1)
        self.assertEqual(cc.bytes_in_flight, 500)
        cc.on_ack(1000, 0.1)  # Bỏ ACK trùng hoặc ACK nhiều byte hơn số đang gửi.
        self.assertEqual(cc.bytes_in_flight, 500)
        now[0] = 12
        before = cc.cwnd
        cc.on_ack(500, 0.1)
        self.assertGreater(cc.cwnd, before)
        self.assertEqual(cc.bytes_in_flight, 0)

    def test_cubic_growth_depends_on_elapsed_time_and_is_capped(self):
        def growth(t):
            now = [0.0]
            cc = CongestionController(clock=lambda: now[0])
            ticket = cc.on_sent(1200)
            cc.on_loss(1200, ticket=ticket)
            now[0] = t
            before = cc.cwnd
            cc.on_ack(1200, 0.1)
            return cc.cwnd - before
        self.assertGreater(growth(10), growth(0))
        cc = CongestionController(initial_packets=2, max_packets=2)
        cc.on_sent(1000)
        cc.on_ack(1000, 0.1)
        self.assertEqual(cc.cwnd, 2400)

    def test_shared_streams_cannot_reserve_more_than_byte_budget(self):
        cc = CongestionController(initial_packets=2)
        cc.on_sent(1200)
        cc.on_sent(1200)
        with self.assertRaises(ValueError):
            cc.on_sent(1)
        cc.discard(1200)
        self.assertEqual(cc.budget_bytes(), 1200)


class CongestionNetworkTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = reliability.ReliabilityTests.asyncSetUp
    asyncTearDown = reliability.ReliabilityTests.asyncTearDown

    async def test_delayed_loss_shared_peer_and_duplicate_ack(self):
        write_a, write_b = self.a._write, self.b._write
        lost = [False]
        handles = []
        def a_write(peer, packet):
            if packet.kind == M.MSG and not lost[0]:
                lost[0] = True
                return
            write_a(peer, packet)
        def b_write(peer, packet):
            if packet.kind == M.ACK:
                handles.append(asyncio.get_running_loop().call_later(0.006, write_b, peer, packet))
                handles.append(asyncio.get_running_loop().call_later(0.008, write_b, peer, packet))
            else:
                write_b(peer, packet)
        self.a._write, self.b._write = a_write, b_write
        try:
            await asyncio.gather(*(self.a.send(self.b.local_address,
                replace(self.packet, stream_id=f'chat-{i%2}')) for i in range(8)))
            cc = self.a._congestion[self.b.local_address]
            self.assertEqual(len(self.a._congestion), 1)
            self.assertEqual(cc.ack_events, 8)
            self.assertEqual(cc.loss_events, 1)
            self.assertEqual(cc.bytes_in_flight, 0)
        finally:
            for handle in handles:
                handle.cancel()
