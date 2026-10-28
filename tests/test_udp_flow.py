import asyncio
import unittest
from dataclasses import replace
from common.models import MessageType as M
from tests import test_udp_reliability as reliability


class FlowTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = reliability.ReliabilityTests.asyncSetUp
    asyncTearDown = reliability.ReliabilityTests.asyncTearDown

    async def test_full_window_lost_update_probe_and_independent_stream(self):
        peer = self.b.local_address
        await asyncio.gather(*(self.a.send(peer, self.packet) for _ in range(8)))
        ninth = asyncio.create_task(self.a.send(peer, self.packet))
        await asyncio.sleep(0.025)
        self.assertFalse(ninth.done())
        self.assertEqual(len(self.b._rx[(self.a.local_address, 'chat')].packets), 8)
        await self.a.send(peer, replace(self.packet, stream_id='other'))
        write = self.b._write
        dropped = [0]
        def filtered(peer, packet):
            if packet.kind == M.WINDOW_UPDATE and packet.stream_id == 'chat' and dropped[0] < 2:
                dropped[0] += 1
                return
            write(peer, packet)
        self.b._write = filtered
        self.assertEqual((await self.b.receive()).packet.seq, 1)
        await asyncio.wait_for(ninth, 1)
        self.assertEqual(dropped[0], 2)
        self.assertEqual(self.a._tx[(peer, 'chat')].limit, 9)

    async def test_reorder_ordered_delivery_and_stale_credit(self):
        write = self.a._write
        held = []
        def reorder(peer, packet):
            if packet.kind == M.MSG and packet.seq == 1 and not held:
                held.append(packet)
            else:
                write(peer, packet)
                if packet.kind == M.MSG and packet.seq == 2 and held:
                    write(peer, held[0])
        self.a._write = reorder
        await asyncio.gather(*(self.a.send(self.b.local_address, self.packet) for _ in range(2)))
        self.assertEqual([(await self.b.receive()).packet.seq for _ in range(2)], [1, 2])
        await asyncio.sleep(0.01)
        state = self.a._tx[(self.b.local_address, 'chat')]
        limit = state.limit
        from common.models import Packet
        self.a._received(self.b.local_address, Packet(kind=M.WINDOW_UPDATE, sender='khoa',
            recipient='hai', stream_id='chat', session_id='a'*32, recv_limit=0))
        self.assertEqual(state.limit, limit)

    async def test_quota_zero_window_stall_and_recovery(self):
        self.b.max_streams = 2  # control + one data stream
        await self.a.send(self.b.local_address, replace(self.packet, stream_id='first'))
        await self.b.receive()
        blocked = asyncio.create_task(self.a.send(self.b.local_address, replace(self.packet, stream_id='second')))
        await asyncio.sleep(0.04)
        self.assertFalse(blocked.done())
        self.b.finish_stream(self.a.local_address, 'first')
        await asyncio.wait_for(blocked, 1)
        with self.assertRaises(TimeoutError):
            await self.a.send(self.b.local_address, replace(self.packet, stream_id='third'))

    async def test_140_streams_tombstone_late_data_and_quota(self):
        for i in range(140):
            stream = f'file{i}'
            await self.a.send(self.b.local_address, replace(self.packet, stream_id=stream))
            received = await self.b.receive()
            self.a.finish_stream(self.b.local_address, stream)
            self.b.finish_stream(self.a.local_address, stream)
        self.assertEqual(len(self.b._rx), 1)
        self.assertEqual(len(self.b._tombstones), 140)
        self.a._write(self.b.local_address, received.packet)
        self.a._write(self.b.local_address, replace(received.packet,
            kind=M.WINDOW_PROBE, seq=0, payload=b'', chunk_index=None))
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.b.receive(), 0.03)
        self.assertEqual(len(self.b._rx), 1)
        # A full history never evicts a live tombstone to admit another stream.
        for i in range(4096 - 140):
            self.b._tombstones[(self.a.local_address, f'old{i}')] = (0, False)
        self.assertFalse(self.b._can_reserve(self.a.local_address, 'fresh'))
        self.assertTrue(self.b._can_reserve(self.a.local_address, 'control'))
        self.assertTrue(self.b._can_reserve(self.a.local_address, 'chat-peer'))

    async def test_lost_initial_probe_and_update_recover(self):
        a_write, b_write = self.a._write, self.b._write
        dropped = set()
        def a_send(peer, packet):
            if packet.kind == M.WINDOW_PROBE and 'probe' not in dropped:
                dropped.add('probe')
                return
            a_write(peer, packet)
        def b_send(peer, packet):
            if packet.kind == M.WINDOW_UPDATE and 'update' not in dropped:
                dropped.add('update')
                return
            b_write(peer, packet)
        self.a._write, self.b._write = a_send, b_send
        await self.a.send(self.b.local_address, self.packet)
        self.assertEqual((await self.b.receive()).packet.payload, b'hello')
        self.assertEqual(dropped, {'probe', 'update'})

    async def test_reserved_control_buffer_does_not_advertise_credit_implicitly(self):
        packet = replace(self.packet, stream_id='control', seq=1, session_id='a'*32)
        self.a._write(self.b.local_address, packet)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.b.receive(), 0.03)
        self.assertFalse(self.b._rx[(self.a.local_address, 'control')].packets)

    async def test_close_drains_senders_waiting_for_congestion_budget(self):
        cc = self.a._congestion[self.b.local_address]
        cc.bytes_in_flight = int(cc.cwnd)
        sending = asyncio.create_task(self.a.send(self.b.local_address, self.packet))
        await asyncio.sleep(0.02)
        await self.a.close()
        self.assertFalse(self.a._pending)
        self.assertFalse(self.a._send_completion)
        with self.assertRaises(ConnectionError):
            await sending

    async def test_cancelled_close_caller_does_not_interrupt_shared_cleanup(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def wait_lost():
            entered.set()
            await release.wait()
        self.a._lost.wait = wait_lost
        closing = asyncio.create_task(self.a.close())
        await entered.wait()
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        release.set()
        await self.a.close()
        self.assertFalse(self.a._peers)
        self.assertFalse(self.a._pending)
        with self.assertRaises(ConnectionError):
            self.a.register_peer(self.b.local_address, 'b'*32)
