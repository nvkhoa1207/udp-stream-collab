import asyncio
import unittest
from dataclasses import replace

from common.models import MessageType as M, Packet
from transport.reliable_udp import ReliableUdpTransport


class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.a = ReliableUdpTransport(('127.0.0.1', 0), retry_seconds=0.02, max_retries=2)
        self.b = ReliableUdpTransport(('127.0.0.1', 0), retry_seconds=0.02, max_retries=2)
        await self.a.start()
        await self.b.start()
        self.a.register_peer(self.b.local_address, 'a' * 32)
        self.b.register_peer(self.a.local_address, 'a' * 32)
        self.packet = Packet(kind=M.MSG, sender='hai', recipient='khoa', stream_id='chat', payload=b'hello')

    async def asyncTearDown(self):
        await self.a.close()
        await self.b.close()
        self.assertFalse(self.a._pending)

    async def test_lost_data_ack_duplicate_and_late_ack(self):
        write_a, write_b = self.a._write, self.b._write
        lost = set()
        def a_write(peer, packet):
            if packet.kind == M.MSG and 'data' not in lost:
                lost.add('data')
                return
            write_a(peer, packet)
            if packet.kind == M.MSG:
                write_a(peer, packet)
        def b_write(peer, packet):
            if packet.kind == M.ACK and 'ack' not in lost:
                lost.add('ack')
                return
            write_b(peer, packet)
        self.a._write, self.b._write = a_write, b_write
        await self.a.send(self.b.local_address, self.packet)
        self.assertEqual((await self.b.receive()).packet.payload, b'hello')
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.b.receive(), 0.04)
        self.assertEqual(lost, {'data', 'ack'})
        write_b(self.a.local_address, Packet(kind=M.ACK, sender='khoa', recipient='hai',
            stream_id='chat', session_id='a' * 32, ack=1, recv_limit=8))
        await asyncio.sleep(0.01)
        self.assertFalse(self.a._pending)

    async def test_wrong_ack_and_retry_limit_fail_stream(self):
        self.b._write = lambda *args: None
        send = asyncio.create_task(self.a.send(self.b.local_address, self.packet))
        await asyncio.sleep(0.01)
        self.a._received(self.b.local_address, Packet(kind=M.ACK, sender='khoa', recipient='hai',
            stream_id='wrong', session_id='a' * 32, ack=1, recv_limit=8))
        self.assertFalse(send.done())
        with self.assertRaises(TimeoutError):
            await send
        with self.assertRaises(ConnectionError):
            await self.a.send(self.b.local_address, self.packet)

    async def test_remove_peer_and_close_unblock_waiters(self):
        self.b._write = lambda *args: None
        send = asyncio.create_task(self.a.send(self.b.local_address, self.packet))
        await asyncio.sleep(0.01)
        self.a.remove_peer(self.b.local_address)
        with self.assertRaises(ConnectionError):
            await send
        receive = asyncio.create_task(self.a.receive())
        await self.a.close()
        with self.assertRaises(ConnectionError):
            await receive

    async def test_rejected_data_not_acked(self):
        self.b.authorize = lambda *args: False
        with self.assertRaises(TimeoutError):
            await self.a.send(self.b.local_address, self.packet)
        self.assertGreater(self.b.rejected, 0)
