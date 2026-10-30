"""Hủy file khi server còn gói đang chờ ACK, dùng UDP thật trên localhost."""
import asyncio
import unittest
from common.models import MessageType
from tests import test_team_e2e as e2e


class CancelRelayTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = e2e.TeamE2ETests.asyncSetUp
    asyncTearDown = e2e.TeamE2ETests.asyncTearDown
    client = e2e.TeamE2ETests.client
    event = e2e.TeamE2ETests.event
    transfer = e2e.TeamE2ETests.transfer

    async def test_real_relay_failure_stays_failed_without_duplicate_banner(self):
        a, b = await self.client('hai'), await self.client('khoa')
        source = self.root / 'failure.bin'
        source.write_bytes(b'x' * 131072)
        send = self.app.transport.send
        async def broken_send(peer, packet):
            if packet.kind == MessageType.FILE_CHUNK:
                raise TimeoutError('Injected relay timeout')
            return await send(peer, packet)
        self.app.transport.send = broken_send
        with self.assertRaisesRegex(RuntimeError, 'RELAY_FAILED|File relay failed'):
            await asyncio.wait_for(a.send_file('khoa', source), 5)
        await e2e.until(lambda: not self.app.router.transfers and not b.file_offers.receiver.transfers)
        await asyncio.sleep(0.1)
        for client in (a, b):
            updates = client.poll_file_updates()
            self.assertEqual(updates[-1]['status'], 'failed')
            self.assertTrue(updates[-1]['message'])
            self.assertFalse([e for e in client.events._queue if e['type'] == 'error'])
        self.assertEqual(self.app.router.failures, 1)
        self.assertFalse(list((self.root / 'khoa').glob('*.part')))
        self.app.transport.send = send
        await self.transfer(a, b, 32, 'after-failure.bin')

    async def test_cancel_pending_relay_keeps_session_and_next_file(self):
        a, b = await self.client('hai'), await self.client('khoa')
        await self.transfer(a, b, 32, 'keep.bin')
        saved = next((self.root / 'khoa').glob('*_keep.bin')).read_bytes()
        source = self.root / 'cancel.bin'
        source.write_bytes(b'x' * 131072)
        write = self.app.transport._write
        held = False
        def hold_chunks(peer, packet):
            if held and packet.kind == MessageType.FILE_CHUNK:
                return
            write(peer, packet)
        self.app.transport._write = hold_chunks
        for receiver_cancel in (False, True):
            held = True
            job = asyncio.create_task(a.send_file('khoa', source))
            try:
                await e2e.until(lambda: bool(b.file_offers.owners) and bool(self.app.transport._pending))
                transfer_id = a._file_sender.transfer_id
                await e2e.until(lambda: any(k[1] == transfer_id for k in self.app.transport._pending))
                if receiver_cancel:
                    await b.cancel_receive_file(transfer_id)
                else:
                    await a.cancel_file(transfer_id)
                with self.assertRaises(Exception):
                    await asyncio.wait_for(job, 5)
                held = False
                await e2e.until(lambda: not self.app.router.transfers and not b.file_offers.receiver.transfers)
                await e2e.until(lambda: not self.app.transport._pending)
                await asyncio.sleep(0.1)
                errors = [e for c in (a, b) for e in c.events._queue if e['type'] == 'error']
                self.assertEqual(errors, [])
                self.assertEqual(self.app.router.failures, 0)
                self.assertFalse(list((self.root / 'khoa').glob('*.part')))
                self.assertEqual(b.file_offers.receiver.reserved_bytes, 0)
                self.assertEqual(next((self.root / 'khoa').glob('*_keep.bin')).read_bytes(), saved)
                self.assertIsNotNone(a.session_id)
                self.assertIsNotNone(b.session_id)
                self.assertEqual(a.poll_file_updates()[-1]['status'], 'cancelled')
                self.assertEqual(b.poll_file_updates()[-1]['status'], 'cancelled')
                await a.send_chat('khoa', 'after cancellation')
                self.assertEqual((await self.event(b, 'message'))['text'], 'after cancellation')
                await self.transfer(a, b, 1024, 'after-' + str(receiver_cancel) + '.bin')
            finally:
                held = False
                if not job.done():
                    job.cancel()
                await asyncio.gather(job, return_exceptions=True)
