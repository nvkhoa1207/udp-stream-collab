"""Kiểm tra ciphertext trên đường gửi thật, sai khóa/tag và không hạ cấp."""
import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from common.codec import decode_packet, encode_packet, encode_tcp_record, read_tcp_record
from common.config import MAX_DATAGRAM_BYTES, MAX_TCP_RECORD_BYTES
from common.models import MessageType, Packet
from security.cipher import PayloadCipher, generate_key
from security.wire import OVERHEAD, get_cipher, open_record, seal_record
from transport.reliable_udp import ReliableUdpTransport, _Protocol


class EncryptionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.key_path = Path(self.folder.name) / 'test.key'
        generate_key(self.key_path)
        self.environment = patch.dict(os.environ, BTL_KEY_FILE=str(self.key_path))
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.packet = Packet(kind=MessageType.FILE_CHUNK, sender='hai', recipient='khoa',
            session_id='a'*32, stream_id='f'*32, seq=1, chunk_index=0,
            payload=b'private-file-content' * 24)

    def test_key_creation_never_overwrites_existing_key(self):
        before = self.key_path.read_bytes()
        self.assertEqual(len(before), 32)
        with self.assertRaises(FileExistsError):
            generate_key(self.key_path)
        self.assertEqual(self.key_path.read_bytes(), before)

    def test_missing_or_invalid_key_fails_closed(self):
        for name in ('missing.key', 'short.key'):
            path = Path(self.folder.name) / name
            if name == 'short.key':
                path.write_bytes(b'bad')
            with patch.dict(os.environ, BTL_KEY_FILE=str(path)):
                with self.assertRaises(ValueError):
                    seal_record(b'secret', b'UDP')

    def test_wrong_key_tampering_and_channel_mismatch_rejected(self):
        data = seal_record(b'secret', b'UDP')
        other = PayloadCipher(os.urandom(32))
        with self.assertRaises(ValueError):
            other.open(data[16:], data[4:16], b'BTL1UDP')
        for index in (0, 4, 16, len(data)-1):
            damaged = bytearray(data)
            damaged[index] ^= 1
            with self.subTest(index=index), self.assertRaises(ValueError):
                open_record(bytes(damaged), b'UDP')
        with self.assertRaises(ValueError):
            open_record(data, b'TCP')

    def test_udp_actual_write_encrypted_and_retry_uses_fresh_nonce(self):
        transport = ReliableUdpTransport(('127.0.0.1', 0))
        transport._socket = Mock()
        peer = ('127.0.0.1', 9001)
        transport._write(peer, self.packet)
        transport._write(peer, self.packet)
        first, second = [call.args[0] for call in transport._socket.sendto.call_args_list]
        self.assertNotEqual(first[4:16], second[4:16])
        for data in (first, second):
            self.assertLessEqual(len(data), MAX_DATAGRAM_BYTES)
            self.assertNotIn(self.packet.payload, data)
            self.assertEqual(decode_packet(open_record(data, b'UDP')), self.packet)

    def test_udp_rejects_cleartext_and_corruption_before_dispatch(self):
        owner = Mock(rejected=0)
        protocol = _Protocol(owner)
        valid = seal_record(encode_packet(self.packet), b'UDP')
        damaged = valid[:-1] + bytes([valid[-1] ^ 1])
        for data in (encode_packet(self.packet), damaged, b'BTL1'):
            protocol.datagram_received(data, ('127.0.0.1', 9001))
        owner._received.assert_not_called()
        self.assertEqual(owner.rejected, 3)
        protocol.datagram_received(valid, ('127.0.0.1', 9001))
        owner._received.assert_called_once_with(('127.0.0.1', 9001), self.packet)

    async def test_tcp_password_encrypted_and_fragmented_round_trip(self):
        record = dict(type='AUTH', username='khoa', password='secret-password-test')
        data = encode_tcp_record(record)
        self.assertNotIn(b'secret-password-test', data)
        reader = asyncio.StreamReader()
        for byte in data:
            reader.feed_data(bytes([byte]))
        self.assertEqual(await read_tcp_record(reader), record)

    async def test_tcp_rejects_cleartext_wrong_key_and_damaged_tag(self):
        good = encode_tcp_record(dict(type='AUTH', password='secret'))
        other_path = Path(self.folder.name) / 'other.key'
        generate_key(other_path)
        with patch.dict(os.environ, BTL_KEY_FILE=str(other_path)):
            wrong_key = encode_tcp_record(dict(type='AUTH', password='secret'))
        plain = b'{"type":"AUTH"}'
        for data in (len(plain).to_bytes(4, 'big') + plain, wrong_key,
                     good[:-1] + bytes([good[-1] ^ 1])):
            reader = asyncio.StreamReader()
            reader.feed_data(data)
            with self.assertRaises(ValueError):
                await read_tcp_record(reader)

    async def test_tcp_limit_includes_encryption_overhead(self):
        # Compact JSON {"x":""} chiếm 8 byte.
        record = {'x': 'a' * (MAX_TCP_RECORD_BYTES - OVERHEAD - 8)}
        data = encode_tcp_record(record)
        self.assertEqual(len(data) - 4, MAX_TCP_RECORD_BYTES)
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        self.assertEqual(await read_tcp_record(reader), record)
        with self.assertRaises(ValueError):
            encode_tcp_record({'x': record['x'] + 'a'})
