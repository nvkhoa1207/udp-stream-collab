import asyncio
import json
import unittest
from dataclasses import replace

from common.codec import (
    decode_packet,
    encode_packet,
    encode_tcp_record,
    read_tcp_record,
)
from common.config import MAX_PLAINTEXT_BYTES, MAX_TCP_RECORD_BYTES
from common.models import MessageType, Packet


class PacketCodecTests(unittest.TestCase):
    def setUp(self):
        self.packet = Packet(
            kind=MessageType.MSG,
            sender="hai",
            recipient="khoa",
            stream_id="chat-khoa",
            payload="Chào Khoa".encode("utf-8"),
            session_id="a" * 32,
            seq=1,
        )

    def wire(self, **changes):
        record = json.loads(encode_packet(self.packet).decode("utf-8"))
        record.update(changes)
        return json.dumps(record, ensure_ascii=False).encode("utf-8")

    def test_chat_round_trip(self):
        data = encode_packet(self.packet)

        self.assertIsInstance(data, bytes)
        self.assertEqual(decode_packet(data), self.packet)

    def test_binary_file_chunk_round_trip(self):
        packet = replace(
            self.packet,
            kind=MessageType.FILE_CHUNK,
            stream_id="file-001",
            chunk_index=0,
            payload=bytes(range(256)),
        )

        self.assertEqual(decode_packet(encode_packet(packet)), packet)

    def test_ack_and_window_packets(self):
        packets = [
            replace(
                self.packet,
                kind=MessageType.ACK,
                seq=0,
                ack=1,
                recv_limit=8,
                payload=b"",
            ),
            replace(
                self.packet,
                kind=MessageType.WINDOW_UPDATE,
                seq=0,
                recv_limit=0,
                payload=b"",
            ),
            replace(
                self.packet,
                kind=MessageType.WINDOW_PROBE,
                seq=0,
                payload=b"",
            ),
        ]

        for packet in packets:
            with self.subTest(kind=packet.kind):
                self.assertEqual(
                    decode_packet(encode_packet(packet)),
                    packet,
                )

    def test_encode_rejects_invalid_fields(self):
        cases = [
            {"seq": True},
            {"seq": 0},
            {"version": 2},
            {"sender": "*"},
            {"session_id": ""},
            {"message_id": "invalid"},
            {"stream_id": ""},
            {"stream_id": "chat\n"},
            {"ack": 1},
            {"recv_limit": 8},
            {"chunk_index": 0},
            {"payload": b"x" * 481},
        ]

        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    encode_packet(replace(self.packet, **changes))

    def test_decode_rejects_invalid_fields(self):
        cases = [
            {"seq": True},
            {"kind": "UNKNOWN"},
            {"payload": "%%%"},
            {"payload": 123},
            {"sender": "hai khoa"},
            {"ack": 1},
        ]

        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    decode_packet(self.wire(**changes))

    def test_decode_rejects_missing_and_extra_fields(self):
        record = json.loads(encode_packet(self.packet))

        missing = record.copy()
        del missing["session_id"]

        extra = record.copy()
        extra["unexpected"] = 1

        for invalid in (missing, extra):
            with self.assertRaises(ValueError):
                decode_packet(json.dumps(invalid).encode("utf-8"))

    def test_decode_rejects_bad_json(self):
        cases = [
            b"",
            b"\xff",
            b"{",
            b"[]",
            b'{"seq":1,"seq":2}',
            b'{"seq":NaN}',
            b"x" * (MAX_PLAINTEXT_BYTES + 1),
        ]

        for data in cases:
            with self.subTest(data=data[:40]):
                with self.assertRaises(ValueError):
                    decode_packet(data)


class TcpRecordTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_and_length_prefix(self):
        record = {"type": "HELLO", "client_name": "Hải"}
        data = encode_tcp_record(record)

        self.assertEqual(
            int.from_bytes(data[:4], byteorder="big"),
            len(data[4:]),
        )

        reader = asyncio.StreamReader()
        reader.feed_data(data)

        self.assertEqual(await read_tcp_record(reader), record)

    async def test_fragmented_record(self):
        record = {"type": "HELLO", "version": 1}
        data = encode_tcp_record(record)
        reader = asyncio.StreamReader()

        task = asyncio.create_task(read_tcp_record(reader))

        try:
            reader.feed_data(data[:2])
            await asyncio.sleep(0)
            reader.feed_data(data[2:7])
            await asyncio.sleep(0)
            reader.feed_data(data[7:])

            self.assertEqual(await task, record)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_two_records_in_one_stream(self):
        first = {"type": "HELLO", "version": 1}
        second = {"type": "AUTH", "username": "hai"}
        reader = asyncio.StreamReader()

        reader.feed_data(
            encode_tcp_record(first) + encode_tcp_record(second)
        )

        self.assertEqual(await read_tcp_record(reader), first)
        self.assertEqual(await read_tcp_record(reader), second)

    async def test_rejects_invalid_length(self):
        for size in (0, MAX_TCP_RECORD_BYTES + 1):
            with self.subTest(size=size):
                reader = asyncio.StreamReader()
                reader.feed_data(size.to_bytes(4, byteorder="big"))

                with self.assertRaises(ValueError):
                    await read_tcp_record(reader)

    async def test_incomplete_body(self):
        reader = asyncio.StreamReader()
        reader.feed_data((10).to_bytes(4, byteorder="big") + b"{}")
        reader.feed_eof()

        with self.assertRaises(asyncio.IncompleteReadError):
            await read_tcp_record(reader)

    async def test_timeout(self):
        reader = asyncio.StreamReader()

        with self.assertRaises(TimeoutError):
            await read_tcp_record(reader, timeout=0.05)

    async def test_rejects_invalid_json_body(self):
        for body in (b"\xff", b"[]", b'{"x":1,"x":2}'):
            with self.subTest(body=body):
                reader = asyncio.StreamReader()
                reader.feed_data(
                    len(body).to_bytes(4, byteorder="big") + body
                )

                with self.assertRaises(ValueError):
                    await read_tcp_record(reader)

    async def test_encoder_rejects_oversized_record(self):
        with self.assertRaises(ValueError):
            encode_tcp_record({"text": "x" * MAX_TCP_RECORD_BYTES})


if __name__ == "__main__":
    unittest.main()