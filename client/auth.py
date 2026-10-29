"""Đăng nhập qua TCP rồi gửi BIND bằng kết nối UDP có sẵn."""
import asyncio
import json
import re
from common.config import MAX_TCP_RECORD_BYTES
from common.models import MessageType, Packet
from common.codec import read_tcp_record


async def read_record(reader):
    return await read_tcp_record(reader, timeout=8)


async def write_record(writer, value):
    body = json.dumps(value).encode('utf-8')
    if len(body) > MAX_TCP_RECORD_BYTES:
        raise ValueError('TCP record is too large')
    writer.write(len(body).to_bytes(4, 'big') + body)
    await writer.drain()


async def authenticate(peer, username, password):
    if type(username) is not str or username == 'server' or re.fullmatch(r'[A-Za-z0-9_-]{1,32}', username) is None:
        raise ValueError('Invalid username')
    if type(password) is not str or not 1 <= len(password) <= 128:
        raise ValueError('Password is empty or too long')
    async with asyncio.timeout(8):
        reader, writer = await asyncio.open_connection(*peer)
        try:
            await write_record(writer, dict(type='HELLO', version=1, client_name='khoa-client'))
            hello = await read_record(reader)
            if (set(hello) != {'type', 'version', 'server_name'} or hello['type'] != 'HELLO_ACK'
                    or type(hello['version']) is not int or hello['version'] != 1
                    or type(hello['server_name']) is not str):
                raise ValueError('Invalid HELLO_ACK')
            await write_record(writer, dict(type='AUTH', username=username, password=password))
            response = await read_record(reader)
            if response.get('type') == 'AUTH_ERROR':
                raise PermissionError('Login was rejected')
            if (set(response) != {'type', 'session_id', 'udp_port'} or response['type'] != 'AUTH_OK'
                    or type(response['session_id']) is not str
                    or re.fullmatch(r'[0-9a-f]{32}', response['session_id']) is None
                    or type(response['udp_port']) is not int or not 1 <= response['udp_port'] <= 65535):
                raise ValueError('Invalid AUTH_OK')
            host = writer.get_extra_info('peername')[0]
            return (host, response['udp_port']), response['session_id']
        finally:
            writer.close()
            await writer.wait_closed()


async def bind_session(transport, peer, username, session_id):
    packet = Packet(kind=MessageType.BIND, sender=username, recipient='server',
                    stream_id='control', session_id=session_id)
    async with asyncio.timeout(9):
        for _ in range(9):
            await asyncio.wait_for(transport.send(peer, packet), timeout=0.5)
            try:
                async with asyncio.timeout(0.5):
                    while True:
                        received = await transport.receive()
                        reply = received.packet
                        if (received.peer == peer and reply.kind == MessageType.BIND_OK
                                and reply.sender == 'server' and reply.recipient == username
                                and reply.session_id == session_id and reply.stream_id == 'control'
                                and reply.seq == 0 and reply.payload == b'' and reply.chunk_index is None):
                            return
            except TimeoutError:
                pass
    raise TimeoutError('No BIND_OK received')
