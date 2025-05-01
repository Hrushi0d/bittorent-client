import asyncio
import logging
import os
import socket


class Peer:
    def __init__(self, ip, port, info_hash, peer_id=None, logger=None):
        self.ip = ip
        self.port = port
        self.logger = logger or logging.getLogger()
        self.info_hash = info_hash
        # Generate or use provided peer_id (20 bytes)
        self.peer_id = peer_id or b'-PC0001-' + os.urandom(12)
        self.reader = None
        self.writer = None
        self.retry_count = 0

    def __repr__(self):
        return f"Peer(ip={self.ip}, port={self.port})"

    async def connect(self, timeout=5):
        backoff_sequence = [1, 2, 4, 8, 16]
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=timeout
            )
            self.logger.info(f"Connected to {self.ip}:{self.port}")
            # Perform handshake immediately after TCP connection
            await self.perform_handshake()
            return True
        except Exception as e:
            self.logger.error(f"Connection to {self.ip}:{self.port} failed: {e}")
            # Exponential backoff retry
            backoff = backoff_sequence[self.retry_count] if self.retry_count < len(backoff_sequence) else backoff_sequence[-1]
            self.retry_count += 1
            await asyncio.sleep(backoff)
            return False

    async def perform_handshake(self):
        pstr = b"BitTorrent protocol"
        handshake = (
            bytes([len(pstr)]) +
            pstr +
            b"\x00" * 8 +
            self.info_hash +
            self.peer_id
        )
        # Send handshake
        self.writer.write(handshake)
        await self.writer.drain()
        # Receive and validate handshake
        response = await self.reader.readexactly(68)
        if response[28:48] != self.info_hash:
            raise ConnectionError("Info hash mismatch during handshake")
        self.logger.info(f"Handshake successful with {self.ip}:{self.port}")

    async def send_interested(self):
        # 'interested' message: length prefix = 1, message ID = 2
        msg = (1).to_bytes(4, 'big') + b"\x02"
        self.writer.write(msg)
        await self.writer.drain()
        self.logger.info(f"Sent 'interested' to {self.ip}:{self.port}")

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info(f"Disconnected from {self.ip}:{self.port}")

    async def __aenter__(self):
        if not await self.connect():
            raise ConnectionError(f"Failed to connect to {self.ip}:{self.port}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
