import asyncio
import hashlib
import logging
import os
import struct
from asyncio import StreamReader, StreamWriter

from utils._DownloadQueue import DownloadQueue

def _create_request_message(block_length, block_offset, piece_index):
    message_id = 6
    payload = struct.pack("!III", piece_index, block_offset, block_length)
    message_length = 1 + len(payload)
    return struct.pack("!IB", message_length, message_id) + payload


def verify_piece_hash(piece_data, expected_hash):
    piece_hash = hashlib.sha1(piece_data).digest()
    return piece_hash == expected_hash


class Peer:
    CHOKE = 0
    UNCHOKE = 1
    def __init__(self, ip: str, port: int, info_hash: bytes, logger: logging.Logger, peer_id: bytes = None):
        self.ip = ip.decode() if isinstance(ip, bytes) else ip
        self.port = port
        self.logger = logger
        self.info_hash = info_hash

        self.reader: StreamReader | None = None
        self.writer: StreamWriter | None = None

        self.peer_id = peer_id or b'-PC0001-' + os.urandom(12)
        self.connected = False
        self.handshaked = False
        self.interested_sent = False
        self.unchoked = False
        self.download_queue = DownloadQueue(logger=self.logger, port=self.port, ip=self.ip)

    def __repr__(self):
        return (
            f"Peer(ip={self.ip}, port={self.port}, "
            f"connected={self.connected}, handshaked={self.handshaked}, "
            f"interested_sent={self.interested_sent})"
        )

    async def _connect(self, timeout: int = 5) -> bool:
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port), timeout=timeout
            )
            self.connected = True
            self.logger.info(f"Peer - Connected to {self.ip}:{self.port}")
            await self.perform_handshake()
            return True
        except Exception as e:
            self.logger.error(f"Peer - Connection to {self.ip}:{self.port} failed: {e}")
            self.connected = False
            return False

    async def perform_handshake(self):
        if not self.reader or not self.writer:
            raise ConnectionError("Peer - Cannot handshake, connection not established")
        pstr = b"BitTorrent protocol"
        handshake = (
            bytes([len(pstr)]) +
            pstr +
            b"\x00" * 8 +
            self.info_hash +
            self.peer_id
        )
        self.writer.write(handshake)
        await self.writer.drain()
        response = await self.reader.readexactly(68)

        if response[28:48] != self.info_hash:
            raise ConnectionError("Peer - Info hash mismatch during handshake")

        self.handshaked = True
        self.logger.info(f"Peer - Handshake successful with {self.ip}:{self.port}")

    async def send_interested(self):
        msg = (1).to_bytes(4, 'big') + b"\x02"
        self.writer.write(msg)
        await self.writer.drain()
        self.interested_sent = True
        self.logger.info(f"Peer - Sent 'interested' to {self.ip}:{self.port}")

    async def send_unchoke(self):
        await self._send_simple_message(Peer.UNCHOKE, "UNCHOKE")

    async def send_choke(self):
        await self._send_simple_message(Peer.CHOKE, "CHOKE")

    async def _send_simple_message(self, msg_id: int, label: str):
        if not self.writer:
            self.logger.warning(f"Peer - Tried to send {label} to {self.ip}:{self.port}, but writer is None.")
            return
        msg = (1).to_bytes(4, 'big') + bytes([msg_id])
        self.writer.write(msg)
        await self.writer.drain()
        self.logger.info(f"Peer - Sent {label} to {self.ip}:{self.port}")

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info(f"Peer - Disconnected from {self.ip}:{self.port}")
            self.connected = False

    async def wait_for_unchoke(self, timeout: int = 10) -> bool:
        # async with self._read_lock:
        if self.unchoked:
            self.logger.debug(f"Peer - Already unchoked by {self.ip}:{self.port}, skipping.")
            return True

        if not self.writer or not self.reader:
            self.logger.warning(f"Peer - Cannot wait for UNCHOKE from {self.ip}:{self.port}, no connection.")
            return False

        self.logger.info(f"Peer - Waiting for UNCHOKE from {self.ip}:{self.port}...")

        try:
            while True:
                header = await asyncio.wait_for(self.reader.readexactly(4), timeout=timeout)
                length = int.from_bytes(header, 'big')
                if length == 0:
                    self.logger.debug(f"Peer - Keep-alive from {self.ip}:{self.port}")
                    continue
                msg_id = await self.reader.readexactly(1)
                payload = await self.reader.readexactly(length - 1) if length > 1 else b''

                if msg_id == b'\x01':  # UNCHOKE
                    self.unchoked = True
                    self.logger.info(f"Peer - Peer {self.ip}:{self.port} unchoked us.")
                    return True
                elif msg_id in [b'\x04', b'\x05', b'\x06']:
                    self.logger.debug(f"Peer - Got {msg_id.hex()} from {self.ip}:{self.port}, still waiting for UNCHOKE.")
                else:
                    self.logger.debug(f"Peer - Ignored msg {msg_id.hex()} from {self.ip}:{self.port}")

        except asyncio.TimeoutError:
            self.logger.warning(f"Peer - Timed out waiting for UNCHOKE from {self.ip}:{self.port}")
            return False
        except Exception as e:
            self.logger.error(f"Peer - Error while waiting for UNCHOKE from {self.ip}:{self.port}: {e}")
            return False

    async def connect(self):
        for attempt, backoff in enumerate([1, 2, 4, 8, 16], start=1):
            if await self._connect():
                return self
            self.logger.warning(
                f"Peer - Retrying connection to {self.ip}:{self.port} in {backoff}s (attempt {attempt})")
            await asyncio.sleep(backoff)
        raise ConnectionError(f"Peer - Failed to connect to {self.ip}:{self.port} after 5 attempts")

    def is_ready(self):
        return self.connected and self.handshaked and self.interested_sent

    async def request_block(
            self,
            piece_index: int,
            block_offset: int,
            block_length: int,
            timeout: int = 5,
            max_retries: int = 3,
            max_backoff: int = 16,
    ) -> bytes:
        if not self.reader or not self.writer:
            raise ConnectionError(f"Peer - Not connected to {self.ip}:{self.port}")

        request_msg = _create_request_message(block_length, block_offset, piece_index)
        peer_id = self.peer_id
        backoff = 1

        for attempt in range(max_retries):
            try:
                self.writer.write(request_msg)
                await self.writer.drain()
                self.logger.debug(
                    f"Peer - Sent request to {peer_id} for piece {piece_index}, offset {block_offset}, length {block_length}"
                )

                # Read length-prefixed message
                length_prefix_data = await asyncio.wait_for(self.reader.readexactly(4), timeout=timeout)
                msg_length = struct.unpack("!I", length_prefix_data)[0]
                msg_data = await asyncio.wait_for(self.reader.readexactly(msg_length), timeout=timeout)

                msg_id = msg_data[0]
                if msg_id != 7:  # PIECE
                    self.logger.warning(f"Peer - {peer_id}: Expected PIECE (ID 7), got ID {msg_id}")
                    raise ValueError("Unexpected message ID")

                index, offset = struct.unpack("!II", msg_data[1:9])
                block_data = msg_data[9:]

                if index != piece_index or offset != block_offset:
                    self.logger.warning(
                        f"Peer - {peer_id}: Mismatched index/offset. Got {index}@{offset}, expected {piece_index}@{block_offset}"
                    )
                    raise ConnectionError("Mismatched piece or offset")

                self.logger.debug(
                    f"Peer - Received block from {peer_id}: piece {index}, offset {offset}, length {len(block_data)}"
                )
                return block_data

            except asyncio.TimeoutError:
                self.logger.warning(f"Peer - Timeout while receiving block from {peer_id}")
            except asyncio.IncompleteReadError as e:
                self.logger.warning(f"Peer - Incomplete read from {peer_id}: {e}")
            except Exception as e:
                self.logger.error(f"Peer - Error receiving block from {peer_id}: {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        self.logger.error(f"Peer - All {max_retries} attempts failed to receive block from {peer_id}")
        raise ConnectionError(f"Failed to receive block {piece_index}@{block_offset} after {max_retries} attempts")

    async def download_piece(self, piece, max_retries=3, max_backoff=16):
        BLOCK_SIZE = 16384
        piece_index = piece.index
        hash_value = piece.hash_value
        piece_length = piece.length
        num_blocks = (piece_length + BLOCK_SIZE - 1) // BLOCK_SIZE

        backoff = 1

        for attempt in range(max_retries):
            try:
                self.logger.info(
                    f"Peer - Attempt {attempt + 1} to download piece {piece_index} from {self.ip}:{self.port}"
                )

                if not self.handshaked:
                    await self.perform_handshake()

                if not self.interested_sent:
                    await self.send_interested()

                if not self.is_ready():
                    raise Exception(f"Peer - {self.ip}:{self.port} not ready after handshake")

                if not self.unchoked:
                    if not await self.wait_for_unchoke():
                        raise Exception(f"Peer - {self.ip}:{self.port} never unchoked us")

                piece_data = bytearray(piece_length)

                for block_index in range(num_blocks):
                    offset = block_index * BLOCK_SIZE
                    block_size = min(BLOCK_SIZE, piece_length - offset)

                    # This method should send a 'request' message and await the corresponding 'piece' response
                    block = await self.request_block(piece_index, offset, block_size)
                    piece_data[offset:offset + block_size] = block

                if verify_piece_hash(piece_data, hash_value):
                    self.logger.info(
                        f"Peer - Successfully downloaded and verified piece {piece_index} from {self.ip}:{self.port}"
                    )
                    piece.update_status(piece.Status.COMPLETED)
                    return piece_data
                else:
                    raise Exception(f"Peer - Hash mismatch for piece {piece_index} from {self.ip}:{self.port}")

            except Exception as e:
                self.logger.warning(
                    f"Peer - Download attempt {attempt + 1} failed for piece {piece_index} from {self.ip}:{self.port}: {e}"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        piece.update_status(piece.Status.FAILED)
        raise Exception(
            f"Peer - Failed to download piece {piece_index} from {self.ip}:{self.port} after {max_retries} retries")
