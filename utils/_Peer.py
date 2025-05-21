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
        # asyncio.create_task(self.download_queue.retry_handler())

        # For pipelined block request/response matching
        self.pending_requests = {}  # (piece_index, block_offset): Future
        self.reader_task = None
        self.reader_task_running = False
        self.response_processor_task = None
        self._reader_lock = asyncio.Lock()  # For handshake and initial messages
        self.bitfield = None
        self.bitfield_received_event = asyncio.Event()
        self.response_queue = asyncio.Queue()  # <--- NEW QUEUE

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
            await self.start_reader_task()  # Start the dispatcher
            return True
        except Exception as e:
            self.logger.error(f"Peer - Connection to {self.ip}:{self.port} failed: {e}")
            self.connected = False
            return False

    async def perform_handshake(self):
        async with self._reader_lock:
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
        self.reader_task_running = False
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
        if self.response_processor_task:
            self.response_processor_task.cancel()
            try:
                await self.response_processor_task
            except asyncio.CancelledError:
                pass
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info(f"Peer - Disconnected from {self.ip}:{self.port}")
            self.connected = False

    async def wait_for_unchoke(self, timeout: int = 10) -> bool:
        if self.unchoked:
            self.logger.debug(f"Peer - Already unchoked by {self.ip}:{self.port}, skipping.")
            return True

        if not self.writer or not self.reader:
            self.logger.warning(f"Peer - Cannot wait for UNCHOKE from {self.ip}:{self.port}, no connection.")
            return False

        self.logger.info(f"Peer - Waiting for UNCHOKE from {self.ip}:{self.port}...")

        try:
            async with self._reader_lock:
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

    async def start_reader_task(self):
        if self.reader_task and not self.reader_task.done():
            return  # Already running
        self.reader_task_running = True
        self.reader_task = asyncio.create_task(self._reader_dispatcher())
        if not self.response_processor_task or self.response_processor_task.done():
            self.response_processor_task = asyncio.create_task(self._process_responses())

    async def _reader_dispatcher(self):
        try:
            while self.reader_task_running:
                try:
                    header = await self.reader.readexactly(4)
                except asyncio.IncompleteReadError:
                    self.logger.warning(f"Peer - Connection closed by peer {self.ip}:{self.port}")
                    break
                length = int.from_bytes(header, 'big')
                if length == 0:
                    self.logger.debug(f"Peer - Keep-alive from {self.ip}:{self.port}")
                    continue
                msg_id_bytes = await self.reader.readexactly(1)
                msg_id = msg_id_bytes[0]
                payload = await self.reader.readexactly(length - 1) if length > 1 else b''
                # Instead of processing message here, put it on the response queue
                await self.response_queue.put((msg_id, payload))
        except Exception as e:
            self.logger.error(f"Peer - Reader dispatcher error: {e}")

    async def _process_responses(self):
        try:
            while True:
                msg_id, payload = await self.response_queue.get()
                if msg_id == 7:  # PIECE
                    if len(payload) < 8:
                        self.logger.warning(f"Peer - PIECE payload too short")
                        continue
                    index, offset = struct.unpack("!II", payload[:8])
                    block_data = payload[8:]
                    key = (index, offset)
                    fut = self.pending_requests.pop(key, None)
                    if fut:
                        self.logger.debug(
                            f"Peer - Dispatcher delivering block for piece {index}, offset {offset}, length {len(block_data)}"
                        )
                        fut.set_result(block_data)
                    else:
                        self.logger.warning(
                            f"Peer - Received block for unknown request {index}@{offset}"
                        )
                elif msg_id == 1:  # UNCHOKE
                    self.unchoked = True
                    self.logger.info(f"Peer - Dispatcher received UNCHOKE from {self.ip}:{self.port}")
                elif msg_id == 5:  # BITFIELD
                    self.bitfield = payload
                    self.bitfield_received_event.set()
                else:
                    self.logger.debug(
                        f"Peer - Dispatcher received message id {msg_id} from {self.ip}:{self.port}"
                    )
        except Exception as e:
            self.logger.error(f"Peer - Response processor error: {e}")

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
        key = (piece_index, block_offset)

        for attempt in range(max_retries):
            try:
                fut = asyncio.get_running_loop().create_future()
                self.pending_requests[key] = fut
                self.writer.write(request_msg)
                await self.writer.drain()
                self.logger.debug(
                    f"Peer - Sent request to {peer_id} for piece {piece_index}, offset {block_offset}, length {block_length}"
                )

                block_data = await asyncio.wait_for(fut, timeout=timeout)
                self.logger.debug(
                    f"Peer - Received block from {peer_id}: piece {piece_index}, offset {block_offset}, length {len(block_data)}"
                )
                return block_data

            except asyncio.TimeoutError:
                self.logger.warning(f"Peer - Timeout while receiving block from {peer_id}")
            except Exception as e:
                self.logger.error(f"Peer - Error receiving block from {peer_id}: {e}")

            # Clean up on failure
            if key in self.pending_requests:
                self.pending_requests.pop(key, None)
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        self.logger.error(f"Peer - All {max_retries} attempts failed to receive block from {peer_id}")
        raise ConnectionError(f"Failed to receive block {piece_index}@{block_offset} after {max_retries} attempts")

    async def download_piece(self, piece, max_retries=3, max_backoff=16, max_pipeline=10):
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
                received_blocks = set()
                pending_blocks = set()
                next_block_index = 0

                # Map of block_index -> asyncio.Task for in-flight block requests
                block_futures = {}

                while len(received_blocks) < num_blocks:
                    # Pipeline new block requests up to max_pipeline
                    while len(pending_blocks) < max_pipeline and next_block_index < num_blocks:
                        offset = next_block_index * BLOCK_SIZE
                        block_size = min(BLOCK_SIZE, piece_length - offset)

                        async def fetch_block(block_idx, o, bsize):
                            try:
                                block = await self.request_block(piece_index, o, bsize)
                                return (block_idx, block)
                            except Exception as e:
                                self.logger.warning(
                                    f"Peer - Failed to fetch block {block_idx} of piece {piece_index}: {e}"
                                )
                                raise

                        fut = asyncio.create_task(fetch_block(next_block_index, offset, block_size))
                        block_futures[next_block_index] = fut
                        pending_blocks.add(next_block_index)
                        next_block_index += 1

                    if not block_futures:
                        raise Exception("Peer - No block futures in progress, but piece not complete")

                    done, _ = await asyncio.wait(
                        block_futures.values(), return_when=asyncio.FIRST_COMPLETED
                    )

                    for finished in done:
                        try:
                            block_index, block = finished.result()
                            offset = block_index * BLOCK_SIZE
                            block_size = min(BLOCK_SIZE, piece_length - offset)
                            piece_data[offset:offset + block_size] = block
                            received_blocks.add(block_index)
                            pending_blocks.remove(block_index)
                            del block_futures[block_index]
                        except Exception as e:
                            # Remove the failed block from pending and futures, will be retried in next outer loop
                            failed_index = None
                            for idx, fut in block_futures.items():
                                if fut == finished:
                                    failed_index = idx
                                    break
                            if failed_index is not None:
                                pending_blocks.remove(failed_index)
                                del block_futures[failed_index]
                            # Let the main loop retry this block

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

    async def wait_for_bitfield(self, timeout=5):
        """
        Wait for the bitfield message to be received from the peer.
        """
        try:
            await asyncio.wait_for(self.bitfield_received_event.wait(), timeout=timeout)
            return self.bitfield
        except asyncio.TimeoutError:
            self.logger.warning(f"Peer - Timeout waiting for bitfield from {self.ip}:{self.port}")
            return None
        except Exception as e:
            self.logger.error(f"Peer - Error waiting for bitfield from {self.ip}:{self.port}: {e}")
            return None