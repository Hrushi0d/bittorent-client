import asyncio
import hashlib
import logging
import random
import struct
from collections import defaultdict

from utils._AsyncQueue import AsyncQueue
from utils._DownloadQueue import QueueClosed, DownloadChecker
from utils._Peer import Peer
from utils._Piece import Piece


def _create_request_message(block_length, block_offset, piece_index):
    message_id = 6
    payload = struct.pack("!III", piece_index, block_offset, block_length)
    message_length = 1 + len(payload)
    return struct.pack("!IB", message_length, message_id) + payload


def verify_piece_hash(piece_data, expected_hash):
    piece_hash = hashlib.sha1(piece_data).digest()
    return piece_hash == expected_hash


class DownloadManager:
    def __init__(self, logger: logging.Logger, async_queue: AsyncQueue, pieces: list[Piece],
                 piece_dict: defaultdict[int, list[Peer]],
                 max_concurrent_pieces=5, max_concurrent_blocks=20):
        self.logger = logger
        logger.info('DownloadManager - Initialized')
        self.pieces_order = pieces
        self.piece_dict = piece_dict
        self.peer_dict = defaultdict(list)
        for piece in self.pieces_order:
            peers = self.piece_dict[piece.index]  # use piece.index instead of piece directly
            for peer in peers:
                self.peer_dict[peer].append(piece)

        self.piece_semaphore = asyncio.Semaphore(max_concurrent_pieces)
        self.block_semaphore = asyncio.Semaphore(max_concurrent_blocks)
        self.async_queue = async_queue
        self.active_peers = set()
        self.checker = DownloadChecker(piece_dict=self.piece_dict, peer_dict=self.peer_dict)

    async def setup_requests(self):
        for piece in self.pieces_order:
            peers = self.piece_dict[piece.index]
            for peer in peers:
                await peer.download_queue.push(piece)
                self.active_peers.add(peer)

        for peer in self.active_peers:
            peer.download_queue.add_checker(checker=self.checker)

    @classmethod
    async def create(cls, logger: logging.Logger, async_queue: AsyncQueue, pieces: list[Piece],
                     piece_dict: defaultdict[int, list[Peer]],
                     max_concurrent_pieces=5, max_concurrent_blocks=20):
        """
        Asynchronously creates and initializes a DownloadManager.
        """
        manager = cls(logger, async_queue, pieces, piece_dict, max_concurrent_pieces, max_concurrent_blocks)
        await manager.setup_requests()
        return manager

    async def start(self, max_concurrent_downloads=50):
        semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.logger.info(f'DownloadManager - started downloading')

        async def limited_download(peer: Peer):
            while True:
                async with semaphore:
                    try:
                        piece = await peer.download_queue.pop()
                        piece.update_status(Piece.Status.IN_PROGRESS)
                    except QueueClosed:
                        self.logger.info(
                            f'DownloadManager - peer {peer.ip}:{peer.port} queue closed, finishing downloads')
                        break
                    except Exception as e:
                        self.logger.error(f"DownloadManager - error popping piece for peer {peer.ip}:{peer.port}: {e}")
                        break

                    try:
                        data = await peer.download_piece(piece)
                        await self.async_queue.push(piece=piece, piece_data=data)
                        piece.update_status(Piece.Status.COMPLETED)
                    except Exception as e:
                        self.logger.error(
                            f"DownloadManager - failed downloading piece {piece.index} from {peer.ip}:{peer.port}: {e}")
                        piece.update_status(Piece.Status.FAILED)

        tasks = [asyncio.create_task(limited_download(peer)) for peer in self.active_peers]
        await asyncio.gather(*tasks)
        self.logger.info('DownloadManager - all downloads complete.')

    async def download_from_peer(self, piece, max_retries=3, max_backoff=16):
        """
        Attempts to download the specified piece from available peers with retries and exponential backoff.
        Tries all peers on each attempt, shuffling order for fairness.
        Returns True on success, False after all retries have failed.
        """
        BLOCK_SIZE = 16384
        piece_index = piece.index
        hash_value = piece.hash_value
        piece_length = piece.length
        num_blocks = (piece_length + BLOCK_SIZE - 1) // BLOCK_SIZE
        backoff = 1

        for attempt in range(max_retries):
            self.logger.info(f"DownloadManager - Download attempt {attempt + 1} for piece {piece_index}")
            peers = list(self.piece_dict.get(piece_index, []))
            if not peers:
                self.logger.warning(f"DownloadManager - No peers available for piece {piece_index}")
                break

            random.shuffle(peers)

            for peer in peers:
                ip, port, peer_id = peer.ip, peer.port, peer.peer_id
                self.logger.debug(f"DownloadManager - Trying piece {piece_index} from peer {ip}:{port}")

                try:
                    async with peer._read_lock:
                        async with peer._write_lock:  # Manages connection and disconnection
                            if not peer.handshaked:
                                await peer.perform_handshake()

                            if not peer.interested_sent:
                                await peer.send_interested()

                            if not peer.is_ready():
                                continue

                            if not peer.unchoked:
                                if not await peer.wait_for_unchoke():
                                    self.logger.warning(f"DownloadManager - Peer {ip}:{port} never unchoked us.")
                                    continue

                            piece_data = bytearray(piece_length)

                            for block_index in range(num_blocks):
                                block_offset = block_index * BLOCK_SIZE
                                block_size = min(BLOCK_SIZE, piece_length - block_offset)

                                async with self.block_semaphore:
                                    request_msg = _create_request_message(piece_index, block_offset, block_size)
                                    await self.send_message_to_peer(peer, request_msg)

                                    block_data = await self.receive_block_from_peer(peer, piece_index, block_offset)
                                    if not block_data:
                                        raise Exception(f"DownloadManager - Incomplete block at offset {block_offset}")

                                    piece_data[block_offset:block_offset + len(block_data)] = block_data

                            if verify_piece_hash(piece_data, hash_value):
                                self.logger.info(
                                    f"DownloadManager - Successfully downloaded piece {piece_index} from {ip}:{port}")
                                await self.async_queue.push(piece, piece_data)
                                return True
                            else:
                                self.logger.warning(
                                    f"DownloadManager - Hash mismatch for piece {piece_index} from {ip}:{port}")
                                continue

                except Exception as e:
                    self.logger.warning(
                        f"DownloadManager - Failed to download piece {piece_index} from {ip}:{port}: {e}")
                    continue

                await asyncio.sleep(0.1)

            # Exponential backoff before next retry
            if attempt < max_retries - 1:
                self.logger.info(f"DownloadManager - Retrying piece {piece_index} after {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        self.logger.error(
            f"DownloadManager - Failed to download piece {piece_index} from all peers after {max_retries} attempts.")
        return False

    async def send_message_to_peer(self, peer: Peer, request_msg, timeout=5, max_retries=3, max_backoff=16):
        writer = peer.writer
        peer_id = peer.peer_id
        backoff = 1

        async def _write_and_drain():
            writer.write(request_msg)
            await writer.drain()
            self.active_peers.add(self)

        for attempt in range(max_retries):
            try:
                await asyncio.wait_for(_write_and_drain(), timeout=timeout)
                self.logger.debug(f"DownloadManager - Sent request to {peer_id}: {request_msg.hex()}")
                return  # success
            except Exception as e:
                self.logger.warning(
                    f"DownloadManager - [Attempt {attempt + 1}] Failed to send message to {peer_id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        self.logger.error(f"DownloadManager - All {max_retries} attempts failed to send message to {peer_id}")

    async def receive_block_from_peer(self, peer, piece_index, block_offset, timeout=5, max_retries=3, max_backoff=16):
        reader = peer.reader
        peer_id = peer.peer_id
        backoff = 1
        for attempt in range(max_retries):
            try:
                self.logger.debug(
                    f"DownloadManager - Attempting to read block from {peer_id}: "
                    f"piece {piece_index}, offset {block_offset}")
                length_prefix_data = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
                msg_length = struct.unpack("!I", length_prefix_data)[0]
                msg_data = await asyncio.wait_for(reader.readexactly(msg_length), timeout=timeout)
                msg_id = msg_data[0]
                if msg_id != 7:
                    self.logger.warning(f"DownloadManager - {peer_id}: Expected piece message (ID 7), got ID {msg_id}")
                    raise ValueError("Unexpected message ID")
                index, offset = struct.unpack('!II', msg_data[1:9])
                block_data = msg_data[9:]

                if index != piece_index or offset != block_offset:
                    self.logger.warning(
                        f"DownloadManager - {peer_id}: Mismatched piece or offset (got {index}@{offset}, expected {piece_index}@{block_offset})")
                    raise ConnectionError("Mismatched piece or offset")

                self.logger.debug(
                    f"DownloadManager - Received block from {peer_id}: piece {index}, offset {offset}, length {len(block_data)}")
                return block_data
            except asyncio.TimeoutError:
                self.logger.warning(f"DownloadManager - Timeout while receiving block from {peer_id}")
            except asyncio.IncompleteReadError as e:
                self.logger.warning(f"DownloadManager - Incomplete read from {peer_id}: {e}")
            except Exception as e:
                self.logger.error(f"DownloadManager - Unexpected error receiving block from {peer_id}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        self.logger.error(f"DownloadManager - All {max_retries} attempts failed to receive block from {peer_id}")
        return None

# if __name__ == "__main__":
#     pieces = ['piece1', 'piece2', 'piece3']
#     piece_dict = defaultdict(list, {
#         'piece1': ['peer1', 'peer2', 'peer3'],
#         'piece2': ['peer2', 'peer3'],
#         'piece3': ['peer1', 'peer3']
#     })
#
#     manager = DownloadManager(pieces, piece_dict, max_concurrent_downloads=2)
#     asyncio.run(manager.run())
