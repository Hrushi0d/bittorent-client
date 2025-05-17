import asyncio
import hashlib
import struct
from collections import defaultdict

from utils._AsyncQueue import AsyncQueue
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
    def __init__(self, logger, async_queue: AsyncQueue, pieces: list[Piece], piece_dict: defaultdict[list[Peer]],
                 max_concurrent_pieces=5, max_concurrent_blocks=20):
        self.logger = logger
        self.pieces_order = pieces
        self.piece_dict = piece_dict
        self.piece_semaphore = asyncio.Semaphore(max_concurrent_pieces)
        self.block_semaphore = asyncio.Semaphore(max_concurrent_blocks)
        self.async_queue = async_queue

    async def download_from_peer(self, piece, peer):
        async with self.piece_semaphore:
            ip, port, peer_id = peer.ip, peer.port, peer.peer_id
            piece_index = piece.index
            hash_value = piece.hash_value
            piece_length = piece.length

            try:
                # Standard block size in BitTorrent is 16KB (16384 bytes)
                BLOCK_SIZE = 16384
                num_blocks = (piece_length + BLOCK_SIZE - 1) // BLOCK_SIZE
                piece_data = bytearray(piece_length)

                for block_index in range(num_blocks):
                    block_offset = block_index * BLOCK_SIZE
                    # For the last block, adjust the block size if needed
                    current_block_size = min(BLOCK_SIZE, piece_length - block_offset)

                    async with self.block_semaphore:
                        request_msg = _create_request_message(piece_index, block_offset, current_block_size)
                        await self.send_message_to_peer(peer, request_msg)

                        block_data = await self.receive_block_from_peer(peer, piece_index, block_offset)
                        if block_data:
                            start_pos = block_offset
                            end_pos = min(start_pos + len(block_data), piece_length)
                            piece_data[start_pos:end_pos] = block_data
                        else:
                            self.logger.warning(
                                f"Failed to download block {block_index} of piece {piece_index} from peer {peer_id}")
                            return None
                if verify_piece_hash(piece_data, hash_value):
                    self.logger.info(f"Successfully downloaded piece {piece_index} from {ip}:{port}")
                    await self.async_queue.push(piece, piece_data)
                else:
                    self.logger.warning(f"Piece {piece_index} hash verification failed from {ip}:{port}")
                    return None
            except Exception as e:
                self.logger.error(f"Error downloading piece {piece_index} from {ip}:{port}: {str(e)}")
                return None

    async def send_message_to_peer(self, peer: Peer, request_msg, timeout=5, max_retries=3, max_backoff=16):
        writer = peer.writer
        peer_id = peer.peer_id
        backoff = 1

        async def _write_and_drain():
            writer.write(request_msg)
            await writer.drain()

        for attempt in range(max_retries):
            try:
                await asyncio.wait_for(_write_and_drain(), timeout=timeout)
                self.logger.debug(f"Sent request to {peer_id}: {request_msg.hex()}")
                return  # success
            except Exception as e:
                self.logger.warning(f"[Attempt {attempt + 1}] Failed to send message to {peer_id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        self.logger.error(f"All {max_retries} attempts failed to send message to {peer_id}")

    async def receive_block_from_peer(self, peer, piece_index, block_offset, timeout=5, max_retries=3, max_backoff=16):
        reader = peer.reader
        peer_id = peer.peer_id
        backoff = 1
        for attempt in range(max_retries):
            try:
                self.logger.debug(
                    f"Attempting to read block from {peer_id}: "
                    f"piece {piece_index}, offset {block_offset}")
                length_prefix_data = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
                msg_length = struct.unpack("!I", length_prefix_data)[0]
                msg_data = await asyncio.wait_for(reader.readexactly(msg_length), timeout=timeout)
                msg_id = msg_data[0]
                if msg_id != 7:
                    self.logger.warning(f"{peer_id}: Expected piece message (ID 7), got ID {msg_id}")
                    raise ValueError("Unexpected message ID")
                index, offset = struct.unpack('!II',msg_data[1:9])
                block_data = msg_data[9:]

                if index != piece_index or offset != block_offset:
                    self.logger.warning(
                        f"{peer_id}: Mismatched piece or offset (got {index}@{offset}, expected {piece_index}@{block_offset})")
                    raise ConnectionError("Mismatched piece or offset")

                self.logger.debug(
                    f"Received block from {peer_id}: piece {index}, offset {offset}, length {len(block_data)}")
                return block_data
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout while receiving block from {peer_id}")
            except asyncio.IncompleteReadError as e:
                self.logger.warning(f"Incomplete read from {peer_id}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error receiving block from {peer_id}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        self.logger.error(f"All {max_retries} attempts failed to receive block from {peer_id}")
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
