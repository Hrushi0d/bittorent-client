import asyncio
import hashlib
from collections import defaultdict

from utils.Bencode import Encoder
from utils._Peer import Peer
from utils._PeerGetter import PeerGetter


class PeerConnectionPool:
    def __init__(self, torrent, logger):
        self.torrent = torrent
        self.logger = logger
        self.info_dict = torrent[b'info']
        self.piece_length = self.info_dict[b'piece length']
        self.pieces_data = self.info_dict[b'pieces']
        bencoded_info = Encoder(self.info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()
        self._peer_getter = PeerGetter(torrent=self.torrent, logger=self.logger)
        self.peers = []
        self.logger.info("PeerConnectionPool initialized with info_hash: %s", self.info_hash.hex())

    def _total_pieces(self):

        if b'info' not in self.torrent:
            self.logger.error("The 'info' key is missing from the torrent data.")
            return 0  # Return 0 or handle this scenario as needed

        # Access the pieces data and piece length from self.torrent instead of self.info_dict
        self.info_dict = self.torrent[b'info']
        self.piece_length = self.info_dict[b'piece length']
        self.pieces_data = self.info_dict[b'pieces']

        # Calculate the number of hashes (each piece is represented by a 20-byte hash)
        num_hashes = len(self.pieces_data) // 20  # Each hash is 20 bytes (SHA1)

        # Calculate the total size of the files for multi-file torrents
        total_size = 0
        if b'files' in self.info_dict:
            # Multi-file torrent, sum the lengths of all files
            for file_info in self.info_dict[b'files']:
                total_size += file_info[b'length']
        elif b'length' in self.info_dict:
            # Single file torrent
            total_size = self.info_dict[b'length']
        else:
            self.logger.error("Neither 'length' nor 'files' found in the 'info' dictionary.")
            return 0  # Return 0 or handle this scenario as needed

        # Calculate the total number of pieces based on the total size and piece length
        total_pieces = (total_size + self.piece_length - 1) // self.piece_length  # Rounded up division

        # Optionally log if there's a mismatch between the expected number of pieces and the hashes
        if num_hashes != total_pieces:
            self.logger.warning(
                f"Number of piece hashes ({num_hashes}) does not match the expected number of pieces ({total_pieces}).")
        else:
            self.logger.info(
                f"All piece hashes found correctly: {num_hashes} pieces as expected.")

        return total_pieces

    def _add_peer(self, ip, port):
        peer = Peer(ip=ip, port=port, info_hash=self.info_hash, logger=self.logger)
        self.peers.append(peer)
        self.logger.info(f"Added peer {ip}:{port} to connection pool.")

    async def _get_peers(self):
        self.logger.info("Attempting to retrieve peers.")
        try:
            peers = await self._peer_getter.get()
            for ip, port in peers:
                self._add_peer(ip, port)
            self.logger.info("Successfully retrieved and added peers.")
        except Exception as e:
            self.logger.error(f"Error while retrieving peers: {e}")

    async def get_piece_info(self, peer, piece_dict):
        reader = peer.reader
        try:
            # Loop until we get a bitfield message
            while True:
                length_bytes = await reader.readexactly(4)
                length = int.from_bytes(length_bytes, 'big')
                if length == 0:
                    continue  # keep-alive
                msg_id = await reader.readexactly(1)
                payload_len = length - 1
                # Bitfield message ID = 5
                if msg_id == b"\x05":
                    payload = await reader.readexactly(payload_len)
                    bitfield = []
                    for byte in payload:
                        for i in range(8):
                            if len(bitfield) < self.piece_length:
                                bitfield.append((byte >> (7 - i)) & 1)
                    # Update piece_dict
                    for idx, has in enumerate(bitfield):
                        if has:
                            piece_dict[idx].append(peer)
                    break
                else:
                    # Skip unknown or unwanted payloads
                    if payload_len:
                        await reader.readexactly(payload_len)
        except Exception as e:
            self.logger.error(f"Error getting piece info from {peer.ip}:{peer.port}: {e}")

    async def _handle_peer(self, peer, peer_results, piece_dict):
        try:
            async with peer:
                # After connect & handshake (inside context), send interested
                await peer.send_interested()
                # Retrieve bitfield info
                await self.get_piece_info(peer, piece_dict)
                peer_results[peer] = True
                self.logger.info(f"Handled peer {peer.ip}:{peer.port} successfully.")
                return True
        except Exception as e:
            self.logger.error(f"Error handling peer {peer.ip}:{peer.port}: {e}")
            peer_results[peer] = False
            return False

    async def run(self):
        self.logger.info("Starting peer connection process.")
        await self._get_peers()
        peer_results = {}
        piece_dict = defaultdict(list)

        # Connect, handshake, and get piece info in one pass
        tasks = [self._handle_peer(peer, peer_results, piece_dict) for peer in self.peers]
        await asyncio.gather(*tasks)
        expected_pieces = range(0, self._total_pieces())
        missing_pieces = [piece for piece in expected_pieces if piece not in piece_dict]

        if missing_pieces:
            self.logger.warning(f"Missing pieces: {missing_pieces}")
            # Handle missing pieces, either retry or report an error
            return None  # Or you could retry fetching missing pieces

        # Log results
        for piece, peers in piece_dict.items():
            self.logger.info(f"Piece {piece} available from {len(peers)} peers: {peers}")

        return piece_dict
