# **********************************************************************************************************************
#                     _________  ________  ________  ________  _______   ________   _________
#                    |\___   ___\\   __  \|\   __  \|\   __  \|\  ___ \ |\   ___  \|\___   ___\
#                    \|___ \  \_\ \  \|\  \ \  \|\  \ \  \|\  \ \   __/|\ \  \\ \  \|___ \  \_|
#                         \ \  \ \ \  \\\  \ \   _  _\ \   _  _\ \  \_|/_\ \  \\ \  \   \ \  \
#                          \ \  \ \ \  \\\  \ \  \\  \\ \  \\  \\ \  \_|\ \ \  \\ \  \   \ \  \
#                           \ \__\ \ \_______\ \__\\ _\\ \__\\ _\\ \_______\ \__\\ \__\   \ \__\
#                            \|__|  \|_______|\|__|\|__|\|__|\|__|\|_______|\|__| \|__|    \|__|
#
#                                                 INFO ABOUT THIS FILE
#                               `PeerConnectionPool` class, handles maintaining a pool of active peers,
#                               gets and organises piece information into piece_dict with key as
#                               piece_index and value as list of peers that have that piece available,
#                               this is used later inside utils._PieceManager to sort peices into
#                               strategic order.
#
#                                - First it acquires peers from the torrent using a petter getter
#                                  instance defined in utils._PeerGetter.
#
#                                - Then, sets up interested and bitfeild requests for each peer
#                                  as an Asyncio Coroutine. using the member functions
#                                  _handle_peer_with_timeout() and _handle_peer(), then it attempts
#                                  retries with exponential backoff on all peer requests just in case.
#
#                                - Checks to see if there are any missing peices, stopping execution
#                                  if no pieces are available. it logs peer scores. then it returns
#                                  the piece_dict.

# *************************************************************** IMPORTS **********************************************

import asyncio
import hashlib
import logging
import random
import time
import traceback
from collections import defaultdict, deque

from utils._Bencode import Encoder
from utils._Peer import Peer
from utils._PeerGetter import PeerGetter

# ********************************************************* PEER CONNECTION POOL ***************************************

class PeerConnectionPool:
    def __init__(self, torrent, logger: logging.Logger):
        self.torrent = torrent
        self.logger = logger
        self.info_dict = torrent[b'info']
        self.piece_length = self.info_dict[b'piece length']
        self.pieces_data = self.info_dict[b'pieces']

        bencoded_info = Encoder(self.info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()

        self._peer_getter = PeerGetter(torrent=self.torrent, logger=self.logger)
        self.peers = []
        self.active_peers = set()  # Track currently active peers
        self.peer_stats = {}  # Track peer statistics for prioritization
        self.failed_peers = []  # Track failed peers for retry
        self.total_pieces = self._calculate_total_pieces()
        self.logger.info("PeerConnectionPool - Initialized with info_hash: %s", self.info_hash.hex())

        # Configure connection parameters
        self.connection_timeout = 5  # Timeout in seconds
        self.max_concurrent_connections = 50  # Limit concurrent connections
        self.peer_connection_semaphore = asyncio.Semaphore(self.max_concurrent_connections)

        # Retry configuration
        self.max_retries = 3  # Maximum number of connection retries per peer
        self.retry_delay_base = 2  # Base delay for exponential backoff (seconds)

        # Pipelining configuration
        self.pipeline_size = 5  # Number of requests to pipeline
        self.request_queue = deque()  # Queue for pending requests

    def _calculate_total_pieces(self):
        """Calculate total pieces once during initialization"""
        if b'info' not in self.torrent:
            self.logger.error("PeerConnectionPool - The 'info' key is missing from the torrent data.")
            return 0

        # Calculate the total size of the files (cached calculation)
        total_size = 0
        if b'files' in self.info_dict:
            # Multi-file torrent, sum the lengths of all files
            for file_info in self.info_dict[b'files']:
                total_size += file_info[b'length']
        elif b'length' in self.info_dict:
            # Single file torrent
            total_size = self.info_dict[b'length']
        else:
            self.logger.error("PeerConnectionPool - Neither 'length' nor 'files' found in the 'info' dictionary.")
            return 0

        # Calculate the total number of pieces based on the total size and piece length
        total_pieces = (total_size + self.piece_length - 1) // self.piece_length  # Rounded up division

        # Verify against number of hashes
        num_hashes = len(self.pieces_data) // 20  # Each hash is 20 bytes (SHA1)
        if num_hashes != total_pieces:
            self.logger.warning(
                f"PeerConnectionPool - Number of piece hashes ({num_hashes}) does not match the expected number of "
                f"pieces ({total_pieces}).")
            # Use the hash count as the source of truth if they differ
            total_pieces = num_hashes

        self.logger.info(
            f"PeerConnectionPool - Total size: {total_size} bytes, Piece length: {self.piece_length} bytes, Total "
            f"pieces: {total_pieces}")
        return total_pieces

    def _add_peer(self, ip, port):
        """Add a peer to the pool"""
        peer = Peer(ip=ip, port=port, info_hash=self.info_hash, logger=self.logger)
        self.peers.append(peer)
        # Initialize peer stats for new peer
        self.peer_stats[peer] = {
            'connection_attempts': 0,
            'successful_connections': 0,
            'last_seen': 0,
            'response_time': float('inf'),
            'available_pieces': set(),
            'score': 0  # Used for prioritization
        }

    async def _get_peers(self, max_tracker_retries=3):
        """Retrieve peers from tracker with retry logic"""
        self.logger.info("PeerConnectionPool - Attempting to retrieve peers from tracker.")
        retry_count = 0

        while retry_count < max_tracker_retries:
            try:
                peers = await self._peer_getter.get()

                if not peers:
                    retry_count += 1
                    wait_time = 2 ** retry_count
                    self.logger.warning(
                        f"PeerConnectionPool - No peers received. Retrying in {wait_time} seconds "
                        f"(attempt {retry_count}/{max_tracker_retries})")
                    await asyncio.sleep(wait_time)
                    continue

                # Batch add peers
                for ip, port in peers:
                    self._add_peer(ip, port)

                self.logger.info(f"PeerConnectionPool - Successfully retrieved and added {len(peers)} peers.")
                return

            except Exception as e:
                retry_count += 1
                wait_time = 2 ** retry_count
                self.logger.error(f"PeerConnectionPool - Error while retrieving peers: {e}")

                if retry_count < max_tracker_retries:
                    self.logger.info(
                        f"PeerConnectionPool - Retrying tracker in {wait_time} seconds "
                        f"(attempt {retry_count}/{max_tracker_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"PeerConnectionPool - Failed to retrieve peers after "
                                      f"{max_tracker_retries} attempts")
                    break

    async def _get_bitfield_with_timeout(self, peer, piece_dict):
        """Get the bitfield from the peer using its event-driven interface."""
        try:
            bitfield = await peer.wait_for_bitfield(timeout=10)
            if bitfield is None:
                self.logger.warning(f"PeerConnectionPool - Timeout waiting for bitfield from {peer.ip}:{peer.port}")
                return False
            # Parse bitfield to determine which pieces the peer has
            available_pieces = set()
            for byte_idx, byte in enumerate(bitfield):
                for bit_idx in range(8):
                    piece_idx = byte_idx * 8 + bit_idx
                    if piece_idx >= self.total_pieces:
                        break
                    if byte & (128 >> bit_idx):
                        piece_dict[piece_idx].append(peer)
                        available_pieces.add(piece_idx)
            # Update peer stats
            if peer in self.peer_stats:
                self.peer_stats[peer]['available_pieces'] = available_pieces
                self.peer_stats[peer]['last_seen'] = time.time()
                self.peer_stats[peer]['response_time'] = 0  # You can update this if you want timing
            self.logger.info(
                f"PeerConnectionPool - Peer {peer.ip}:{peer.port} has {len(available_pieces)}/{self.total_pieces} "
                f"pieces available"
            )
            return True
        except Exception as e:
            self.logger.error(f"PeerConnectionPool - Error getting bitfield from {peer.ip}:{peer.port}: {e}")
            return False

    async def _handle_peer_with_timeout(self, peer, peer_results, piece_dict, retry_count=0):
        """Handle peer connection with timeout wrapper"""
        try:
            return await asyncio.wait_for(
                self._handle_peer(peer, peer_results, piece_dict, retry_count),
                timeout=self.connection_timeout
            )
        except asyncio.TimeoutError:
            self.logger.error(f"PeerConnectionPool - Timeout handling peer {peer.ip}:{peer.port}")
            peer_results[peer] = False

            # Retry logic
            if retry_count < self.max_retries:
                # Exponential backoff
                retry_delay = self.retry_delay_base * (2 ** retry_count) * (0.5 + random.random())
                self.logger.info(
                    f"PeerConnectionPool - Scheduling retry for {peer.ip}:{peer.port} in {retry_delay:.2f}s "
                    f"(attempt {retry_count + 1}/{self.max_retries})")

                # Add to failed peers for later retry
                self.failed_peers.append((peer, retry_count + 1))

            return False

    async def _handle_peer(self, peer: Peer, peer_results, piece_dict, retry_count=0):
        """Handle connection and communication with a peer with retry logic"""
        self.peer_stats[peer]['connection_attempts'] += 1

        async with self.peer_connection_semaphore:  # Limit concurrent connections
            try:
                # Mark peer as active
                self.active_peers.add(peer)
                # After connect & handshake (inside context), send interested
                await peer.connect()
                await peer.send_interested()

                # Retrieve bitfield info
                success = await self._get_bitfield_with_timeout(peer, piece_dict)

                if success:
                    # Update peer stats
                    self.peer_stats[peer]['successful_connections'] += 1
                    self.peer_stats[peer]['last_seen'] = time.time()

                    # Set up pipelining for this peer if available pieces
                    if peer in self.peer_stats and self.peer_stats[peer]['available_pieces']:
                        await self._setup_pipeline(peer)

                    # Update score for prioritization
                    self._update_peer_score(peer)

                peer_results[peer] = success
                return success

            except Exception as e:
                self.logger.error(
                    f"PeerConnectionPool - Error handling peer {peer.ip}:{peer.port}: {type(e).__name__}: {e}")
                self.logger.debug("PeerConnectionPool - Traceback:\n" + traceback.format_exc())
                self.logger.error(f"PeerConnectionPool - Disconnecting {peer.ip}:{peer.port}")
                await peer.disconnect()
                peer_results[peer] = False

                # Retry logic
                if retry_count < self.max_retries:
                    # Exponential backoff
                    retry_delay = self.retry_delay_base * (2 ** retry_count) * (0.5 + random.random())
                    self.logger.info(
                        f"Scheduling retry for {peer.ip}:{peer.port} in {retry_delay:.2f}s "
                        f"(attempt {retry_count + 1}/{self.max_retries})")

                    # Add to failed peers for later retry
                    self.failed_peers.append((peer, retry_count + 1))

                return False
            finally:
                # Remove from active peers
                if peer in self.active_peers:
                    self.active_peers.remove(peer)

    def _update_peer_score(self, peer):
        """Update peer score based on performance metrics"""
        if peer not in self.peer_stats:
            return

        stats = self.peer_stats[peer]

        # Calculate success rate
        success_rate = stats['successful_connections'] / max(1, stats['connection_attempts'])

        # Calculate recency (higher is better)
        recency = max(0, 1.0 - (time.time() - stats['last_seen']) / 3600)  # Normalize to last hour

        # Calculate piece availability (percentage of total pieces)
        availability = len(stats['available_pieces']) / max(1, self.total_pieces)

        # Calculate response speed (inverse of response time, capped)
        response_speed = 1.0 / max(0.5, min(stats['response_time'], 10))

        # Weighted score calculation
        score = (
                0.3 * success_rate +
                0.2 * recency +
                0.4 * availability +
                0.1 * response_speed
        )

        # Update the score
        stats['score'] = score

    async def _setup_pipeline(self, peer):
        """Set up pipelining for a peer to request multiple pieces efficiently"""
        if peer not in self.peer_stats:
            return

        stats = self.peer_stats[peer]
        available_pieces = stats['available_pieces']

        # Skip if no pieces available
        if not available_pieces:
            return

        self.logger.debug(
            f"PeerConnectionPool - Setting up pipeline for peer {peer.ip}:{peer.port} with {len(available_pieces)}"
            f" available pieces")

        # Queue up pipeline_size requests
        pieces_to_request = list(available_pieces)[:self.pipeline_size]

        # In a real implementation, you would:
        # 1. Send multiple piece requests without waiting for responses
        # 2. Process responses as they arrive
        # For this example, we'll simulate the concept
        for piece_idx in pieces_to_request:
            self.request_queue.append((peer, piece_idx))

        self.logger.debug(
            f"PeerConnectionPool - Queued {len(pieces_to_request)} piece requests for {peer.ip}:{peer.port}")

    async def _retry_failed_peers(self, peer_results, piece_dict):
        """Retry connecting to failed peers with exponential backoff"""
        if not self.failed_peers:
            return

        self.logger.info(f"PeerConnectionPool - Retrying {len(self.failed_peers)} failed peers")
        retry_tasks = []

        # Process each failed peer
        while self.failed_peers:
            peer, retry_count = self.failed_peers.pop(0)
            # Create a retry task with the current retry count
            task = asyncio.create_task(self._handle_peer_with_timeout(peer, peer_results, piece_dict, retry_count))
            retry_tasks.append(task)

        # Wait for all retry tasks to complete
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)

    def _prioritize_peers(self):
        """Sort peers by their score for prioritized processing"""
        # Calculate scores for any peers that don't have one
        for peer in self.peers:
            if peer in self.peer_stats and self.peer_stats[peer]['score'] == 0:
                self._update_peer_score(peer)

        # Sort peers by score (highest first)
        self.peers.sort(key=lambda p: self.peer_stats.get(p, {'score': 0})['score'], reverse=True)

        # Log top peers
        top_n = min(5, len(self.peers))
        if top_n > 0:
            self.logger.info(f"Top {top_n} peers by score:")
            for i in range(top_n):
                peer = self.peers[i]
                if peer in self.peer_stats:
                    score = self.peer_stats[peer]['score']
                    self.logger.info(f"PeerConnectionPool -   {i + 1}. {peer.ip}:{peer.port} - Score: {score:.4f}")

    async def run(self):
        """Run the peer connection process to gather piece information with retry logic and pipelining"""
        self.logger.info("PeerConnectionPool - Starting peer connection process.")
        await self._get_peers()

        if not self.peers:
            self.logger.error("PeerConnectionPool - No peers available to connect.")
            return None

        peer_results = {}
        piece_dict = defaultdict(list)

        # Prioritize peers based on historical performance
        self._prioritize_peers()

        # Process peers in batches to avoid overwhelming resources
        batch_size = min(len(self.peers), self.max_concurrent_connections)
        self.logger.info(f"PeerConnectionPool - Processing {len(self.peers)} peers in batches of {batch_size}")

        # Create initial tasks for all peers
        tasks = [self._handle_peer_with_timeout(peer, peer_results, piece_dict) for peer in self.peers]

        # Run tasks with gather for better performance
        await asyncio.gather(*tasks, return_exceptions=True)

        # Retry failed peers
        if self.failed_peers:
            await self._retry_failed_peers(peer_results, piece_dict)

        # Check for missing pieces
        missing_pieces = []
        for piece in range(self.total_pieces):
            if piece not in piece_dict or not piece_dict[piece]:
                missing_pieces.append(piece)

        if missing_pieces:
            self.logger.warning(f"PeerConnectionPool - Missing {len(missing_pieces)} pieces out of {self.total_pieces}")
            if len(missing_pieces) < 20:  # Only log if the list is reasonably small
                self.logger.warning(f"PeerConnectionPool - Missing pieces: {missing_pieces}")
            return False
        else:
            self.logger.info(f"PeerConnectionPool - All {self.total_pieces} pieces are available from peers")

        # Return the piece dictionary
        return piece_dict
# *************************************************************** EOF **************************************************
