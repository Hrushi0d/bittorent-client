import asyncio
import logging
import random

from utils._Piece import Piece


class DownloadChecker:
    def __init__(self, peer_dict: dict, piece_dict: dict):
        self.peer_dict = peer_dict  # peer -> set of piece indices
        self.piece_dict = piece_dict # piece index -> list of peers
        self.completed_pieces = set()

        # Track peers that have failed to download a piece:
        # piece_index -> set of peers that failed
        self.failed_peers_for_piece = {}

    def mark_piece_completed(self, piece_index):
        self.completed_pieces.add(piece_index)
        # Once completed, clear failure record since piece is done
        if piece_index in self.failed_peers_for_piece:
            del self.failed_peers_for_piece[piece_index]

    def mark_peer_failed_for_piece(self, peer, piece_index):
        if piece_index not in self.failed_peers_for_piece:
            self.failed_peers_for_piece[piece_index] = set()
        self.failed_peers_for_piece[piece_index].add(peer)

    def is_piece_downloadable(self, piece_index):
        """Returns True if piece is not completed and at least one peer remains who hasn't failed."""
        if piece_index in self.completed_pieces:
            return False  # Already done

        peers = set(self.piece_dict.get(piece_index, []))
        failed_peers = self.failed_peers_for_piece.get(piece_index, set())

        # If all peers have failed, piece is not downloadable
        return not peers.issubset(failed_peers)

    def peer_has_pending_pieces(self, peer):
        """Check if peer has any piece that is downloadable."""
        for piece in self.peer_dict.get(peer, set()):
            if self.is_piece_downloadable(piece):
                return True
        return False


class QueueClosed(Exception):
    """Raised when the queue is closed and no more items can be popped."""
    pass


class DownloadQueue:
    def __init__(self, logger: logging.Logger, ip, port):
        self.ip, self.port = ip, port
        self.requests = asyncio.Queue()
        self.logger = logger
        self._closed = False  # Indicates no more pushes will happen
        self.checker: DownloadChecker | None = None

    def empty(self):
        return self.requests.empty()

    def add_checker(self, checker: DownloadChecker):
        self.checker = checker

    async def push(self, piece: Piece):
        if self._closed:
            self.logger.warning(f"DownloadQueue - Attempt to push to closed queue for peer {self.ip}:{self.port}")
            return
        self.logger.info(f'DownloadQueue - pushing piece {piece.index} into requests of peer {self.ip}:{self.port}')
        try:
            await self.requests.put(piece)
        except Exception as e:
            self.logger.warning(f'Unable to push piece {piece.index} into requests of peer {self.ip}:{self.port}: {e}')

    def close(self):
        self._closed = True

    async def pop(self):
        try:
            while True:
                if self._closed and self.requests.empty():
                    self.logger.info(f'DownloadQueue - queue closed and empty for peer {self.ip}:{self.port}')
                    raise QueueClosed()

                self.logger.info(f'DownloadQueue - pulling request from the requests of peer {self.ip}:{self.port}')
                piece = await self.requests.get()

                if piece.status() == Piece.Status.COMPLETED:
                    self.requests.task_done()
                    continue  # Skip and try next

                if piece.status() == Piece.Status.IN_PROGRESS:
                    self.logger.info(f'DownloadQueue - piece {piece.index} is already in progress, retrying later')
                    await self.push(piece)
                    self.requests.task_done()
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    continue  # Retry from queue

                self.requests.task_done()
                return piece

        except asyncio.CancelledError:
            self.logger.info(f'DownloadQueue - pop cancelled for peer {self.ip}:{self.port}')
            raise
