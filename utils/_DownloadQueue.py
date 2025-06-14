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
#                               provides key infrastructure for managing and tracking the
#                               download of pieces from peers in a BitTorrent client
#                               implementation. It defines the `DownloadChecker`,
#                               `QueueClosed`, and `DownloadQueue` classes, which together
#                               enable robust, concurrent, and fault-tolerant downloading of
#                               torrent pieces. This module is essential for coordinating
#                               piece downloads among multiple peers, ensuring that each
#                               peer works on appropriate pieces, handling completion and
#                               failure scenarios gracefully, and enabling concurrent
#                               downloads within a BitTorrent client.
#                               [See utils._Peer for usage]
#
#                               Downloader Checker:
#                               - Tracks piece completion and peer failures for each piece.
#                               - Determines if a piece is still downloadable based on peer
#                                 availability and previous failures.
#                               - Maintains overall download progress metrics and provides
#                                 progress reporting.
#                               - Supports efficient per-peer and per-piece status queries
#                                 for download scheduling.
#
#                               DownloadQueue:
#                               - Asynchronous queue for managing piece download requests
#                                 on a per-peer basis. Prevents pushing new tasks into a
#                                 closed queue and safely handles queue closure.
#                               - Integrates with `DownloadChecker` to avoid scheduling
#                                 already completed or un-downloadable pieces.
#                               - Handles piece status transitions and ensures that only
#                                 eligible pieces are attempted for download.
#                               - Supports safe cancellation and shutdown in concurrent
#                                 download scenarios
#
#                               QueueClosed Exception:
#                               - Custom exception to signal that a download queue has been
#                                 closed and no further items are available, allowing for
#                                 clean task termination.
#
# ***************************************************** IMPORTS *******************************************************

import asyncio
import logging
import random
import time
from typing import Tuple

from utils._Piece import Piece

# ********************************************** DOWNLOAD CHECKER *****************************************************

class DownloadChecker:
    __slots__ = (
        "peer_dict", "piece_dict", "completed_pieces", "finished",
        "failed_peers_for_piece", "start_time", "total_pieces"
    )

    def __init__(
            self,
            peer_dict,
            piece_dict,
            total_pieces=None
    ):
        self.peer_dict = peer_dict  # peer -> set of piece indices
        self.piece_dict = piece_dict  # piece index -> list of peers
        self.completed_pieces = set()
        self.finished = False
        self.failed_peers_for_piece = {}
        self.start_time = time.time()
        self.total_pieces = total_pieces if total_pieces is not None else len(piece_dict)

    def mark_piece_completed(self, piece_index: int) -> None:
        self.completed_pieces.add(piece_index)
        self.failed_peers_for_piece.pop(piece_index, None)  # Remove record if exists

    def mark_peer_failed_for_piece(self, peer: Tuple[str, int], piece_index: int) -> None:
        failed = self.failed_peers_for_piece.setdefault(piece_index, set())
        failed.add(peer)

    def is_piece_downloadable(self, piece_index: int) -> bool:
        if piece_index in self.completed_pieces:
            return False
        peers = self.piece_dict.get(piece_index)
        if not peers:
            return False
        failed = self.failed_peers_for_piece.get(piece_index)
        if not failed:
            return True  # No failures recorded
        # If all peers have failed, not downloadable
        for peer in peers:
            if peer not in failed:
                return True
        return False

    def peer_has_pending_pieces(self, ip: str, port: int) -> bool:
        pieces = self.peer_dict.get((ip, port))
        if not pieces:
            return False
        is_downloadable = self.is_piece_downloadable
        for piece in pieces:
            if is_downloadable(piece):
                return True
        return False

    def get_progress(self) -> Tuple[int, int, float]:
        done = len(self.completed_pieces)
        total = self.total_pieces
        return done, total, done / total if total else 0.0

    def get_time_elapsed(self) -> float:
        return time.time() - self.start_time

    def progress_report(self) -> dict:
        done, total, fraction = self.get_progress()
        return {
            "completed_pieces": done,
            "total_pieces": total,
            "fraction_done": fraction,
            "time_elapsed_seconds": self.get_time_elapsed()
        }

# ******************************************* QUEUE CLOSED EXCEPTION ***************************************************

class QueueClosed(Exception):
    """Raised when the queue is closed and no more items can be popped."""
    pass

# *********************************************** DOWNLOAD QUEUE ******************************************************

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
        # self.logger.info(f'DownloadQueue - pushing piece {piece.index} into requests of peer {self.ip}:{self.port}')
        try:
            await self.requests.put(piece)
        except Exception as e:
            self.logger.warning(f'Unable to push piece {piece.index} into requests of peer {self.ip}:{self.port}: {e}')

    def close(self):
        self._closed = True

    async def pop(self):
        try:
            while True:
                if not self.checker.peer_has_pending_pieces(self.ip, self.port):
                    self._closed = True

                if self._closed and self.requests.empty():
                    self.logger.info(f'DownloadQueue - queue closed and empty for peer {self.ip}:{self.port}')
                    raise QueueClosed()

                piece = await self.requests.get()

                if piece.status() == Piece.Status.COMPLETED:
                    self.requests.task_done()
                    continue  # Skip and try next

                if piece.status() == Piece.Status.IN_PROGRESS:
                    # self.logger.info(f'DownloadQueue - piece {piece.index} is already in progress, retrying later')
                    await self.push(piece)
                    self.requests.task_done()
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    continue  # Retry from queue

                self.requests.task_done()
                if self.checker.is_piece_downloadable(piece_index=piece.index):
                    self.logger.info(f'DownloadQueue - pulling request from the requests of peer {self.ip}:{self.port}')
                    return piece

        except asyncio.CancelledError:
            self.logger.info(f'DownloadQueue - pop cancelled for peer {self.ip}:{self.port}')
            raise
# ******************************************************** EOF *********************************************************
