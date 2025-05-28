import asyncio
import logging
from collections import defaultdict

from utils._AsyncQueue import AsyncQueue
from utils._DownloadQueue import QueueClosed, DownloadChecker
from utils._Peer import Peer
from utils._Piece import Piece


class DownloadManager:
    def __init__(self, logger: logging.Logger, async_queue: AsyncQueue, pieces: list[Piece],
                 piece_dict: defaultdict[int, list[Peer]],
                 max_concurrent_pieces=50, max_concurrent_blocks=200):
        self.logger = logger
        logger.info('DownloadManager - Initialized')
        self.pieces_order = pieces
        self.piece_dict = piece_dict
        self.peer_dict = defaultdict(list)
        for piece in self.pieces_order:
            peers = self.piece_dict[piece.index]  # use piece.index instead of piece directly
            for peer in peers:
                self.peer_dict[(peer.ip, peer.port)].append(piece.index)

        self.piece_semaphore = asyncio.Semaphore(max_concurrent_pieces)
        self.block_semaphore = asyncio.Semaphore(max_concurrent_blocks)
        self.async_queue = async_queue
        self.active_peers = set()
        self.checker = DownloadChecker(piece_dict=self.piece_dict, peer_dict=self.peer_dict)
        self._running = True
        self._peer_tasks = []

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

    async def start(self):
        self.logger.info(f'DownloadManager - started downloading')

        async def limited_download(peer: Peer):
            while self._running:
                async with self.piece_semaphore:
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
                        self.checker.mark_piece_completed(piece_index=piece.index)
                    except Exception as e:
                        self.logger.error(
                            f"DownloadManager - failed downloading piece {piece.index} from {peer.ip}:{peer.port}: {e}")
                        piece.update_status(Piece.Status.FAILED)
                        self.checker.mark_peer_failed_for_piece(peer=peer, piece_index=piece.index)

        self._peer_tasks = [asyncio.create_task(limited_download(peer)) for peer in self.active_peers]
        await asyncio.gather(*self._peer_tasks)
        self.logger.info('DownloadManager - all downloads complete.')

    async def stop(self):
        """Signal all download loops to exit and cancel tasks."""
        self._running = False
        for task in self._peer_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._peer_tasks, return_exceptions=True)
        self.logger.info('DownloadManager - stopped.')



    # async def setup_requests_2(self):
    #     for rank in range(len(self.pieces_order)):
    #         piece = self.pieces_order[rank]
    #         peers = self.piece_dict[piece.index]
    #         for peer in peers:
    #             await peer.download_queue.push(block, rank)
    #             self.active_peers.add(peer)