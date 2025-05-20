import asyncio
import logging
from typing import Dict, Optional, List, Set
from dataclasses import dataclass, field

from utils._Piece import Piece


@dataclass
class _PieceInfo:
    index: int
    data: bytes
    hash_value: bytes
    timestamp: float = field(default_factory=lambda: asyncio.get_running_loop().time())


class AsyncQueue:
    """
    An asyncio-based queue for managing unique torrent pieces with support
    for waiting on specific or any piece, duplicate prevention, and safe closure.
    Not thread-safe; use only in a single event loop.
    """
    def __init__(self, logger: logging.Logger, max_size: int = 1000):
        self._queue = asyncio.Queue(maxsize=max_size)
        self._piece_set: Set[int] = set()  # Track piece indices in the queue to avoid duplicates
        self._processing: Set[int] = set()  # Track pieces currently being processed
        self._piece_events: Dict[int, asyncio.Event] = {}  # Events for waiting on specific pieces
        self._piece_results: Dict[int, _PieceInfo] = {}  # Store results for specific pieces
        self._listeners: List[asyncio.Future] = []  # Futures for listeners waiting for any piece
        self._closed = False
        self.logger = logger

    async def push(self, piece: Piece, piece_data: bytes):
        piece_info = _PieceInfo(index=piece.index,data=piece_data,hash_value=piece.hash_value)
        if self._closed:
            self.logger.warning("AsyncQueue - Attempted to push to a closed queue.")
            return
        if piece_info.index in self._piece_set or piece_info.index in self._processing:
            self.logger.debug(f"AsyncQueue - Duplicate piece {piece_info.index} ignored.")
            return

        await self._queue.put(piece_info)
        self._piece_set.add(piece_info.index)
        self._piece_results[piece_info.index] = piece_info

        # Notify any listeners waiting on this piece
        event = self._piece_events.get(piece_info.index)
        if event:
            event.set()

        # Notify generic listeners waiting on *any* piece
        for fut in self._listeners:
            if not fut.done():
                try:
                    fut.set_result(piece_info)
                except Exception as e:
                    self.logger.error(f"AsyncQueue - Failed to notify listener: {e}")
        self._listeners.clear()
        self.logger.debug(f"AsyncQueue - Pushed piece {piece_info.index} (size={len(piece_data)} bytes) to queue.")

    async def pop(self) -> _PieceInfo:
        if self._closed and self._queue.empty():
            raise RuntimeError("AsyncQueue - Queue is closed and empty")
        piece = await self._queue.get()
        self._piece_set.discard(piece.index)
        self._processing.add(piece.index)
        return piece

    def task_done(self, piece_index: int):
        # Only call task_done if there are unfinished tasks
        try:
            self._queue.task_done()
        except ValueError:
            pass  # Defensive: ignore over-calling

        self._processing.discard(piece_index)
        # Clean up result/event to avoid memory leak
        self._piece_results.pop(piece_index, None)
        event = self._piece_events.pop(piece_index, None)
        if event:
            event.clear()  # Defensive, but not strictly needed

    async def wait_for_piece(self, piece_index: int, timeout: Optional[float] = None) -> Optional[_PieceInfo]:
        # If already processed or queued, return immediately if possible
        if piece_index in self._piece_results:
            return self._piece_results[piece_index]
        # If the piece is expected but not processed yet, wait for it
        loop = asyncio.get_running_loop()
        if piece_index not in self._piece_events:
            self._piece_events[piece_index] = asyncio.Event()
        event = self._piece_events[piece_index]
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return self._piece_results.get(piece_index)
        except asyncio.TimeoutError:
            return None

    async def wait_for_any(self, timeout: Optional[float] = None) -> Optional[_PieceInfo]:
        if self._closed and self._queue.empty():
            return None

        if not self._queue.empty():
            return await self.pop()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._listeners.append(future)

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            if future in self._listeners:
                self._listeners.remove(future)

    def close(self):
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed
