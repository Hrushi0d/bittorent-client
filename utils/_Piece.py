import time
from enum import Enum, auto


class Block_tracker: # per piece
    def __init__(self, length):
        self.BLOCK_SIZE = 16_384
        self.length = length
        self.num_blocks = (length + self.BLOCK_SIZE - 1) // self.BLOCK_SIZE
        self.block_status = [False] * self.num_blocks
        self.completed_count = 0
        self.finished = False

    def mark_block(self, index: int):
        """Mark a single block as downloaded."""
        if 0 <= index < self.num_blocks and not self.block_status[index]:
            self.block_status[index] = True
            self.completed_count += 1
            if self.completed_count == self.num_blocks:
                self.finished = True

    def add_blocks(self, indices: list[int]):
        """Mark multiple blocks as downloaded."""
        for index in indices:
            self.mark_block(index)

    def is_complete(self) -> bool:
        return self.finished

    def check(self, index: int) -> bool:
        """Check if a block is already downloaded."""
        if 0 <= index < self.num_blocks:
            return self.block_status[index]
        return False

    def remaining(self) -> int:
        return self.num_blocks - self.completed_count

    def progress(self) -> float:
        return self.completed_count / self.num_blocks


class Piece:
    class Status(Enum):
        NOT_STARTED = auto()
        IN_PROGRESS = auto()
        COMPLETED = auto()
        FAILED = auto()

    def __init__(self, index: int, hash_value: bytes, length: int):
        self.index = index
        self.hash_value = hash_value
        self.length = length
        self._status = Piece.Status.NOT_STARTED  # Correctly use Piece.Status
        self._status_history = [(self._status, time.time())]



    def update_status(self, new_status: Status):
        if new_status == self._status:
            return  # no change
        self._status = new_status
        self._status_history.append((new_status, time.time()))

    def status(self):
        return self._status

    def reset_status(self):
        self.update_status(Piece.Status.NOT_STARTED)

    def status_history(self):
        return self._status_history

    def is_completed(self) -> bool:
        return self._status == Piece.Status.COMPLETED

    def __repr__(self):
        return f'Piece(index={self.index}, length={self.length}, status={self._status.value})'


class Block:
    def __init__(self, piece: Piece, block_offset: int, block_length: int, priority: int):
        self.piece = piece
        self.block_offset = block_offset
        self.block_length = block_length
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority
