import time
from enum import Enum, auto


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
        self._status = Piece.Status.NOT_STARTED
        self._status_history = [(self._status, time.time())]

    def update_status(self, new_status: Status):
        if new_status == self._status:
            return  # no change
        self._status = new_status
        self._status_history.append((new_status, time.time()))

    def reset_status(self):
        self.update_status(Piece.Status.NOT_STARTED)

    def status_history(self):
        return self._status_history

    def is_completed(self) -> bool:
        return self._status == Piece.Status.COMPLETED

    def __repr__(self):
        return f'Piece(index={self.index}, length={self.length})'
