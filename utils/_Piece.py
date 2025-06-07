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
#                               `Piece` class, which represents an individual piece of a
#                               file in a BitTorrent download. Each piece contains metadata
#                               such as its index, hash value, length, and current download
#                               status. Uses an internal `Status` enum to represent the
#                               download state of the piece (NOT_STARTED, IN_PROGRESS,
#                               COMPLETED, FAILED). Maintains a history of status changes
#                               with timestamps for debugging and analytics.
#
# ******************************************************** IMPORTS *****************************************************

import time
from enum import Enum, auto

# ******************************************************** PIECE *******************************************************

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

# ********************************************************* EOF ********************************************************
