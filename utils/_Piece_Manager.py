# **********************************************************************************************************************
# 							    _________  ________  ________  ________  _______   ________   _________
# 							   |\___   ___\\   __  \|\   __  \|\   __  \|\  ___ \ |\   ___  \|\___   ___\
# 							   \|___ \  \_\ \  \|\  \ \  \|\  \ \  \|\  \ \   __/|\ \  \\ \  \|___ \  \_|
# 							        \ \  \ \ \  \\\  \ \   _  _\ \   _  _\ \  \_|/_\ \  \\ \  \   \ \  \
# 							         \ \  \ \ \  \\\  \ \  \\  \\ \  \\  \\ \  \_|\ \ \  \\ \  \   \ \  \
# 							          \ \__\ \ \_______\ \__\\ _\\ \__\\ _\\ \_______\ \__\\ \__\   \ \__\
# 							           \|__|  \|_______|\|__|\|__|\|__|\|__|\|_______|\|__| \|__|    \|__|
#
#                                                             INFO ABOUT THIS FILE
#                                           `PieceManager` class, which encapsulates the logic for
#                                           selecting and managing pieces to download in a BitTorrent
#                                           client. It supports multiple piece selection strategies
#                                           to optimize the download process based on piece rarity
#                                           and availability.
#
#                                            - Sequential : Downloads pieces in order from first to last.
#
#                                            - Rarest-First: Prioritizes pieces that are available
#                                              from the fewest peers, helping ensure rare pieces
#                                              are acquired before they disappear from the swarm.
#
#                                            - Random Rarest-First: Randomizes the order among
#                                              pieces of equal rarity for improved swarm health
#                                              and parallelism.
#
# ******************************************************** IMPORTS *****************************************************

import logging
import pickle
import random
from collections import defaultdict

from utils._Piece import Piece


# ******************************************************** FUNCTIONS ***************************************************


def _get_last_piece_length(files, piece_length):
    total_length = sum(file[b'length'] for file in files)
    return total_length % piece_length


def load_piece_dict(pickle_path="pieces.pkl"):
    with open(pickle_path, "rb") as f:
        piece_dict = pickle.load(f)
    return piece_dict


# ***************************************************** PIECE MANAGER **************************************************

class PieceManager:
    def __init__(self, piece_dict: defaultdict[list], torrent, mode, logger: logging.Logger):
        self.piece_dict = piece_dict
        self.torrent = torrent
        self.mode = mode
        self.info_dict = torrent[b'info']
        self.logger = logger
        self.logger.info(f"PieceManager - Initialized with mode: {mode}")
        self.pieces_done = set()
        self.peers_used = set()

    def _get_piece_info(self, index):
        piece_length = self.info_dict[b'piece length']
        pieces_data = self.info_dict[b'pieces']
        files = self.info_dict.get(b'files', None)

        start = index * 20
        end = start + 20
        piece_hash = pieces_data[start:end]

        if files:
            total_pieces = len(pieces_data) // 20
            if index == total_pieces - 1:
                piece_length = _get_last_piece_length(files, piece_length)
                self.logger.debug(f"PieceManager - Last piece, adjusted length: {piece_length}")
                pass

        self.logger.debug(f"PieceManager - Piece {index} - Hash: {piece_hash.hex()}, Length: {piece_length}")
        return piece_hash, piece_length

    def _load_pieces(self, order):
        pieces = []
        for index in order:
            self.logger.debug(f"PieceManager - Loading piece {index}")
            hash_value, length = self._get_piece_info(index=index)
            piece = Piece(index=index, hash_value=hash_value, length=length)
            pieces.append(piece)
        self.logger.info(f"PieceManager - Loaded {len(pieces)} pieces.")
        return pieces

    def _rarest_first(self):
        self.logger.info("PieceManager - Selecting pieces using 'rarest-first' strategy.")
        pieces = sorted(
            ((piece_index, peers) for piece_index, peers in self.piece_dict.items() if
             piece_index not in self.pieces_done),
            key=lambda item: len(item[1])  # Sorting by the number of peers
        )
        self.logger.debug(f"PieceManager - Sorted pieces by rarity (number of peers).")
        pieces = self._load_pieces([piece for piece, _ in pieces if piece not in self.pieces_done])
        return pieces

    def _random_rarest_first(self):
        rarity_groups = defaultdict(list)
        self.logger.info("PieceManager - Selecting pieces using 'random-rarest-first' strategy.")
        for piece_index, peers in self.piece_dict.items():
            if piece_index in self.pieces_done:
                continue
            rarity = len(peers)
            rarity_groups[rarity].append(piece_index)

        sorted_rarities = sorted(rarity_groups.items(), key=lambda item: item[0])
        self.logger.debug(f"PieceManager - Sorted rarity groups.")

        sorted_order = []
        for _, piece_indices in sorted_rarities:
            random.shuffle(piece_indices)
            sorted_order.extend(piece_indices)

        self.logger.debug(f"PieceManager - Shuffled pieces within rarity group.")
        return self._load_pieces(sorted_order)

    def _sequential(self):
        """
        Sequentially download pieces based on their index, from 0 to the last piece.
        """
        self.logger.info("PieceManager - Selecting pieces using 'sequential' strategy.")
        remaining_pieces = [piece_index for piece_index in sorted(self.piece_dict.keys()) if
                            piece_index not in self.pieces_done]
        self.logger.debug(f"PieceManager - Remaining pieces to download: {remaining_pieces}")
        return self._load_pieces(remaining_pieces)

    def run(self):
        self.logger.info(f"PieceManager - Running PieceManager with mode: {self.mode}")
        # pieces = None
        if self.mode == 'rarest-first':
            pieces = self._rarest_first()
        elif self.mode == 'random-rarest-first':
            pieces = self._random_rarest_first()
        else:
            pieces = self._sequential()
        self.logger.info(f"PieceManager - Completed piece selection for mode: {self.mode}.")
        return pieces

# ********************************************************** EOF *******************************************************
