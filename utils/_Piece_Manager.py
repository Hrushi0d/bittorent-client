import asyncio
import logging
import pickle
import random
from collections import defaultdict
from tqdm import tqdm

from utils.Bencode import Decoder
from utils._Piece import Piece


def _get_last_piece_length(files, piece_length):
    total_length = sum(file[b'length'] for file in files)
    return total_length % piece_length


class PieceManager:
    def __init__(self, piece_dict: defaultdict[list], torrent, mode, logger):
        self.piece_dict = piece_dict
        self.torrent = torrent
        self.mode = mode
        self.info_dict = torrent[b'info']
        self.logger = logger
        self.logger.info(f"Initialized PieceManager with mode: {mode}")
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
                self.logger.debug(f"Last piece, adjusted length: {piece_length}")
                pass

        self.logger.debug(f"Piece {index} - Hash: {piece_hash.hex()}, Length: {piece_length}")
        return piece_hash, piece_length

    def _load_pieces(self, order):
        pieces = []
        for index in order:
            self.logger.debug(f"Loading piece {index}")
            hash_value, length = self._get_piece_info(index=index)
            piece = Piece(index=index, hash_value=hash_value, length=length)
            pieces.append(piece)
        self.logger.info(f"Loaded {len(pieces)} pieces.")
        return pieces

    def _rarest_first(self):
        self.logger.info("Selecting pieces using 'rarest-first' strategy.")
        pieces = sorted(
            ((piece_index, peers) for piece_index, peers in self.piece_dict.items() if
             piece_index not in self.pieces_done),
            key=lambda item: len(item[1])  # Sorting by the number of peers
        )
        self.logger.debug(f"Sorted pieces by rarity (number of peers).")
        pieces = self._load_pieces([piece for piece, _ in pieces if piece not in self.pieces_done])
        return pieces

    def _random_rarest_first(self):
        rarity_groups = defaultdict(list)
        self.logger.info("Selecting pieces using 'random-rarest-first' strategy.")
        for piece_index, peers in self.piece_dict.items():
            if piece_index in self.pieces_done:
                continue
            rarity = len(peers)
            rarity_groups[rarity].append(piece_index)

        sorted_rarities = sorted(rarity_groups.items(), key=lambda item: item[0])
        self.logger.debug(f"Sorted rarity groups.")

        sorted_order = []
        for _, piece_indices in sorted_rarities:
            random.shuffle(piece_indices)
            sorted_order.extend(piece_indices)

        self.logger.debug(f"Shuffled pieces within rarity group.")
        return self._load_pieces(sorted_order)

    def _sequential(self):
        """
        Sequentially download pieces based on their index, from 0 to the last piece.
        """
        self.logger.info("Selecting pieces using 'sequential' strategy.")
        remaining_pieces = [piece_index for piece_index in sorted(self.piece_dict.keys()) if
                            piece_index not in self.pieces_done]
        self.logger.debug(f"Remaining pieces to download: {remaining_pieces}")
        return self._load_pieces(remaining_pieces)

    def run(self):
        self.logger.info(f"Running PieceManager with mode: {self.mode}")
        # pieces = None
        if self.mode == 'rarest-first':
            pieces = self._rarest_first()
        elif self.mode == 'random-rarest-first':
            pieces = self._random_rarest_first()
        else:
            pieces = self._sequential()
        self.logger.info(f"Completed piece selection for mode: {self.mode}.")
        return pieces


def load_piece_dict(pickle_path="pieces.pkl"):
    with open(pickle_path, "rb") as f:
        piece_dict = pickle.load(f)
    return piece_dict


async def main():
    with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        piece_dict = load_piece_dict()
        for key, value in piece_dict.items():
            print(f"Key type: {type(key)}, Value type: {type(value[0])},{value[0]}")
            break  # Remove this if you want to inspect all entries
        manager = PieceManager(piece_dict, torrent, 'random-rarest-first', logger=logging.getLogger())
    # results = await manager.run()

    # total_pieces = len(piece_dict)
    # print(f"\nDownloaded pieces: {len(manager.pieces_done)}/{total_pieces}")
    # total_peers = {peer for peers in piece_dict.values() for peer in peers}
    # percent_peers_used = 100 * len(manager.peers_used) / len(total_peers)
    # print(f"Percentage of peers used: {percent_peers_used:.2f}%")


if __name__ == "__main__":
    asyncio.run(main())
