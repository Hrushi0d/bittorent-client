import asyncio
from collections import defaultdict
from typing import Dict, List, Set

from utils._Peer import Peer
from utils._Piece import Piece


class _PeerPool:
    def __init__(self, piece_dict: Dict[int, List[Peer]], piece_order: List[Piece]):
        self.queue = asyncio.Queue()
        self.active_peers: Set[Peer] = set()
        self.piece_dict = piece_dict
        self.piece_order = piece_order
        self.inverted_dict: Dict[str, Set[int]] = defaultdict(set)  # peer_id -> set(piece_index)

    async def initialize(self):
        for piece in self.piece_order:
            peers = self.piece_dict[piece.index]  # use piece.index instead of piece directly
            for peer in peers:
                await self.push(peer)
                self.inverted_dict[peer.peer_id].add(piece.index)