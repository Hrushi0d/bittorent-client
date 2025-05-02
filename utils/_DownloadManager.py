import asyncio
import random
from collections import defaultdict

from utils._Peer import Peer
from utils._Piece import Piece


class DownloadManager:
    def __init__(self, pieces: list[Piece], piece_dict: defaultdict[list[Peer]], max_concurrent_downloads: int = 5):
        self.pieces = pieces
        self.piece_dict = piece_dict
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)

    async def download_from_peer(self, piece, peer): # simulate for now
        print(f"🔄 Attempting {piece} from {peer}")
        await asyncio.sleep(random.uniform(0.5, 1.0))  # simulate network delay
        if random.random() < 0.3:  # simulate 30% failure
            raise Exception(f"Peer {peer} failed")
        print(f"✅ Successfully downloaded {piece} from {peer}")
        return True

    async def download_piece(self, piece):
        peers = self.piece_dict[piece]
        attempt = 0
        max_attempts = len(peers)
        delay = 1

        async with self.semaphore:
            for i in range(max_attempts):
                peers = self.piece_dict[piece][:]
                random.shuffle(peers)
                for peer in peers:
                    try:
                        await self.download_from_peer(piece, peer)
                        return
                    except Exception:
                        continue
            print(f"❌ All peers failed for {piece}")

    async def run(self):
        downloads = [self.download_piece(piece) for piece in self.pieces]
        await asyncio.gather(*downloads)

    # Example usage


if __name__ == "__main__":
    pieces = ['piece1', 'piece2', 'piece3']
    piece_dict = defaultdict(list, {
        'piece1': ['peer1', 'peer2', 'peer3'],
        'piece2': ['peer2', 'peer3'],
        'piece3': ['peer1', 'peer3']
    })

    manager = DownloadManager(pieces, piece_dict, max_concurrent_downloads=2)
    asyncio.run(manager.run())