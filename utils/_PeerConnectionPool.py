import asyncio
import hashlib

from utils.Bencode import Encoder
from utils._Peer import Peer
from utils._PeerGetter import PeerGetter


class PeerConnectionPool:
    def __init__(self, torrent, logger):
        self.torrent = torrent
        self.logger = logger
        self.peers = []
        info_dict = torrent[b'info']
        bencoded_info = Encoder(info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()
        self._peer_getter = PeerGetter(torrent=self.torrent, logger=self.logger)

        self.logger.info("PeerConnectionPool initialized with info_hash: %s",
                         self.info_hash.hex())

    async def _get_peers(self):
        self.logger.info("Attempting to retrieve peers.")
        try:
            peers = await self._peer_getter.get()
            for ip, port in peers:
                self._add_peer(ip, port)
            self.logger.info("Successfully retrieved and added peers.")
        except Exception as e:
            self.logger.error("Error while retrieving peers: %s", e)

    def _add_peer(self, ip, port):
        peer = Peer(ip=ip, port=port, info_hash=self.info_hash, logger=self.logger)
        self.peers.append(peer)
        self.logger.info("Added peer %s:%d to connection pool.", ip, port)

    async def _handle_peer(self, peer):
        try:
            # async with handles both connect() in __aenter__ and disconnect() in __aexit__
            async with peer:
                # placeholder for real messaging logic
                await asyncio.sleep(0.1)
                return True
        except Exception as e:
            self.logger.error("Error handling peer %s:%d - %s",
                              peer.ip, peer.port, e)
            return False

    async def run(self):
        self.logger.info("Starting peer connection process.")
        await self._get_peers()

        tasks = [self._handle_peer(peer) for peer in self.peers]
        results = await asyncio.gather(*tasks)

        # Count the True and False values
        true_count = results.count(True)
        false_count = results.count(False)

        # Log the connection results
        self.logger.info("Connection process completed with results: %s", results)
        self.logger.info("Number of successful connections (True): %d", true_count)
        self.logger.info("Number of failed connections (False): %d", false_count)
