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

    async def _handle_peer(self, peer, peer_results):
        try:
            # Using async with to handle connect() and disconnect() automatically
            async with peer:
                # placeholder for real messaging logic (like requesting piece info)
                await asyncio.sleep(0.1)  # Simulate async work
                # Successfully connected and handled, update peer results dictionary
                peer_results[peer] = True
                self.logger.info(f"Successfully handled peer {peer.ip}:{peer.port}")
                return True
        except Exception as e:
            # Log any exceptions that occur while handling the peer
            self.logger.error(f"Error handling peer {peer.ip}:{peer.port} - {e}")
            # Mark the peer as failed in the results dictionary
            peer_results[peer] = False
            return False

    async def run(self):
        self.logger.info("Starting peer connection process.")
        await self._get_peers()

        # Dictionary to store peer success/failure
        peer_results = {}

        # Create a list of tasks to handle each peer connection
        tasks = [self._handle_peer(peer, peer_results) for peer in self.peers]

        # Wait for all peer handling tasks to finish
        results = await asyncio.gather(*tasks)

        # Filter successful and failed peers
        successful_peers = [peer for peer, success in peer_results.items() if success]
        failed_peers = [peer for peer, success in peer_results.items() if not success]

        # Log the number of successful and failed connections
        self.logger.info(f"Number of successful connections (True): {len(successful_peers)}")
        self.logger.info(f"Number of failed connections (False): {len(failed_peers)}")

        # Optionally: log or process the successful and failed peers
        self.logger.info(f"Successful Peers: {successful_peers}")
        self.logger.info(f"Failed Peers: {failed_peers}")

