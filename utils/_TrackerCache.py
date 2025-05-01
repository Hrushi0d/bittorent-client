import json
import logging

from utils.Bencode import Encoder, Decoder
from utils._RedisClient import RedisClient


class TrackerCache:
    def __init__(self, redis_client: RedisClient, ttl=3600):
        self.redis_client = redis_client
        self.ttl = ttl  # Time-to-live for cached responses

    def __repr__(self):
        return 'TrackerCache()'

    def generate_cache_key(self, info_hash):
        """ Generate a unique cache key based on the info_hash. """
        return f"tracker_cache:{info_hash.hex()}"

    async def get_cached_peers(self, info_hash):
        """Retrieve cached peers for a given info_hash from Redis."""
        cache_key = self.generate_cache_key(info_hash)
        cached = await self.redis_client.get_cache(cache_key)
        if cached:
            try:
                # Ensure cached is in bytes before passing to Decoder
                if isinstance(cached, str):
                    cached = cached.encode('utf-8')

                # Decode the Bencoded data
                cached_peers = Decoder(cached).decode()
                # Convert lists back to tuples if needed
                cached_peers = [tuple(peer) if isinstance(peer, list) else peer for peer in cached_peers]
                logging.info(f"Found cached peers for info_hash: {info_hash.hex()}")
                return cached_peers
            except Exception as e:
                logging.error(f"Error decoding cached peers for info_hash {info_hash.hex()}: {e}")
                return None
        else:
            logging.info(f"No cached peers found for info_hash: {info_hash.hex()}")
            return None

    async def cache_peers(self, info_hash, peers):
        """Cache the peers data for a given info_hash."""
        # Ensure peers are serialized to a format compatible with JSON
        peers_list = [list(peer) if isinstance(peer, tuple) else peer for peer in peers]
        encoded_peers = Encoder(peers_list).encode()
        cache_key = self.generate_cache_key(info_hash)
        await self.redis_client.set_cache(cache_key, encoded_peers)
        logging.info(f"Caching peers for info_hash: {info_hash.hex()}")

    def delete_cached_peers(self, info_hash):
        """ Delete the cached peers for a given info_hash. """
        cache_key = self.generate_cache_key(info_hash)
        self.redis_client.delete_cache(cache_key)
        logging.info(f"Deleted cached peers for info_hash: {info_hash.hex()}")
