import asyncio
import hashlib
import logging
import os
import random
import struct
import time
import urllib.parse
from datetime import datetime

import aiohttp
import urllib3

from utils.Bencode import Decoder, Encoder, print_torrent
from utils._DHTClient import _DHTClient
from utils._RedisClient import RedisClient

# Configure logging to write to a file
os.makedirs('../logs', exist_ok=True)
timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
log_filename = f'../logs/peergetter_{timestamp}.log'

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


import asyncio
import hashlib
import logging
import random
import string
import struct
import urllib.parse

import aiohttp
import urllib3

from utils.Bencode import Encoder, Decoder
from utils._RedisClient import RedisClient
from utils._TrackerCache import TrackerCache  # Assuming this is where you defined TrackerCache


class PeerGetter:
    class UDPTrackerProtocol(asyncio.DatagramProtocol):
        def __init__(self):
            self.transport = None
            self.future = asyncio.get_event_loop().create_future()

        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            if not self.future.done():
                self.future.set_result(data)

        def error_received(self, exc):
            if not self.future.done():
                self.future.set_exception(exc)

    def __init__(self, torrent):
        self.torrent = torrent
        self.redis_client = RedisClient()
        self.cache = TrackerCache(self.redis_client)

        if b'info' not in torrent:
            raise ValueError("Torrent metadata does not contain 'info' dictionary.")

        info_dict = torrent[b'info']
        bencoded_info = Encoder(info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()
        self.peer_id = f'-PC0001-{"".join(random.choices(string.ascii_letters + string.digits, k=12))}'.encode('utf-8')
        self.peers_found = False
        self.peers = []
        self.peer_set = set()

        logging.info("Info hash: %s", self.info_hash.hex())
        logging.info("Info hash length: %d", len(self.info_hash))  # Must be 20

    def _parse_compact_format(self, peers_data):
        if not isinstance(peers_data, (bytes, bytearray)):
            logging.error("Invalid 'peers' data format, expected bytes.")
            return []

        peers = []
        for i in range(0, len(peers_data), 6):
            ip = '.'.join(str(b) for b in peers_data[i:i+4])
            port = int.from_bytes(peers_data[i+4:i+6], 'big')
            peers.append((ip, port))
            self.peer_set.add((ip, port))

        self.peers_found = True
        logging.info("Successfully parsed %d peers (compact).", len(peers))
        return peers

    def _parse_verbose_format(self, peer_list):
        if not isinstance(peer_list, list):
            logging.error("Invalid verbose peer data format.")
            return []

        peers = []
        for peer in peer_list:
            ip = peer.get(b'ip')
            port = peer.get(b'port')
            if ip and port:
                peers.append((ip, port))
                self.peer_set.add((ip, port))
            else:
                logging.error("Invalid peer data entry: %s", peer)

        self.peers_found = True
        logging.info("Successfully parsed %d peers (verbose).", len(peers))
        return peers

    def _parse_peers(self, tracker_response):
        peers_data = tracker_response.get(b'peers')
        if peers_data is None:
            logging.error("No peers in tracker response: %s", tracker_response)
            return []

        if isinstance(peers_data, (bytes, bytearray)):
            return self._parse_compact_format(peers_data)
        elif isinstance(peers_data, list):
            return self._parse_verbose_format(peers_data)
        else:
            logging.error("Unknown peers data type: %s", type(peers_data))
            return []

    async def _peers_from_http(self, url):
        if self.peers_found:
            return []

        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme or 'http'
        host = parsed.hostname
        port = parsed.port or (443 if scheme == 'https' else 80)
        path = parsed.path or '/'

        info_hash_q = urllib.parse.quote_from_bytes(self.info_hash)
        peer_id_q = urllib.parse.quote_from_bytes(self.peer_id)
        query = (
            f"info_hash={info_hash_q}&"
            f"peer_id={peer_id_q}&port=6881&uploaded=0&downloaded=0&left=0&event="
        )
        tracker_url = f"{scheme}://{host}:{port}{path}?{query}"
        logging.info("Requesting tracker HTTP/HTTPS: %s", tracker_url)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(tracker_url, timeout=5) as resp:
                    if resp.status != 200:
                        logging.error("Tracker HTTP returned status %d", resp.status)
                        return []
                    body = await resp.read()
                    if not body:
                        logging.error("Empty body from HTTP tracker %s", tracker_url)
                        return []
                    try:
                        data = Decoder(body).decode()
                        return self._parse_peers(data)
                    except Exception as e:
                        logging.error("Failed to decode HTTP tracker response: %s", e)
                        return []
        except asyncio.TimeoutError:
            logging.error("Timeout accessing HTTP tracker %s", tracker_url)
            return []
        except Exception as e:
            logging.error("Error accessing HTTP tracker %s: %s", tracker_url, e)
            return []

    async def _peers_from_udp(self, url):
        if self.peers_found:
            return []

        logging.info("Requesting peers from UDP tracker: %s", url)
        parsed = urllib3.util.parse_url(url)
        host = parsed.host
        port = parsed.port
        if not host or not port:
            logging.error("Invalid UDP URL: %s", url)
            return []

        loop = asyncio.get_running_loop()
        try:
            transport, proto = await loop.create_datagram_endpoint(
                lambda: self.UDPTrackerProtocol(),
                remote_addr=(host, port)
            )
        except Exception as e:
            logging.error("Cannot create UDP endpoint for %s: %s", url, e)
            return []

        try:
            tid = random.getrandbits(32)
            conn_req = struct.pack(">QLL", 0x41727101980, 0, tid)
            proto.transport.sendto(conn_req)
            resp = await asyncio.wait_for(proto.future, timeout=5)
            action, resp_tid, conn_id = struct.unpack(">LLQ", resp)
            if action != 0 or resp_tid != tid:
                logging.error("Invalid UDP connect response: %s", resp)
                return []

            tid = random.getrandbits(32)
            announce_req = struct.pack(
                ">QLL20s20sQQQLLLlh",
                conn_id, 1, tid,
                self.info_hash, self.peer_id,
                0, 0, 0, 0, 0,
                random.getrandbits(32), -1, 6881
            )
            proto.future = loop.create_future()
            proto.transport.sendto(announce_req)
            resp = await asyncio.wait_for(proto.future, timeout=5)

            action, rtid, interval, leechers, seeders = struct.unpack(">LLLLL", resp[:20])
            if action != 1 or rtid != tid:
                logging.error("Invalid UDP announce response: %s", resp)
                return []

            peers = []
            for i in range(20, len(resp), 6):
                ip = '.'.join(str(b) for b in resp[i:i+4])
                port_num = struct.unpack(">H", resp[i+4:i+6])[0]
                peers.append((ip, port_num))
                self.peer_set.add((ip, port_num))

            self.peers_found = True
            logging.info("Found %d peers via UDP tracker %s", len(peers), url)
            return peers
        except asyncio.TimeoutError:
            logging.error("Timeout in UDP tracker %s", url)
            return []
        except Exception as e:
            logging.error("Error during UDP tracker communication %s: %s", url, e)
            return []
        finally:
            transport.close()

    async def _peers_from_single_url(self, url):
        url = url.decode('utf-8') if isinstance(url, (bytes, bytearray)) else url
        if url.startswith('udp://'):
            return await self._peers_from_udp(url)
        elif url.startswith('http://') or url.startswith('https://'):
            return await self._peers_from_http(url)
        else:
            logging.error("Unsupported tracker protocol: %s", url)
            return []

    async def _peers_from_multiple_urls(self, announce_list):
        tasks = []
        for tier in announce_list:
            for url in tier:
                tasks.append(self._peers_from_single_url(url))
        await asyncio.gather(*tasks)
        self.peers = list(self.peer_set)

    async def get(self):
        # Try to get from cache first
        cached_peers = await self.cache.get_cached_peers(self.info_hash)
        if cached_peers:
            self.peers = cached_peers
            return self.peers

        # No cache, fetch from trackers
        if b'announce-list' in self.torrent:
            await self._peers_from_multiple_urls(self.torrent[b'announce-list'])
        elif b'announce' in self.torrent:
            await self._peers_from_single_url(self.torrent[b'announce'])
        else:
            # Fallback to DHT or other method
            dht = _DHTClient(self.info_hash)
            self.peers = await dht.peers_from_DHT()

        # Save fetched peers to cache
        await self.cache.cache_peers(self.info_hash, self.peers)
        return self.peers



if __name__ == '__main__':
    try:
        # Start the timer
        start_time = time.time()

        with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
            meta_info = f.read()
            torrent = Decoder(meta_info).decode()
            # info_dict = torrent[b'info']
            # bencoded_info = Encoder(info_dict).encode()
            # info_hash = hashlib.sha1(bencoded_info).digest()

            # print_torrent(torrent)

            peergetter = PeerGetter(torrent=torrent)

            # Run peer discovery asynchronously
            asyncio.run(peergetter.get())

            # Calculate the elapsed time
            elapsed_time = time.time() - start_time

            # Log the peers and the time taken
            logging.info("Peers: %s", peergetter.peers)
            print("Peers: ", peergetter.peers)
            logging.info(f"Peer discovery took {elapsed_time:.2f} seconds.")
            print(f"Peer discovery took {elapsed_time:.2f} seconds.")

    except Exception as e:
        logging.exception("An error occurred during peer discovery.")
        print(f"An error occurred: {e}")

    finally:
        print(f"Logs saved to {log_filename}")
