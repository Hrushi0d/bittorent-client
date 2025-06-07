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
#                               `PeerGetter` class, responsible for robust peer discovery
#                               in a BitTorrent client. It enables the collection of peer
#                               addresses from various sources: HTTP/HTTPS trackers, UDP
#                               trackers, and the Distributed Hash Table (DHT), with
#                               support for peer caching and tracker augmentation.
#
#                               It first checks with the redis cache, if its online and
#                               has cached peers, and uses them else it retrieves peers
#                               from the trackers given in announce and announce-list
#                               sections of the .torrent file. it then identifies the
#                               type of tracker and uses HTTP, HTTPS, or UDP to return
#                               the peers, parse them into a list of peers and return
#                               said list, if the .torrent file has no announce or
#                               announce-list section it uses DHT client to retrieve
#                               peers [See, utils._DHTClient]

# ******************************************************** IMPORTS *****************************************************

import asyncio
import hashlib
import ipaddress
import logging
import random
import string
import struct
import urllib.parse

import aiohttp
import urllib3

from utils._Bencode import Encoder, Decoder
from utils._DHTClient import _DHTClient
from utils._RedisClient import RedisClient
from utils._TrackerCache import TrackerCache


# ****************************************************** FUNCTIONS *****************************************************

def is_valid_peer(ip, port: int) -> bool:
    """
    Returns True if ip is a valid IPv4 or IPv6 address (not multicast, not unspecified, not loopback, not private)
    and port is in the valid BitTorrent peer port range (1-65535), excluding reserved/blocked ports.
    """
    try:
        ip = ip.decode() if isinstance(ip, bytes) else ip
        ip_obj = ipaddress.ip_address(ip)
        if (
                ip_obj.is_multicast
                or ip_obj.is_unspecified
                or ip_obj.is_loopback
                or ip_obj.is_link_local
                or ip_obj.is_reserved
                or ip_obj.is_private
        ):
            return False
    except ValueError:
        return False

    # Commonly blocked ports (0, 135-139, 445, 1024, >65535)
    if not (1 <= port <= 65535):
        return False
    if port in {135, 136, 137, 138, 139, 445, 0}:
        return False

    return True


def load_extra_trackers(file_path="extra_trackers.txt"):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except FileNotFoundError:
        return []

# ****************************************************** PEER GETTER ***************************************************


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

    def __init__(self, torrent, logger: logging.Logger):
        self.torrent = torrent
        self.redis_client = RedisClient()
        self.cache = TrackerCache(self.redis_client)
        self.logger = logger

        if b'info' not in torrent:
            raise ValueError("Torrent metadata does not contain 'info' dictionary.")

        info_dict = torrent[b'info']
        bencoded_info = Encoder(info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()
        self.peer_id = f'-PC0001-{"".join(random.choices(string.ascii_letters + string.digits, k=12))}'.encode('utf-8')
        self.peers_found = False
        self.peers = []
        self.peer_set = set()

        self.logger.info("PeerGetter - Info hash: %s", self.info_hash.hex())
        self.logger.info("PeerGetter - Info hash length: %d", len(self.info_hash))  # Must be 20

    def __repr__(self):
        return f'PeerGetter(info_hash={self.info_hash})'

    def _parse_compact_format(self, peers_data):
        if not isinstance(peers_data, (bytes, bytearray)):
            self.logger.error("PeerGetter - Invalid 'peers' data format, expected bytes.")
            return []

        peers = []
        for i in range(0, len(peers_data), 6):
            ip = '.'.join(str(b) for b in peers_data[i:i + 4])
            port = int.from_bytes(peers_data[i + 4:i + 6], 'big')
            if is_valid_peer(ip, port):
                peers.append((ip, port))
                self.peer_set.add((ip, port))

        self.peers_found = True
        self.logger.info("PeerGetter - Successfully parsed %d peers (compact).", len(peers))
        return peers

    def _parse_verbose_format(self, peer_list):
        if not isinstance(peer_list, list):
            self.logger.error("PeerGetter - Invalid verbose peer data format.")
            return []

        peers = []
        for peer in peer_list:
            ip = peer.get(b'ip')
            port = peer.get(b'port')
            if ip and port and is_valid_peer(ip, port):
                peers.append((ip, port))
                self.peer_set.add((ip, port))
            else:
                self.logger.error("PeerGetter - Invalid peer data entry: %s", peer)

        self.peers_found = True
        self.logger.info("PeerGetter - Successfully parsed %d peers (verbose).", len(peers))
        return peers

    def _parse_peers(self, tracker_response):
        peers_data = tracker_response.get(b'peers')
        if peers_data is None:
            self.logger.error("PeerGetter - No peers in tracker response: %s", tracker_response)
            return []

        if isinstance(peers_data, (bytes, bytearray)):
            return self._parse_compact_format(peers_data)
        elif isinstance(peers_data, list):
            return self._parse_verbose_format(peers_data)
        else:
            self.logger.error("PeerGetter - Unknown peers data type: %s", type(peers_data))
            return []

    async def _peers_from_http(self, url):
        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme or 'http'
        host = parsed.hostname
        port = parsed.port or (443 if scheme == 'https' else 80)
        path = parsed.path or '/'

        info_hash_q = urllib.parse.quote_from_bytes(self.info_hash)
        peer_id_q = urllib.parse.quote_from_bytes(self.peer_id)
        # Consider event=started and parameterize left/port if needed
        query = (
            f"info_hash={info_hash_q}&"
            f"peer_id={peer_id_q}&port=6881&uploaded=0&downloaded=0&left=0&event=started"
        )
        tracker_url = f"{scheme}://{host}:{port}{path}?{query}"
        self.logger.info("PeerGetter - Requesting tracker HTTP/HTTPS: %s", tracker_url)

        headers = {'User-Agent': 'uTorrent/3.5.5'}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(tracker_url, timeout=5, headers=headers) as resp:
                    if resp.status != 200:
                        self.logger.error("PeerGetter - Tracker HTTP returned status %d", resp.status)
                        return []
                    body = await resp.read()
                    if not body:
                        self.logger.error("PeerGetter - Empty body from HTTP tracker %s", tracker_url)
                        return []
                    try:
                        data = Decoder(body).decode()
                        return self._parse_peers(data)
                    except Exception as e:
                        self.logger.error("PeerGetter - Failed to decode HTTP tracker response: %s", e)
                        return []
        except asyncio.TimeoutError:
            self.logger.error("PeerGetter - Timeout accessing HTTP tracker %s", tracker_url)
            return []
        except Exception as e:
            self.logger.error("PeerGetter - Error accessing HTTP tracker %s: %s", tracker_url, e)
            return []

    async def _peers_from_udp(self, url):
        if self.peers_found:
            return []

        self.logger.info("PeerGetter - Requesting peers from UDP tracker: %s", url)
        parsed = urllib3.util.parse_url(url)
        host = parsed.host
        port = parsed.port
        if not host or not port:
            self.logger.error("PeerGetter - Invalid UDP URL: %s", url)
            return []

        loop = asyncio.get_running_loop()
        try:
            transport, proto = await loop.create_datagram_endpoint(
                lambda: self.UDPTrackerProtocol(),
                remote_addr=(host, port)
            )
        except Exception as e:
            self.logger.error("PeerGetter - Cannot create UDP endpoint for %s: %s", url, e)
            return []

        try:
            tid = random.getrandbits(32)
            conn_req = struct.pack(">QLL", 0x41727101980, 0, tid)
            proto.transport.sendto(conn_req)
            resp = await asyncio.wait_for(proto.future, timeout=5)
            action, resp_tid, conn_id = struct.unpack(">LLQ", resp)
            if action != 0 or resp_tid != tid:
                self.logger.error("PeerGetter - Invalid UDP connect response: %s", resp)
                return []

            tid = random.getrandbits(32)
            announce_req = struct.pack(
                ">QLL20s20sQQQLLLlh",
                conn_id,             # 0–7   : connection_id      (uint64)
                1,                       # 8–11  : action = 1 (announce)
                tid,                     # 12–15 : transaction_id      (uint32)
                self.info_hash,          # 16–35 : info_hash (20 bytes)
                self.peer_id,            # 36–55 : peer_id  (20 bytes)
                0,                       # 56–63 : downloaded (uint64)
                0,                       # 64–71 : left       (uint64)
                0,                       # 72–79 : uploaded   (uint64)
                0,                       # 80–83 : event = 0 (none)
                0,                       # 84–87 : IP address = 0 (default)
                random.getrandbits(32),  # 88–91 : key
                -1,                      # 92–95 : num_want = -1
                6881                     # 96–97 : port
            )
            proto.future = loop.create_future()
            proto.transport.sendto(announce_req)
            resp = await asyncio.wait_for(proto.future, timeout=5)

            action, rtid, interval, leechers, seeders = struct.unpack(">LLLLL", resp[:20])
            if action != 1 or rtid != tid:
                self.logger.error("PeerGetter - Invalid UDP announce response: %s", resp)
                return []

            peers = []
            for i in range(20, len(resp), 6):
                ip = '.'.join(str(b) for b in resp[i:i + 4])
                port_num = struct.unpack(">H", resp[i + 4:i + 6])[0]
                peers.append((ip, port_num))
                self.peer_set.add((ip, port_num))

            self.peers_found = True
            self.logger.info("PeerGetter - Found %d peers via UDP tracker %s", len(peers), url)
            return peers
        except asyncio.TimeoutError:
            self.logger.error("PeerGetter - Timeout in UDP tracker %s", url)
            return []
        except Exception as e:
            self.logger.error("PeerGetter - Error during UDP tracker communication %s: %s", url, e)
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
            self.logger.error("PeerGetter - Unsupported tracker protocol: %s", url)
            return []

    async def _peers_from_multiple_urls(self, announce_list):
        tasks = []

        # Existing trackers from the .torrent file
        for tier in announce_list:
            for url in tier:
                tasks.append(self._peers_from_single_url(url))

        # Optional: Add extra trackers from NGOsang's list
        extra_trackers = load_extra_trackers()
        for url in extra_trackers:
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
            async with _DHTClient(self.info_hash, self.logger) as dht:
                self.peers = await dht.peers_from_DHT()

        # Save fetched peers to cache
        await self.cache.cache_peers(self.info_hash, self.peers)
        return self.peers

# ********************************************************** EOF *******************************************************
