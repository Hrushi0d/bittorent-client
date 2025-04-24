import asyncio
import datetime
import hashlib
import logging
import random
import socket
import string
import struct
import urllib.parse
from collections import deque, defaultdict

import aiohttp
import urllib3
from pydantic import BaseModel

from utils.Bencode import Decoder, Encoder

# Configure logging to write to a file
logging.basicConfig(
    filename='../peergetter.log',  # Log file name
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
)


class Node(BaseModel):
    node_id: bytes
    ip: str
    port: int
    last_seen: datetime.datetime

    def to_dict(self):
        return {
            "node_id": self.node_id.hex(),
            "ip": self.ip,
            "port": self.port,
            "last_seen": self.last_seen.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            node_id=bytes.fromhex(data["node_id"]),
            ip=data["ip"],
            port=data["port"],
            last_seen=datetime.datetime.fromisoformat(data["last_seen"])
        )


class PeerGetter(object):
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
        if b'info' not in torrent:
            raise ValueError("Torrent metadata does not contain 'info' dictionary.")
        info_dict = self.torrent[b'info']
        bencoded_info = Encoder(info_dict).encode()
        self.info_hash = hashlib.sha1(bencoded_info).digest()
        self.peer_id = f'-PC0001-{"".join(random.choices(string.ascii_letters + string.digits, k=12))}'.encode('utf-8')
        self.peers_found = False
        self.peers = []
        self.peer_set = set()
        logging.info("Info hash: %s", self.info_hash)
        logging.info("Info hash length: %d", len(self.info_hash))  # Must be 20

    def _parse_compact_format(self, tracker_response):
        if b'peers' in tracker_response:
            peers_data = tracker_response[b'peers']

            # Ensure peers_data is of type 'bytes'
            if not isinstance(peers_data, bytes):
                logging.error("Invalid 'peers' data format, expected bytes.")
                return []

            self.peers = []

            # Extract peers from the response data
            for i in range(0, len(peers_data), 6):
                # Extract the IP address (4 bytes)
                ip = '.'.join(str(byte) for byte in peers_data[i:i + 4])

                # Extract the port number (2 bytes, big-endian)
                port = int.from_bytes(peers_data[i + 4:i + 6], 'big')

                # Append the peer (IP, port) to the list
                self.peers.append((ip, port))
                self.peer_set.add((ip, port))

            self.peers_found = True  # Set the flag to indicate peers were found
            logging.info("Successfully parsed %d peers.", len(self.peers))
            return self.peers

        else:
            logging.error("No peers found in response.")
            self.peers_found = False
            return []

    def _parse_verbose_format(self, data):
        if isinstance(data, list):
            self.peers = []

            for peer in data:
                # Extract IP address and port from the peer dictionary
                ip = peer.get(b'ip', None)
                port = peer.get(b'port', None)

                if ip and port:
                    self.peers.append((ip, port))
                    self.peer_set.add((ip, port))
                else:
                    logging.error("Invalid peer data: %s", peer)

            self.peers_found = True  # Set the flag to indicate peers were found
            logging.info("Successfully parsed %d verbose peers.", len(self.peers))
            return self.peers
        else:
            logging.error("Invalid verbose peer data format.")
            self.peers_found = False
            return []

    def _parse_peers(self, tracker_response):
        if b'peers' in tracker_response:
            peers_data = tracker_response[b'peers']

            # Check if 'peers' is a bytes object (compact format)
            if isinstance(peers_data, bytes):
                return self._parse_compact_format(peers_data)

            # Check if 'peers' is a list of dictionaries (verbose format)
            elif isinstance(peers_data, list) and isinstance(peers_data[0], dict):
                return self._parse_verbose_format(peers_data)

        return []

    async def _peers_from_http(self, url):
        if self.peers_found:
            return

        # Parse the URL
        data = urllib3.util.parse_url(url)
        path = data.path or '/'
        host = data.host
        port = data.port or 443  # Default to port 443 for HTTPS

        # URL encode the info_hash and peer_id
        info_hash_encoded = urllib.parse.quote_from_bytes(self.info_hash)  # ensure correct encoding
        peer_id_encoded = urllib.parse.quote(self.peer_id.decode('utf-8'))

        # Build query string
        params = {
            'info_hash': info_hash_encoded,
            'peer_id': peer_id_encoded,
            'port': 6881,
            'uploaded': 0,
            'downloaded': 0,
            'left': 0,
            'event': '',
        }

        # Final tracker URL
        tracker_url = f'http://{host}:{port}{path}?{urllib.parse.urlencode(params)}'
        logging.info("Requesting URL: %s", tracker_url)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(tracker_url, timeout=5) as response:
                    if response.status == 200:
                        content = await response.read()
                        if not content:
                            logging.error("Empty response.")
                            return []

                        try:
                            tracker_response = Decoder(content).decode()
                            logging.info("Tracker Response: %s", tracker_response)

                            self._parse_peers(tracker_response)
                        except RuntimeError as e:
                            logging.error("Error decoding response: %s", e)
                            return []
                    else:
                        logging.error("Tracker returned status code %d", response.status)
                        return []
        except asyncio.TimeoutError:
            logging.error("%s timed out.", tracker_url)
            return []
        except aiohttp.ClientError as e:
            logging.error("Request failed: %s", e)
            return []

    async def _peers_from_udp(self, url):
        if self.peers_found:
            return

        logging.info("Requesting peers from: %s", url)

        try:
            data = urllib3.util.parse_url(url)
            host = data.host
            port = data.port

            if not host or not port:
                logging.warning("Invalid tracker URL: %s", url)
                return []

            loop = asyncio.get_running_loop()
            try:
                transport, protocol = await loop.create_datagram_endpoint(
                    lambda: self.UDPTrackerProtocol(),
                    remote_addr=(host, port)
                )
            except Exception as e:
                logging.error("Failed to create UDP endpoint for %s:%d – %s", host, port, e)
                return []

            try:
                # Step 1: Send connect request
                transaction_id = random.randint(0, 2 ** 32 - 1)
                conn_req = struct.pack(">QLL", 0x41727101980, 0, transaction_id)
                protocol.transport.sendto(conn_req)

                # Wait for connect response
                resp = await asyncio.wait_for(protocol.future, timeout=5)
                action, resp_tid, conn_id = struct.unpack(">LLQ", resp)

                if resp_tid != transaction_id or action != 0:
                    logging.error("Invalid connect response")
                    return []

                # Step 2: Send announce request
                transaction_id = random.randint(0, 2 ** 32 - 1)
                downloaded = 0
                left = 0
                uploaded = 0
                event = 0
                ip = 0
                key = random.randint(0, 2 ** 32 - 1)
                num_want = -1
                port = 6881

                announce_req = struct.pack(">QLL20s20sQQQLLLlh", conn_id, 1, transaction_id,
                                           self.info_hash, self.peer_id, downloaded, left, uploaded,
                                           event, ip, key, num_want, port)

                # Reset future and send announce
                protocol.future = loop.create_future()
                protocol.transport.sendto(announce_req)

                resp = await asyncio.wait_for(protocol.future, timeout=5)

                action, resp_tid, interval, leechers, seeders = struct.unpack(">LLLLL", resp[:20])
                if resp_tid != transaction_id or action != 1:
                    logging.error("Invalid announce response")
                    return []

                peers = []
                for i in range(20, len(resp), 6):
                    ip_bytes = resp[i:i + 4]
                    port_bytes = resp[i + 4:i + 6]
                    ip_addr = '.'.join(str(b) for b in ip_bytes)
                    port_num = struct.unpack(">H", port_bytes)[0]
                    peers.append((ip_addr, port_num))

                self.peers = peers
                self.peers_found = True
                logging.info("Found %d peers from %s", len(peers), url)
                return peers

            except asyncio.TimeoutError:
                logging.error("Timeout while connecting to tracker or waiting for response.")
                return []
            except Exception as e:
                logging.error("Error during tracker communication: %s", e)
                return []
            finally:
                transport.close()

        except Exception as e:
            logging.error("Failed to parse or resolve tracker URL '%s': %s", url, e)
            return []

    async def _peers_from_https(self, url):
        if self.peers_found:
            return

        logging.info("Requesting peers from: %s", url)

        # Parse the URL
        data = urllib3.util.parse_url(url)
        path = data.path
        host = data.host
        port = data.port or 443  # Default to port 443 for HTTPS

        # URL encode the info_hash and peer_id
        info_hash_encoded = urllib.parse.quote_from_bytes(self.info_hash)
        peer_id_encoded = urllib.parse.quote(self.peer_id.decode('utf-8'))

        # Construct the tracker request URL
        tracker_url = f"https://{host}{path}?info_hash={info_hash_encoded}&peer_id={peer_id_encoded}&port=6881&uploaded=0&downloaded=0&left=100000&event=started"
        logging.info("Sending request to: %s", tracker_url)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(tracker_url, timeout=5) as response:
                    if response.status == 200:
                        content = await response.read()
                        if not content:
                            logging.error("Empty response body.")
                            return []

                        try:
                            tracker_response = Decoder(content).decode()
                            logging.info("Tracker Response: %s", tracker_response)

                            self._parse_peers(tracker_response)
                        except RuntimeError as e:
                            logging.error("Error decoding response: %s", e)
                            return []
        except asyncio.TimeoutError:
            logging.error("Request to %s timed out.", url)
            return []
        except aiohttp.ClientError as e:
            logging.error("Request failed for %s: %s", url, e)
            return []

    async def _peers_from_single_url(self, url):
        if isinstance(url, bytes):
            url = url.decode('utf-8')

        if url.startswith('https://'):
            return await self._peers_from_https(url)
        elif url.startswith('udp://'):
            return await self._peers_from_udp(url)
        elif url.startswith('http://'):
            return await self._peers_from_http(url)

    async def _peers_from_multiple_urls(self, announce_list):
        tasks = []
        for tier in announce_list:
            for url in tier:
                if self.peers_found:
                    return
                tasks.append(self._peers_from_single_url(url))
        await asyncio.gather(*tasks)

    async def get(self):
        if b'announce-list' in self.torrent:
            await self._peers_from_multiple_urls(self.torrent[b'announce-list'])
        elif b'announce' in self.torrent:
            await self._peers_from_single_url(self.torrent[b'announce'].decode('utf-8'))
        else:
            client = _DHTClient(info_hash)
            self.peers = await client.peers_from_DHT()


class _DHTClient:
    BOOTSTRAP_NODES = [
        ("67.215.246.10", 6881),  # router.bittorrent.com
        ("87.98.162.88", 6881),  # dht.transmissionbt.com
        ("82.221.103.244", 6881),  # router.utorrent.com
    ]

    def __init__(self, info_hash, node_id=None):
        self.info_hash = info_hash
        self.node_id = node_id or self._generate_node_id()
        self.found_peers = set()
        self.seen_nodes = set()
        self.nodes_to_query = deque(_DHTClient.BOOTSTRAP_NODES)
        self.node_timeouts = defaultdict(lambda: 1)

    def _generate_node_id(self):
        return bytes(random.getrandbits(8) for _ in range(20))

    async def _send_message(self, addr, msg, timeout=5):
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.settimeout(timeout)  # Set a timeout for the socket
        try:
            sock.sendto(Encoder(msg).encode(), addr)
            data = await asyncio.wait_for(loop.sock_recv(sock, 4096), timeout)  # Ensure timeout
            return Decoder(data).decode()
        except asyncio.TimeoutError:
            print(f"[ERROR][{datetime.datetime.utcnow()}] Timeout while waiting for response from {addr}")
            return None
        except Exception as e:
            print(f"[ERROR][{datetime.datetime.utcnow()}] Failed to get response from {addr}: {e}")
            return None
        finally:
            sock.close()

    def _build_get_peers(self, transaction_id):
        return {
            b"t": transaction_id,
            b"y": b"q",
            b"q": b"get_peers",
            b"a": {
                b"id": self.node_id,
                b"info_hash": self.info_hash
            }
        }

    @staticmethod
    def _parse_compact_nodes(nodes_blob):
        nodes = []
        for i in range(0, len(nodes_blob), 26):
            ip = socket.inet_ntoa(nodes_blob[i + 20:i + 24])
            port = struct.unpack("!H", nodes_blob[i + 24:i + 26])[0]
            nodes.append((ip, port))
        return nodes

    @staticmethod
    def _parse_compact_peers(peers_blob):
        peers = []
        for peer in peers_blob:
            ip = socket.inet_ntoa(peer[:4])
            port = struct.unpack("!H", peer[4:6])[0]
            peers.append((ip, port))
        return peers

    async def _query_node(self, ip, port):
        if (ip, port) in self.seen_nodes:
            return
        self.seen_nodes.add((ip, port))

        tid = b"aa"
        msg = self._build_get_peers(tid)
        timeout = self.node_timeouts[(ip, port)]

        try:
            print(f"[SEND][{datetime.datetime.utcnow()}] Sending get_peers to {ip}:{port} \t -TIMEOUT- {timeout}s")
            response = await self._send_message((ip, port), msg, timeout)
            if not response:
                self.node_timeouts[(ip, port)] = min(timeout * 2, 16)
                return

            r = response.get(b"r", {})

            if b"values" in r:
                new_peers = self._parse_compact_peers(r[b"values"])
                self.found_peers.update(new_peers)

            if b"nodes" in r:
                new_nodes = self._parse_compact_nodes(r[b"nodes"])
                for node in new_nodes:
                    if node not in self.seen_nodes:
                        self.nodes_to_query.append(node)

        except asyncio.TimeoutError:
            print(f"[ERROR][{datetime.datetime.utcnow()}] {ip}:{port} timed out at {timeout}s")
            self.node_timeouts[(ip, port)] = min(timeout * 2, 16)  # Back off
        except Exception as e:
            print(f"[ERROR][{datetime.datetime.utcnow()}] Failed to query {ip}:{port}: {e}")
            self.node_timeouts[(ip, port)] = min(timeout * 2, 16)

    async def peers_from_DHT(self, max_peers=50):
        print(f"[START][{datetime.datetime.utcnow()}] Starting DHT peer discovery for info_hash {self.info_hash.hex()}")
        while self.nodes_to_query and len(self.found_peers) < max_peers:
            ip, port = self.nodes_to_query.popleft()
            await self._query_node(ip, port)
            await asyncio.sleep(0.01)  # avoid flooding
        print(f"[DONE][{datetime.datetime.utcnow()}] Found {len(self.found_peers)} peers")
        return list(self.found_peers)


class RoutingTable:
    def __init__(self, node_id, k=8):
        self.node_id = node_id
        self.k = k
        self.buckets = [deque() for _ in range(160)]  # 160 buckets

    @staticmethod
    def xor_distance(id1: int, id2: int) -> int:
        return id1 ^ id2

    def _bucket_index(self, other_node_id: bytes) -> int:
        other = int.from_bytes(other_node_id, byteorder='big')
        dist = self.xor_distance(self.node_id, other)
        if dist == 0:
            return 0
        return dist.bit_length() - 1

    def insert(self, node: Node):
        index = self._bucket_index(node.node_id)
        bucket = self.buckets[index]

        for existing in bucket:
            if existing.node_id == node.node_id:
                existing.last_seen = datetime.datetime.now()
                bucket.remove(existing)
                bucket.append(existing)
                return

        if len(bucket) < self.k:
            bucket.append(node)
        else:
            # Optional: ping the oldest node and evict if dead
            oldest = bucket[0]
            # For now, let's just evict without ping check
            bucket.popleft()
            bucket.append(node)

    def get_closest(self, target_id: bytes, count=8):
        target = int.from_bytes(target_id, byteorder='big')
        all_nodes = [node for bucket in self.buckets for node in bucket]
        all_nodes.sort(key=lambda n: self.xor_distance(int.from_bytes(n.node_id, byteorder='big'), target))
        return all_nodes[:count]


if __name__ == '__main__':
    with open('../Devil May Cry 4 - Special Edition [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        info_dict = torrent[b'info']
        # print_torrent(torrent)
        bencoded_info = Encoder(info_dict).encode()
        # print(bencoded_info)
        info_hash = hashlib.sha1(bencoded_info).digest()
        # peer = Peer()
        # print(torrent[b'announce'])
        peergetter = PeerGetter(torrent=torrent)
        asyncio.run(peergetter.get())
        logging.info("Peers: %s", peergetter.peers)
        print(f'{len(peergetter.peer_set)} Peers found')
