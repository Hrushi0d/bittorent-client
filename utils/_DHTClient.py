import asyncio
import datetime
import os
import pickle
import random
import socket
import struct
from collections import defaultdict

from utils.Bencode import Encoder, Decoder
from utils._RoutingTable import RoutingTable
from utils._Node import Node


class _DHTClient:
    BOOTSTRAP_NODES = [
        ("67.215.246.10", 6881),  # router.bittorrent.com
        ("87.98.162.88", 6881),  # dht.transmissionbt.com
        ("82.221.103.244", 6881),  # router.utorrent.com
    ]

    def __init__(self, info_hash, logger, node_id=None):
        self.info_hash = info_hash
        self.logger = logger
        self.node_id = node_id or self._generate_node_id()
        self.found_peers = set()
        self.pickle_file = 'routing_table.pkl'
        self.routing_table = self.load_routing_table(self.pickle_file)
        self.node_timeouts = defaultdict(lambda: 1)
        self._initialize_bootstrap_nodes()

    def load_routing_table(self, filename):
        """Load routing table from a pickle file, or create a new one if not found."""
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                return pickle.load(f)
        else:
            # Create a new routing table if no pickle file exists
            self.logger.info("No saved routing table found, creating a new one.")
            return RoutingTable(int.from_bytes(self.node_id, byteorder='big'))

    def save_routing_table(self):
        """Save routing table to a pickle file."""
        with open(self.pickle_file, 'wb') as f:
            pickle.dump(self.routing_table, f)
        self.logger.info(f"Routing table saved to {self.pickle_file}")

    def __del__(self):
        """Save routing table automatically when the object is deleted."""
        self.save_routing_table()

    def _initialize_bootstrap_nodes(self):
        for ip, port in self.BOOTSTRAP_NODES:
            # Create dummy node IDs for bootstrap nodes (in reality we don't know their IDs yet)
            dummy_id = int.from_bytes(self._generate_node_id(), byteorder='big')
            node = Node(ip, port, dummy_id)
            self.routing_table.insert(node)

    def _generate_node_id(self):
        return bytes(random.getrandbits(8) for _ in range(20))

    async def _send_message(self, addr, msg, timeout=5):
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.settimeout(timeout)
        try:
            sock.sendto(Encoder(msg).encode(), addr)
            data = await asyncio.wait_for(loop.sock_recv(sock, 4096), timeout)
            return Decoder(data).decode()
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout while waiting for response from {addr}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get response from {addr}: {e}")
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
            node_id = nodes_blob[i:i + 20]
            ip = socket.inet_ntoa(nodes_blob[i + 20:i + 24])
            port = struct.unpack("!H", nodes_blob[i + 24:i + 26])[0]
            nodes.append((ip, port, node_id))
        return nodes

    @staticmethod
    def _parse_compact_peers(peers_blob):
        peers = []
        for peer in peers_blob:
            ip = socket.inet_ntoa(peer[:4])
            port = struct.unpack("!H", peer[4:6])[0]
            peers.append((ip, port))
        return peers

    async def _query_node(self, node):
        timeout = self.node_timeouts[(node.ip, node.port)]
        tid = b"aa"
        msg = self._build_get_peers(tid)

        try:
            self.logger.info(f"Sending get_peers to {node.ip}:{node.port} with timeout {timeout}s")
            response = await self._send_message((node.ip, node.port), msg, timeout)
            if not response:
                self.node_timeouts[(node.ip, node.port)] = min(timeout * 2, 16)
                return

            r = response.get(b"r", {})

            # Update the node's ID if we got a response (since bootstrap nodes have dummy IDs)
            if b"id" in r:
                node.node_id = int.from_bytes(r[b"id"], byteorder='big')
                self.routing_table.insert(node)  # Re-insert with correct ID

            if b"values" in r:
                new_peers = self._parse_compact_peers(r[b"values"])
                self.found_peers.update(new_peers)

            if b"nodes" in r:
                new_nodes = self._parse_compact_nodes(r[b"nodes"])
                for ip, port, node_id in new_nodes:
                    node_id_int = int.from_bytes(node_id, byteorder='big')
                    new_node = Node(ip, port, node_id_int)
                    self.routing_table.insert(new_node)

        except asyncio.TimeoutError:
            self.logger.error(f"{node.ip}:{node.port} timed out at {timeout}s")
            self.node_timeouts[(node.ip, node.port)] = min(timeout * 2, 16)
        except Exception as e:
            self.logger.error(f"Failed to query {node.ip}:{node.port}: {e}")
            self.node_timeouts[(node.ip, node.port)] = min(timeout * 2, 16)

    async def peers_from_DHT(self, max_peers=50):
        self.logger.info(f"Starting DHT peer discovery for info_hash {self.info_hash.hex()}")

        while len(self.found_peers) < max_peers:
            closest_nodes = self.routing_table.get_closest(int.from_bytes(self.info_hash, byteorder='big'))

            if not closest_nodes:
                self.logger.warning("No more nodes to query in routing table")
                break

            # Query all closest nodes in parallel
            tasks = [self._query_node(node) for node in closest_nodes]
            await asyncio.gather(*tasks)

            # Small delay to prevent flooding
            await asyncio.sleep(0.1)

        self.logger.info(f"Found {len(self.found_peers)} peers")
        return list(self.found_peers)