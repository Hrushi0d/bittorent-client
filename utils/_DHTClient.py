import asyncio
import datetime
import logging
import os
import pickle
import random
import socket
import struct
import time
from collections import defaultdict, OrderedDict
from typing import List, Dict, Set, Tuple, Optional, Any

from utils._Bencode import Encoder, Decoder
from utils._RoutingTable import RoutingTable
from utils._Node import Node


class _DHTClient:
    BOOTSTRAP_NODES = [
        ("67.215.246.10", 6881),  # router.bittorrent.com
        ("87.98.162.88", 6881),  # dht.transmissionbt.com
        ("82.221.103.244", 6881),  # router.utorrent.com
        ("212.129.33.50", 6881),  # dht.aelitis.com
        ("182.140.167.7", 6881),  # router.bitcomet.com
        ("67.215.246.12", 6881),  # dht.aelitis.com (alt)
        ("185.61.149.45", 6881),  # router.silotv.com
        ("107.22.210.36", 6881),  # router.bittorrent.org
    ]

    def __init__(self, info_hash, logger: logging.Logger, node_id=None, max_nodes_per_request=8, socket_timeout=5):
        self.info_hash = info_hash
        self.logger = logger
        self.node_id = node_id or self._generate_node_id()
        self.found_peers = set()
        self.pickle_file = 'routing_table.pkl'
        self.routing_table = self.load_routing_table(self.pickle_file)
        self.node_timeouts = defaultdict(lambda: 1)
        self.responsive_nodes = OrderedDict()  # Track nodes that responded recently
        self.max_nodes_per_request = max_nodes_per_request
        self.socket_timeout = socket_timeout
        self.socket_pool = {}  # Reuse sockets for performance
        self._initialize_bootstrap_nodes()

    def __repr__(self):
        return f'DHTClient(node_id={self.node_id.hex() if isinstance(self.node_id, bytes) else self.node_id})'

    def load_routing_table(self, filename):
        """Load routing table from a pickle file, or create a new one if not found."""
        try:
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                with open(filename, 'rb') as f:
                    routing_table = pickle.load(f)
                    self.logger.info(
                        f"DHTClient - Loaded routing table with {sum(len(bucket) for bucket in routing_table.buckets)} nodes")
                    return routing_table
        except (pickle.PickleError, EOFError, AttributeError) as e:
            self.logger.warning(f"DHTClient - Error loading routing table: {e}. Creating new one.")

        # Create a new routing table if no pickle file exists or error occurred
        self.logger.info("DHTClient - Creating a new routing table.")
        return RoutingTable(int.from_bytes(self.node_id, byteorder='big'))

    def save_routing_table(self):
        """Save routing table to a pickle file."""
        try:
            with open(self.pickle_file, 'wb') as f:
                pickle.dump(self.routing_table, f)
            self.logger.info(
                f"DHTClient - Routing table with {sum(len(bucket) for bucket in self.routing_table.buckets)} nodes saved to {self.pickle_file}")
        except Exception as e:
            self.logger.error(f"DHTClient - Failed to save routing table: {e}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.save_routing_table()
        # Close any open sockets
        for sock in self.socket_pool.values():
            sock.close()

    def _initialize_bootstrap_nodes(self):
        """Add bootstrap nodes to the routing table"""
        for ip, port in self.BOOTSTRAP_NODES:
            # Create dummy node IDs for bootstrap nodes
            dummy_id = int.from_bytes(self._generate_node_id(), byteorder='big')
            node = Node(ip, port, dummy_id)
            self.routing_table.insert(node)

    def _generate_node_id(self):
        """Generate a random node ID"""
        return bytes(random.getrandbits(8) for _ in range(20))

    def _get_socket(self, addr):
        """Get a socket from the pool or create a new one"""
        if addr not in self.socket_pool:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            self.socket_pool[addr] = sock
        return self.socket_pool[addr]

    async def _send_message(self, addr, msg, timeout=None):
        """Send a DHT message and wait for response with timeout"""
        if timeout is None:
            timeout = self.socket_timeout

        loop = asyncio.get_event_loop()
        sock = self._get_socket(addr)

        try:
            encoded_msg = Encoder(msg).encode()
            sock.sendto(encoded_msg, addr)

            # Wait for response with timeout
            start_time = time.time()
            data = await asyncio.wait_for(loop.sock_recv(sock, 4096), timeout)

            # Record response time for adaptive timeouts
            response_time = time.time() - start_time
            self.node_timeouts[addr] = response_time * 1.2  # Add a small buffer

            # Track this as a responsive node (with timestamp)
            addr_key = f"{addr[0]}:{addr[1]}"
            self.responsive_nodes[addr_key] = time.time()

            # Keep responsive_nodes dict from growing too large
            if len(self.responsive_nodes) > 100:
                # Remove oldest entries
                while len(self.responsive_nodes) > 50:
                    self.responsive_nodes.popitem(last=False)

            try:
                return Decoder(data).decode()
            except Exception as e:
                self.logger.error(f"DHTClient - Failed to decode response from {addr}: {e}")
                return None

        except asyncio.TimeoutError:
            self.logger.error(f"DHTClient - {addr[0]}:{addr[1]} timed out at {timeout}s")
            # Exponential backoff for timeouts
            self.node_timeouts[addr] = min(timeout * 2, 16)
            return None
        except Exception as e:
            self.logger.error(f"DHTClient - Failed to query {addr[0]}:{addr[1]}: {e}")
            self.node_timeouts[addr] = min(timeout * 2, 16)
            return None

    def _build_get_peers(self, transaction_id):
        """Build a get_peers query message"""
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
        """Parse compact node information"""
        if not nodes_blob or len(nodes_blob) % 26 != 0:
            return []

        nodes = []
        for i in range(0, len(nodes_blob), 26):
            if i + 26 <= len(nodes_blob):  # Ensure we have complete node data
                node_id = nodes_blob[i:i + 20]
                ip = socket.inet_ntoa(nodes_blob[i + 20:i + 24])
                port = struct.unpack("!H", nodes_blob[i + 24:i + 26])[0]
                nodes.append((ip, port, node_id))
        return nodes

    @staticmethod
    def _parse_compact_peers(peers_blob):
        """Parse compact peer information"""
        if not peers_blob:
            return []

        peers = []
        for peer in peers_blob:
            if len(peer) >= 6:  # Ensure we have complete peer data
                ip = socket.inet_ntoa(peer[:4])
                port = struct.unpack("!H", peer[4:6])[0]
                if port > 0:  # Skip invalid ports
                    peers.append((ip, port))
        return peers

    async def _query_node(self, node):
        """Query a DHT node for peers and new nodes"""
        # Use an adaptive timeout based on past performance
        addr = (node.ip, node.port)
        timeout = self.node_timeouts.get(addr, self.socket_timeout)

        tid = os.urandom(2)  # More random transaction ID
        msg = self._build_get_peers(tid)

        try:
            self.logger.info(f"DHTClient - Sending get_peers to {node.ip}:{node.port} with timeout {timeout}s")
            response = await self._send_message(addr, msg, timeout)

            if not response:
                return

            r = response.get(b"r", {})

            # Update the node's ID if we got a response
            if b"id" in r:
                node.node_id = int.from_bytes(r[b"id"], byteorder='big')
                self.routing_table.insert(node)  # Re-insert with correct ID
                node.last_seen = datetime.datetime.now()  # Update timestamp

            # Process peers if we got them
            if b"values" in r:
                new_peers = self._parse_compact_peers(r[b"values"])
                self.found_peers.update(new_peers)
                self.logger.info(f"DHTClient - Found {len(new_peers)} new peers from {node.ip}:{node.port}")

            # Process new nodes to query
            if b"nodes" in r:
                new_nodes = self._parse_compact_nodes(r[b"nodes"])
                for ip, port, node_id in new_nodes:
                    # Skip obviously invalid nodes
                    if port <= 0 or port > 65535:
                        continue

                    node_id_int = int.from_bytes(node_id, byteorder='big')
                    new_node = Node(ip, port, node_id_int)
                    self.routing_table.insert(new_node)

        except Exception as e:
            self.logger.error(f"DHTClient - Error in _query_node for {node.ip}:{node.port}: {e}")

    def _get_nodes_to_query(self, max_nodes):
        """Get nodes to query, prioritizing responsive and closest nodes"""
        # First, get nodes close to the target info_hash
        target_id = int.from_bytes(self.info_hash, byteorder='big')
        closest_nodes = self.routing_table.get_closest(target_id, count=max_nodes * 2)

        # Sort nodes by a combination of:
        # 1. Recent responsiveness (if available)
        # 2. Distance to the target
        prioritized_nodes = []
        for node in closest_nodes:
            addr_key = f"{node.ip}:{node.port}"
            responsiveness = self.responsive_nodes.get(addr_key, 0)
            distance = self.routing_table.xor_distance(node.node_id, target_id)

            # Compute a priority score (lower is better)
            # Recently responsive nodes get a big boost
            if responsiveness > time.time() - 300:  # Response in last 5 minutes
                priority = distance / 10000  # Significant boost
            else:
                priority = distance

            prioritized_nodes.append((priority, node))

        # Sort by priority and return the best nodes
        prioritized_nodes.sort(key=lambda x: x[0])
        return [node for _, node in prioritized_nodes[:max_nodes]]

    async def peers_from_DHT(self, max_peers=50, max_queries=100):
        """Find peers using the DHT network"""
        self.logger.info(f"DHTClient - Starting DHT peer discovery for info_hash {self.info_hash.hex()}")
        queries_performed = 0
        start_time = time.time()

        while len(self.found_peers) < max_peers and queries_performed < max_queries:
            # Get the best nodes to query based on responsiveness and distance
            nodes_to_query = self._get_nodes_to_query(self.max_nodes_per_request)

            if not nodes_to_query:
                self.logger.warning("DHTClient - No more nodes to query in routing table")
                break

            # Query all selected nodes in parallel
            tasks = [self._query_node(node) for node in nodes_to_query]
            await asyncio.gather(*tasks)
            queries_performed += len(tasks)

            # Dynamic delay based on how many peers we've found
            if len(self.found_peers) > max_peers // 2:
                # We're finding peers well, can slow down a bit
                await asyncio.sleep(0.2)
            else:
                # Still need more peers, query more aggressively
                await asyncio.sleep(0.05)

            # Check if we're taking too long
            elapsed = time.time() - start_time
            if elapsed > 30 and len(self.found_peers) > 0:
                self.logger.info(
                    f"DHTClient - DHT search taking too long ({elapsed:.1f}s), returning with {len(self.found_peers)} peers")
                break

        self.logger.info(f"DHTClient - DHT search complete. Found {len(self.found_peers)} peers in {queries_performed} queries")
        return list(self.found_peers)[:max_peers]  # Return only up to max_peers