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
#                               `RoutingTable` class, which implements the Kademlia DHT
#                               (Distributed Hash Table) routing table logic for a BitTorrent
#                               client. The routing table is responsible for efficiently
#                               storing and retrieving information about known DHT nodes,
#                               facilitating lookups and network traversal. Maintains 160
#                               buckets (for 160-bit node IDs, as used in BitTorrent DHT),
#                               each implemented as a deque. Each bucket holds up to `k` nodes.
#
#                               Uses XOR distance to determine bucket placement and node
#                               closeness, a core principle of Kademlia.Adds new nodes
#                               to the appropriate bucket, updating last-seen time if
#                               the node already exists.
#
#                               If a bucket is full, evicts the oldest node
#                               (simplified; real implementations would ping the oldest first)

# ******************************************************** IMPORTS *****************************************************

import datetime
from collections import deque

from utils._Node import Node

# ***************************************************** ROUTING TABLE **************************************************


class RoutingTable:
    def __init__(self, node_id, k=32, filename='routing_table.pkl'):
        self.filename = filename
        self.node_id = node_id
        self.k = k
        self.buckets = [deque() for _ in range(160)]  # 160 buckets for each bit

    def __repr__(self):
        return f'RoutingTable(filename={self.filename}, k={self.k})'

    @staticmethod
    def xor_distance(id1: int, id2: int) -> int:
        return id1 ^ id2

    def _bucket_index(self, other_node_id: int) -> int:
        dist = self.xor_distance(self.node_id, other_node_id)
        if dist == 0:
            return 0
        return dist.bit_length() - 1

    def insert(self, node: Node):
        index = self._bucket_index(node.node_id)
        bucket = self.buckets[index]

        # Update existing node if found
        for existing in bucket:
            if existing.node_id == node.node_id:
                existing.last_seen = datetime.datetime.now()
                bucket.remove(existing)
                bucket.append(existing)
                return

        # Add new node if bucket isn't full
        if len(bucket) < self.k:
            bucket.append(node)
        else:
            # In a real implementation, we would ping the oldest node here For simplicity, we'll just evict the oldest
            bucket.popleft()
            bucket.append(node)

    def get_closest(self, target_id: int, count=8):
        """Returns the closest nodes to the target ID"""
        all_nodes = [node for bucket in self.buckets for node in bucket]
        all_nodes.sort(key=lambda n: self.xor_distance(n.node_id, target_id))
        return all_nodes[:count]
# ********************************************************** EOF *******************************************************
