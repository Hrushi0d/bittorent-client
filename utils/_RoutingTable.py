import datetime
from collections import deque

from utils._Node import Node


class RoutingTable:
    def __init__(self, node_id, k=8, filename='routing_table.pkl'):
        self.filename = filename
        self.node_id = node_id
        self.k = k
        self.buckets = [deque() for _ in range(160)]  # 160 buckets for each bit

    def __repr__(self):
        return f'RoutingTable(filename={self.filename}, k={8})'

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
