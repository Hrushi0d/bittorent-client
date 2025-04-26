import datetime
import os
import pickle
from collections import deque


class Node:
    def __init__(self, ip, port, node_id):
        self.ip = ip
        self.port = port
        self.node_id = node_id
        self.last_seen = datetime.datetime.now()


class RoutingTable:
    def __init__(self, node_id, k=8, filename='routing_table.pkl'):
        self.filename = filename
        self.node_id = node_id
        self.k = k
        if os.path.exists(self.filename):
            self.load_from_pkl(self.filename)
        else:
            self.buckets = [deque() for _ in range(160)]  # 160 buckets for each bit
            print("No saved routing table found, creating a new one.")  # 160 buckets for each bit

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

    def save_to_pkl(self, filename=None):
        """Save the routing table to a pickle file"""
        filename = filename or self.filename
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
        print(f"Routing table saved to {filename}")

    def load_from_pkl(self, filename):
        """Load a routing table from a pickle file"""
        with open(filename, 'rb') as f:
            loaded_table = pickle.load(f)

        # Re-initialize the current instance with the loaded one
        self.node_id = loaded_table.node_id
        self.k = loaded_table.k
        self.filename = loaded_table.filename
        self.buckets = loaded_table.buckets
        print(f"Routing table loaded from {filename}")

    def __del__(self):
        """Destructor to automatically save the routing table to a pkl file when the object is destroyed."""
        self.save_to_pkl('routing_table.pkl')
        print("Routing table automatically saved to routing_table.pkl on object destruction.")