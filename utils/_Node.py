from datetime import datetime


class Node:
    def __init__(self, ip, port, node_id):
        self.ip = ip
        self.port = port
        self.node_id = node_id
        self.last_seen = datetime.now()

    def __repr__(self):
        return f'Node(node_id={self.node_id}, ip={self.ip}, port={self.port}, last_seen={self.last_seen})'
