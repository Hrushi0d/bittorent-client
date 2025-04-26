from datetime import datetime


class Node:
    def __init__(self, ip, port, node_id):
        self.ip = ip
        self.port = port
        self.node_id = node_id
        self.last_seen = datetime.now()