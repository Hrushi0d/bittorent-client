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
#                               `Node` class, a lightweight representation of a node used
#                               in the utils._DHTClient. Each node represents a participant
#                               in the DHT network, identified by its IP address, port,
#                               and unique node ID.

# ***************************************************** IMPORTS *******************************************************

from datetime import datetime

# ******************************************************* NODE *********************************************************

class Node:
    def __init__(self, ip, port, node_id):
        self.ip = ip
        self.port = port
        self.node_id = node_id
        self.last_seen = datetime.now()

    def __repr__(self):
        return f'Node(node_id={self.node_id}, ip={self.ip}, port={self.port}, last_seen={self.last_seen})'
# ******************************************************* EOF *********************************************************
