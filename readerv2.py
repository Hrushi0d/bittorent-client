import asyncio
import hashlib
import random
import socket
import string
import struct
import urllib
from urllib.parse import urlparse

import requests
import urllib3.util

from utils.Bencode import Encoder, Decoder, print_torrent


class Peer(object):
    def __init__(self, host, port, file_name, info_hash, PEER_ID=None):
        self.writer = None
        self.reader = None
        self.host = host
        self.port = port
        self.file_name = file_name
        self.info_hash = info_hash
        self.peer_choking = True
        self.peer_interested = False

        if PEER_ID is None:
            suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
            self.PEER_ID = f'-PC0001-{suffix}'.encode('utf-8')  # peer_id must be bytes
        else:
            self.PEER_ID = PEER_ID

    async def connect(self):
        reader, writer = await asyncio.open_connection(
            host=self.host,
            port=self.port
        )

        self.reader = reader
        self.writer = writer

        # Build handshake
        pstr = b'BitTorrent protocol'
        pstrlen = bytes([len(pstr)])  # 1 byte length
        reserved = b'\x00' * 8  # 8 reserved bytes

        handshake = b''.join([
            pstrlen,
            pstr,
            reserved,
            self.info_hash,
            self.PEER_ID
        ])

        writer.write(handshake)
        await writer.drain()

        # read the handshake response from peer
        peer_handshake = await reader.read(68)
        print("Handshake response:", peer_handshake)

        # Parse response
        resp_pstrlen = peer_handshake[0]
        resp_pstr = peer_handshake[1:20]
        resp_reserved = peer_handshake[20:28]
        resp_info_hash = peer_handshake[28:48]
        resp_peer_id = peer_handshake[48:68]

        if resp_info_hash != self.info_hash:
            print("⚠️ Mismatched info_hash! This peer is not for our torrent.")
            writer.close()
            await writer.wait_closed()
            return

        print("✅ Connected to peer:", self.host)
        print("Peer ID:", resp_peer_id)


class PeerGetter(object):

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
        print("Info hash:", self.info_hash)
        print("Info hash length:", len(self.info_hash))  # Must be 20

    def _parse_compact_format(self, tracker_response):
        if b'peers' in tracker_response:
            peers_data = tracker_response[b'peers']

            # Ensure peers_data is of type 'bytes'
            if not isinstance(peers_data, bytes):
                print("❌ Invalid 'peers' data format, expected bytes.")
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

            self.peers_found = True  # Set the flag to indicate peers were found
            print(f"✅ Successfully parsed {len(self.peers)} peers.")
            return self.peers

        else:
            print("❌ No peers found in response.")
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
                else:
                    print(f"❌ Invalid peer data: {peer}")

            self.peers_found = True  # Set the flag to indicate peers were found
            print(f"✅ Successfully parsed {len(self.peers)} verbose peers.")
            return self.peers
        else:
            print("❌ Invalid verbose peer data format.")
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

    def _peers_from_http(self, url):
        if self.peers_found:
            return
        # Parse URL
        data = urllib3.util.parse_url(url)
        path = data.path
        host = data.host
        port = data.port

        # URL encode the info_hash and peer_id
        info_hash_encoded = urllib.parse.quote(self.info_hash)  # Converting to hex string first
        peer_id_encoded = urllib.parse.quote(self.peer_id.decode('utf-8'))

        params = {
            'info_hash': info_hash_encoded,
            'peer_id': peer_id_encoded,
            'port': 6881,  # Port number for the peer
            'uploaded': 0,  # The number of bytes uploaded
            'downloaded': 0,  # The number of bytes downloaded
            'left': 0,  # The number of bytes left to download
            'event': '',  # Event parameter (usually left empty or set to 'completed' / 'started' / 'stopped')
        }

        url = f'http://{host}{path}?{urllib.parse.urlencode(params)}'
        print(f"Requesting URL: {url}")

        try:
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                try:
                    # Decode the Bencoded response
                    tracker_response = Decoder(response.content).decode()
                    print("Tracker Response:", tracker_response)

                    self._parse_peers(tracker_response)
                except RuntimeError as e:
                    print(f"❌ Error decoding response: {e}")
                    return []
            else:
                print(f"❌ Tracker returned status code {response.status_code}")
                return []
        except requests.exceptions.Timeout:
            print(f"❌ {url} timed out.")
            return []
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return []

    def _peers_from_udp(self, url):  # udp not implemented yet
        if self.peers_found:
            return

        print(f"Requesting peers from: {url}")
        # Parse URL
        data = urllib3.util.parse_url(url)
        path = data.path
        host = data.host
        port = data.port
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)  # Increase timeout to 120 seconds

        transaction_id = random.randint(0, 2 ** 32 - 1)
        conn_req = struct.pack(">QLL", 0x41727101980, 0, transaction_id)

        try:
            sock.sendto(conn_req, (host, port))
            resp, _ = sock.recvfrom(1024)
        except socket.timeout:
            print("❌ Timeout while connecting to tracker.")
            return []
        except Exception as e:  # Capture the exception and print it
            print(f"❌ Exception occurred: {e}")
            return []

        try:
            action, resp_tid, conn_id = struct.unpack(">LLQ", resp)
        except struct.error:
            print("❌ Error unpacking connect response.")
            return []

        if resp_tid != transaction_id or action != 0:
            print("❌ Invalid connect response")
            return []

        # Now send announce request
        transaction_id = random.randint(0, 2 ** 32 - 1)
        downloaded = 0
        left = 0
        uploaded = 0
        event = 0  # none
        ip = 0
        key = random.randint(0, 2 ** 32 - 1)
        num_want = -1
        port = 6881

        announce_req = struct.pack(">QLL20s20sQQQLLLlh", conn_id, 1, transaction_id,
                                   self.info_hash, self.peer_id, downloaded, left, uploaded,
                                   event, ip, key, num_want, port)

        try:
            sock.sendto(announce_req, (host, port))
            resp, _ = sock.recvfrom(4096)
        except socket.timeout:
            print("❌ Timeout while waiting for peers list.")
            return []

        try:
            action, resp_tid, interval, leechers, seeders = struct.unpack(">LLLLL", resp[:20])
        except struct.error:
            print("❌ Error unpacking announce response.")
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
        return peers

    def _peers_from_https(self, url):
        if self.peers_found:
            return

        print(f"Requesting peers from: {url}")

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
        print(f"Sending request to: {tracker_url}")

        try:
            # Send the request to the tracker
            response = requests.get(tracker_url, timeout=5)
            if response.status_code == 200:
                try:
                    if not response.content:
                        print("❌ Empty response body.")
                        return []

                    # Decode the Bencoded response
                    tracker_response = Decoder(response.content).decode()
                    print("Tracker Response:", tracker_response)

                    self._parse_peers(tracker_response)

                except RuntimeError as e:
                    print(f"❌ Error decoding response: {e}")
                    return []
            else:
                print(f"❌ Tracker returned status code {response.status_code}")
                return []
        except requests.exceptions.Timeout:
            print(f"❌ Request to {url} timed out.")
            return []
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed for {url}: {e}")
            return []

    def _peers_from_single_url(self, url):

        if isinstance(url, bytes):
            url = url.decode('utf-8')
        if url.startswith('https://'):
            return self._peers_from_https(url)
        elif url.startswith('udp://'):
            return self._peers_from_udp(url)
        elif url.startswith('http://'):
            return self._peers_from_http(url)

    def _peers_from_multiple_urls(self, announce_list):
        for tier in announce_list:
            for url in tier:
                if self.peers_found:
                    return
                self._peers_from_single_url(url)

    def get(self):
        if b'announce-list' in self.torrent.keys():
            return self._peers_from_multiple_urls(self.torrent[b'announce-list'])
        elif b'announce' in self.torrent.keys():
            # If no announce-list, try single announce
            return self._peers_from_single_url(self.torrent[b'announce'].decode('utf-8'))
        else:
            print("No valid announce URL found in the torrent.")
            return []



if __name__ == '__main__':
    with open('Devil May Cry 4 - Special Edition [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        info_dict = torrent[b'info']
        print_torrent(torrent)
        bencoded_info = Encoder(info_dict).encode()
        # # print(bencoded_info)
        info_hash = hashlib.sha1(bencoded_info).digest()
        # # peer = Peer()
        # # print(torrent[b'announce'])
        peergetter = PeerGetter(torrent=torrent)
        peergetter.get()
        print(peergetter.peers)
