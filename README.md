# BitTorrent Client

A fully asynchronous BitTorrent client written in Python, featuring robust peer discovery, strategic piece downloading, and efficient file management. This client is designed for learning, experimentation, and extension, providing a clean, modular codebase.

---

## Features

- **Custom BEncoding/BDecoding**  
  Implements its own BEncoding and BDecoding classes for parsing `.torrent` files and tracker responses, fully compliant with the BitTorrent protocol.

- **Comprehensive Peer Discovery**  
  - **DHT (Distributed Hash Table):** Peer discovery via the global BitTorrent DHT network, supporting decentralized downloads.
  - **Tracker Parsing:** Parses and connects to trackers listed in the `.torrent` file, with support for:
    - **HTTP/HTTPS Trackers**  
    - **UDP Trackers**

- **Strategic Piece Ordering**  
  - Implements multiple algorithms for selecting which pieces to download:
    - Rarest-first
    - Random rarest-first
    - Sequential
  - Helps maximize swarm efficiency and download reliability.

- **Asynchronous Downloader & Writer**  
  - Entire download pipeline is fully async using Python's `asyncio`.
  - Pieces are downloaded and written to disk concurrently for maximum throughput and responsiveness.

- **Python-Only Implementation**  
  All logic, networking, and file handling is implemented in Python (100%), with no reliance on third-party BitTorrent libraries.

---

## Project Structure

- `utils/_Bencode.py`: Custom BEncoding/BDecoding logic
- `utils/_DHTClient.py`: DHT peer discovery and communication
- `utils/_Piece.py`, `utils/_PieceManager.py`: Piece representation and selection strategies
- `utils/_DownloadQueue.py`, `utils/_AsyncQueue.py`: Async download queue management
- `utils/_FileManager.py`: Asynchronous file writer
- `utils/_PeerGetter.py`: Peer acquisition from trackers and DHT
- `utils/_Peer.py`: Peer connection and piece download logic
- `utils/_TrackerCache.py`, `utils/_RedisClient.py`: Peer caching infrastructure (optional)
- `utils/_Node.py`, `utils/_RoutingTable.py`: DHT node and routing table logic

---

## Usage

1. **Install dependencies**  
   ```sh
   pip install -r requirements.txt
   ```

2. **Run the client**  
   Ensure you have a valid `.torrent` file. Example entrypoint:
   ```sh
   python cli.py -f path/to/file.torrent -m rarest-first
   ```

3. **Configuration**  
   - Logging, download directories, and piece selection strategy can be configured in the code or via command-line arguments (see `main.py`).

---

## Notes

- **Learning Purpose**: This client is optimized for clarity and extensibility, not raw performance.
- **No GUI**: This is a command-line tool. You can extend it for GUI or web use as needed.
- **Redis**: Peer/Tracker caching is optional, but if used, requires a Redis server.

---

## License

MIT License

---

## Acknowledgments

- Inspired by the original BitTorrent protocol specification and open-source client implementations.
- Developed for educational purposes.

---