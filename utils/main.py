import asyncio
import logging
import os
import time
from datetime import datetime

from utils.Bencode import Decoder
from utils._DownloadManager import DownloadManager
from utils._PeerConnectionPool import PeerConnectionPool
from utils._Piece_Manager import PieceManager

# Ensure logs directory exists
os.makedirs('../logs', exist_ok=True)

# Generate timestamped log filename
timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
log_filename = f'../logs/downloader_{timestamp}.log'

# Configure logging
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


mode = ['rarest-first', 'random-rarest-first', 'sequential']
selected_mode = mode[0]  # Change index to select other modes
logger = logging.getLogger(__name__)

try:
    logger.info("Starting PeerGetter BitTorrent client...")
    start_time = time.time()

    with open('../Devil May Cry 4 - Special Edition [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        logger.info("Successfully decoded .torrent file.")

    peer_connection_pool = PeerConnectionPool(torrent=torrent, logger=logger)
    piece_dict = asyncio.run(peer_connection_pool.run())
    logger.info("Peer discovery completed.")

    piece_manager = PieceManager(piece_dict, torrent=torrent, mode=selected_mode, logger=logger)
    pieces = piece_manager.run()
    logger.info(f"{len(pieces)} pieces loaded using strategy: {selected_mode}")

    download_manager = DownloadManager(pieces=pieces, piece_dict=piece_dict)
    logger.info("DownloadManager initialized.")

    # add downloading here
    # asyncio.run(download_manager.start())

    end_time = time.time()
    logger.info(f"Execution completed in {end_time - start_time:.2f} seconds")

except Exception as e:
    logger.exception("An error occurred during execution.")
    print(f"An error occurred: {e}")

finally:
    print(f"Logs saved to {log_filename}")
