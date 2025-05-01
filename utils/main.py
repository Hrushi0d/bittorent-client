import asyncio
import logging
import os
import time
from datetime import datetime

from utils.Bencode import Decoder
from utils._PeerConnectionPool import PeerConnectionPool

# Create log directory and timestamped log filename
os.makedirs('../logs', exist_ok=True)
timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
log_filename = f'../logs/peergetter_{timestamp}.log'

# Configure logging
logger = logging.getLogger('PeerGetterLogger')
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


try:
    # Start time tracking
    start_time = time.time()

    # Read and decode the torrent file
    with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()

    # Create and run the PeerConnectionPool instance
    peer_connection_pool = PeerConnectionPool(torrent=torrent, logger=logger)
    asyncio.run(peer_connection_pool.run())  # Ensure async run of the pool

    # End time tracking
    end_time = time.time()

    # Calculate and log execution time
    execution_time = end_time - start_time
    logger.info(f"Execution time: {execution_time:.2f} seconds")

except Exception as e:
    logging.exception("An error occurred during peer connection.")
    print(f"An error occurred: {e}")

finally:
    print(f"Logs saved to {log_filename}")
