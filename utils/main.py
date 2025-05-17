import asyncio
import logging
import os
import time
from datetime import datetime
from yaspin import yaspin
from yaspin.spinners import Spinners

from utils._Bencode import Decoder
from utils._DownloadManager import DownloadManager
from utils._PeerConnectionPool import PeerConnectionPool
from utils._Piece_Manager import PieceManager

os.makedirs('../logs', exist_ok=True)

timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
log_filename = f'../logs/downloader_{timestamp}.log'

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

    # === Step 1: Torrent Decoding ===
    with yaspin(Spinners.line, text="Reading and decoding torrent file...") as spinner:
        t0 = time.time()
        with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
            meta_info = f.read()
            torrent = Decoder(meta_info).decode()
        duration = time.time() - t0
        logger.info(f"Decoded torrent in {duration:.2f} seconds.")
        spinner.ok("✔")
        spinner.write(f"✓ Decoded .torrent file in {duration:.2f} seconds")

    # === Step 2: Peer Discovery ===
    with yaspin(Spinners.line, text="Running peer discovery...") as spinner:
        t0 = time.time()
        peer_connection_pool = PeerConnectionPool(torrent=torrent, logger=logger)
        piece_dict = asyncio.run(peer_connection_pool.run())
        duration = time.time() - t0
        logger.info(f"Peer discovery completed in {duration:.2f} seconds.")
        spinner.ok("✔")
        spinner.write(f"✓ Peer discovery done in {duration:.2f} seconds")

    # === Step 3: Piece Manager ===
    with yaspin(Spinners.line, text="Preparing piece manager...") as spinner:
        t0 = time.time()
        spinner.write(f"✓ Strategy used: {selected_mode}")
        piece_manager = PieceManager(piece_dict, torrent=torrent, mode=selected_mode, logger=logger)
        pieces = piece_manager.run()
        duration = time.time() - t0
        logger.info(f"Loaded {len(pieces)} pieces in {duration:.2f} seconds using {selected_mode} strategy.")
        spinner.ok("✔")
        spinner.write(f"✓ Loaded {len(pieces)} pieces in {duration:.2f} seconds")
        for i, piece in enumerate(pieces[:5]):
            spinner.write(f"Piece {i}: {piece}")

    # === Step 4: Download Manager Init ===
    with yaspin(Spinners.line, text="Initializing download manager...") as spinner:
        t0 = time.time()
        download_manager = DownloadManager(pieces=pieces, piece_dict=piece_dict, logger=logger)
        duration = time.time() - t0
        logger.info(f"DownloadManager initialized in {duration:.2f} seconds.")
        spinner.ok("✔")
        spinner.write(f"✓ Download manager ready in {duration:.2f} seconds")

    # === Optional: Download Start ===
    # with yaspin(Spinners.line, text="Starting download...") as spinner:
    #     t0 = time.time()
    #     asyncio.run(download_manager.start())
    #     duration = time.time() - t0
    #     logger.info(f"Download started and ran for {duration:.2f} seconds.")
    #     spinner.ok("✔")
    #     spinner.write(f"✓ Download completed in {duration:.2f} seconds")

    total_time = time.time() - start_time
    logger.info(f"Execution completed in {total_time:.2f} seconds")
    print(f"🎯 Execution completed in {total_time:.2f} seconds")

except Exception as e:
    logger.exception("An error occurred during execution.")
    print(f"❌ An error occurred: {e}")

finally:
    print(f"📝 Logs saved to {log_filename}")