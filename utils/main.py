import asyncio
import logging
import os
import time
from datetime import datetime
from yaspin import yaspin
from yaspin.spinners import Spinners

from utils._AsyncQueue import AsyncQueue
from utils._Bencode import Decoder
from utils._DownloadManager import DownloadManager
from utils._FileManager import FileManager
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

async def async_main():
    try:
        logger.info("Main - Starting PeerGetter BitTorrent client...")
        start_time = time.time()

        # === Step 1: Decode Torrent File ===
        with yaspin(Spinners.line, text="Reading and decoding torrent file...") as spinner:
            torrent, decode_time = None, None
            try:
                t0 = time.time()
                with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
                    meta_info = f.read()
                    torrent = Decoder(meta_info).decode()
                decode_time = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✔ Decoded torrent in {decode_time:.2f} seconds")
                logger.info(f"Main - Decoded torrent in {decode_time:.2f} seconds.")
            except Exception as e:
                spinner.fail("💥")
                spinner.write("✖ Failed to decode .torrent file")
                logger.exception("Main - Failed to decode .torrent file")
                raise e

        # === Step 2: Peer Discovery ===
        with yaspin(Spinners.line, text="Running peer discovery...") as spinner:
            piece_dict = None
            try:
                t0 = time.time()
                peer_connection_pool = PeerConnectionPool(torrent=torrent, logger=logger)
                piece_dict = await peer_connection_pool.run()
                if piece_dict is False:
                    spinner.fail("💥")
                    spinner.write("✖ No pieces found. Exiting.")
                    logger.error("Main - Peer discovery returned False. Exiting.")
                    exit(1)
                elapsed = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✓ Peer discovery completed in {elapsed:.2f} seconds")
                logger.info(f"Main - Peer discovery completed in {elapsed:.2f} seconds.")
            except Exception as e:
                spinner.fail("💥")
                spinner.write("✖ Peer discovery failed")
                logger.exception("Main - Peer discovery failed.")
                raise e

        # === Step 3: Piece Selection ===
        with yaspin(Spinners.line, text="Selecting pieces...") as spinner:
            pieces = []
            try:
                spinner.write(f"✓ Strategy used: {selected_mode}")
                t0 = time.time()
                piece_manager = PieceManager(piece_dict, torrent=torrent, mode=selected_mode, logger=logger)
                pieces = piece_manager.run()
                elapsed = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✓ Selected {len(pieces)} pieces in {elapsed:.2f} seconds")
                logger.info(f"Main - Loaded {len(pieces)} pieces in {elapsed:.2f} seconds using {selected_mode} strategy.")
            except Exception as e:
                spinner.fail("💥")
                spinner.write("✖ Piece selection failed")
                logger.exception("Main - Piece selection failed.")
                raise e

        # === Step 4: Initialize Managers ===
        async_queue = AsyncQueue(logger=logger)
        file_manager = FileManager(torrent=torrent, logger=logger, async_queue=async_queue, download_dir='../')

        with yaspin(Spinners.line, text="Initializing download manager...") as spinner:
            try:
                t0 = time.time()
                download_manager = await DownloadManager.create(
                    pieces=pieces,
                    piece_dict=piece_dict,
                    logger=logger,
                    async_queue=async_queue
                )
                elapsed = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✓ Download manager ready in {elapsed:.2f} seconds")
                logger.info(f"Main - DownloadManager initialized in {elapsed:.2f} seconds.")
            except Exception as e:
                spinner.fail("💥")
                spinner.write("✖ Failed to initialize download manager")
                logger.exception("Main - DownloadManager initialization failed.")
                raise e

        # === Step 5: Start Download ===
        with yaspin(Spinners.line, text="Starting download...") as spinner:
            try:
                t0 = time.time()

                downloader = asyncio.create_task(download_manager.start())
                writer = asyncio.create_task(file_manager.start_writer())

                await asyncio.gather(downloader, writer)

                elapsed = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✓ Download completed in {elapsed:.2f} seconds")
                logger.info(f"Main - Download ran for {elapsed:.2f} seconds.")
            except asyncio.CancelledError:
                logger.warning("Main - Tasks were cancelled.")
                downloader.cancel()
                writer.cancel()
                await asyncio.gather(downloader, writer, return_exceptions=True)
                raise
            except KeyboardInterrupt:
                spinner.fail("🛑")
                spinner.write("✖ Download interrupted by user (Ctrl+C)")
                logger.warning("Main - Download manually interrupted by user.")
    except Exception as exp:
        logger.exception("Main - Download failed.")
        raise exp

if __name__ == "__main__":
    try:
        start_time = time.time()
        asyncio.run(async_main())
        total_time = time.time() - start_time
        logger.info(f"Main - Execution completed in {total_time:.2f} seconds")
        print(f"🎯 Execution completed in {total_time:.2f} seconds")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        logger.exception("Main - Unhandled exception in main try block.")
    finally:
        print(f"📝 Logs saved to {log_filename}")