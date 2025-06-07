import argparse
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


def setup_logger():
    os.makedirs('../logs', exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    log_filename = f'../logs/downloader_{timestamp}.log'
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    return log_filename


async def progress_reporter(download_manager, interval=1.0, spinner=None):
    while True:
        try:
            report = download_manager.checker.progress_report()
            progress_message = (
                f"[Progress] {report['completed_pieces']}/{report['total_pieces']} "
                f"({report['fraction_done'] * 100:.1f}%) - "
                f"Elapsed: {report['time_elapsed_seconds']:.1f}s"
            )

            if spinner:
                spinner.text = f"Downloading... {progress_message}"
            else:
                print(progress_message)

            if report['completed_pieces'] >= report['total_pieces']:
                download_manager.checker.finished = True
                return True

            await asyncio.sleep(interval)
        except Exception:
            logging.exception("Progress reporter encountered an error")
            return False


async def async_main(torrent_path: str, mode: str):
    logger = logging.getLogger(__name__)
    main_spinner = None
    active_tasks = []

    try:
        logger.info("Main - Starting PeerGetter BitTorrent client...")
        start_time = time.time()

        # Step 1: Decode Torrent File
        with yaspin(Spinners.line, text="Reading and decoding torrent file...") as spinner:
            try:
                t0 = time.time()
                with open(torrent_path, 'rb') as f:
                    meta_info = f.read()
                    torrent = Decoder(meta_info).decode()
                decode_time = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✔ Decoded torrent in {decode_time:.2f} seconds")
                logger.info(f"Main - Decoded torrent in {decode_time:.2f} seconds.")
            except Exception:
                spinner.fail("💥")
                spinner.write("✖ Failed to decode .torrent file")
                logger.exception("Main - Failed to decode .torrent file")
                raise

        # Step 2: Peer Discovery
        with yaspin(Spinners.line, text="Running peer discovery...") as spinner:
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
            except Exception:
                spinner.fail("💥")
                spinner.write("✖ Peer discovery failed")
                logger.exception("Main - Peer discovery failed.")
                raise

        # Step 3: Piece Selection
        with yaspin(Spinners.line, text="Selecting pieces...") as spinner:
            try:
                spinner.write(f"✓ Strategy used: {mode}")
                t0 = time.time()
                piece_manager = PieceManager(piece_dict, torrent=torrent, mode=mode, logger=logger)
                pieces = piece_manager.run()
                elapsed = time.time() - t0
                spinner.ok("✔")
                spinner.write(f"✓ Selected {len(pieces)} pieces in {elapsed:.2f} seconds")
                logger.info(
                    f"Main - Loaded {len(pieces)} pieces in {elapsed:.2f} seconds using {mode} strategy.")
            except Exception:
                spinner.fail("💥")
                spinner.write("✖ Piece selection failed")
                logger.exception("Main - Piece selection failed.")
                raise

        # Step 4: Initialize Managers
        async_queue = AsyncQueue(logger=logger)
        file_manager = FileManager(torrent=torrent, logger=logger, async_queue=async_queue, download_dir='../outputs/')

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
            except Exception:
                spinner.fail("💥")
                spinner.write("✖ Failed to initialize download manager")
                logger.exception("Main - DownloadManager initialization failed.")
                raise

        # Step 5: Start Download
        main_spinner = yaspin(Spinners.line, text="Starting download...")
        main_spinner.start()

        try:
            t0 = time.time()
            downloader_task = asyncio.create_task(download_manager.start())
            writer_task = asyncio.create_task(file_manager.start_writer())
            active_tasks.extend([downloader_task, writer_task])
            is_complete = await progress_reporter(download_manager, interval=1.0, spinner=main_spinner)

            if is_complete:
                await download_manager.stop()
                await file_manager.stop()
                await asyncio.gather(*active_tasks, return_exceptions=True)
                elapsed = time.time() - t0
                main_spinner.ok("✔")
                main_spinner.write(f"✓ Download completed in {elapsed:.2f} seconds")
                logger.info(f"Main - Download completed in {elapsed:.2f} seconds.")
            else:
                main_spinner.fail("❌")
                main_spinner.write("✖ Download failed to complete properly")
                logger.error("Main - Download did not complete successfully.")
        except KeyboardInterrupt:
            logger.warning("Main - Download manually interrupted by user.")
            main_spinner.fail("🛑")
            main_spinner.write("✖ Download interrupted by user (Ctrl+C)")
            await download_manager.stop()
            await file_manager.stop()
            for task in active_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*active_tasks, return_exceptions=True)
        except Exception as e:
            main_spinner.fail("💥")
            main_spinner.write(f"✖ Download error: {str(e)}")
            logger.exception("Main - Download failed with exception.")
            await download_manager.stop()
            await file_manager.stop()
            for task in active_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*active_tasks, return_exceptions=True)
            raise
    finally:
        if main_spinner:
            main_spinner.stop()


def main():
    parser = argparse.ArgumentParser(description="BitTorrent CLI Downloader")
    parser.add_argument("-f", "--file", required=True, help="Path to the .torrent file")
    parser.add_argument("-m", "--mode", choices=["rarest-first", "random-rarest-first", "sequential"],
                        default="rarest-first", help="Piece selection strategy")
    args = parser.parse_args()

    log_path = setup_logger()
    try:
        asyncio.run(async_main(args.file, args.mode))
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        logging.exception("Main - Unhandled exception in CLI")
    finally:
        print(f"📝 Logs saved to {log_path}")


if __name__ == "__main__":
    main()
