import time
from enum import Enum, auto


class Piece:
    class Status(Enum):
        NOT_STARTED = auto()
        IN_PROGRESS = auto()
        COMPLETED = auto()
        FAILED = auto()

    def __init__(self, index: int, hash_value: bytes, length: int):
        self.index = index
        self.hash_value = hash_value
        self.length = length
        self._status = Piece.Status.NOT_STARTED  # Correctly use Piece.Status
        self._status_history = [(self._status, time.time())]

    def update_status(self, new_status: Status):
        if new_status == self._status:
            return  # no change
        self._status = new_status
        self._status_history.append((new_status, time.time()))

    def status(self):
        return self._status

    def reset_status(self):
        self.update_status(Piece.Status.NOT_STARTED)

    def status_history(self):
        return self._status_history

    def is_completed(self) -> bool:
        return self._status == Piece.Status.COMPLETED

    def __repr__(self):
        return f'Piece(index={self.index}, length={self.length}, status={self._status.value})'


# import asyncio
# import time
# import numpy as np
# from enum import Enum, auto
# from tqdm import tqdm
# from random import randint, random
# from dataclasses import dataclass
# from typing import List, Dict
# from collections import defaultdict
# import math
#
#
# class Piece:
#     class Status(Enum):
#         NOT_STARTED = auto()
#         IN_PROGRESS = auto()
#         COMPLETED = auto()
#         FAILED = auto()
#         PAUSED = auto()
#
#     def __init__(self, index: int, length: int):
#         self.index = index
#         self.length = length
#         self._status = Piece.Status.NOT_STARTED
#         self._status_history = [(self._status, time.time())]
#         self.downloaded_bytes = 0
#         self.start_time = None
#         self.end_time = None
#
#     def update_status(self, new_status: Status):
#         if new_status != self._status:
#             self._status = new_status
#             self._status_history.append((new_status, time.time()))
#             if new_status == Piece.Status.IN_PROGRESS and self.start_time is None:
#                 self.start_time = time.time()
#             if new_status in (Piece.Status.COMPLETED, Piece.Status.FAILED):
#                 self.end_time = time.time()
#
#     def is_completed(self) -> bool:
#         return self._status == Piece.Status.COMPLETED
#
#     def is_failed(self) -> bool:
#         return self._status == Piece.Status.FAILED
#
#     def is_active(self) -> bool:
#         return self._status == Piece.Status.IN_PROGRESS
#
#     def progress(self) -> float:
#         return self.downloaded_bytes / self.length if self.length > 0 else 0
#
#     def download_speed(self) -> float:
#         if not self.start_time or not self.is_active():
#             return 0
#         elapsed = time.time() - self.start_time
#         return self.downloaded_bytes / elapsed if elapsed > 0 else 0
#
#     def time_remaining(self) -> float:
#         speed = self.download_speed()
#         if speed == 0:
#             return float('inf')
#         return (self.length - self.downloaded_bytes) / speed
#
#     def __repr__(self):
#         return f'Piece(index={self.index}, length={self.length}, status={self._status}, progress={self.progress():.1%})'
#
#
# @dataclass
# class DownloadStats:
#     total_pieces: int = 0
#     completed_pieces: int = 0
#     failed_pieces: int = 0
#     active_pieces: int = 0
#     total_bytes: int = 0
#     downloaded_bytes: int = 0
#     download_speed: float = 0  # bytes/sec
#     time_remaining: float = 0
#
#
# class DownloadManager:
#     def __init__(self, pieces: List[Piece], max_concurrent_downloads: int = 3):
#         self.pieces = pieces
#         self.max_concurrent_downloads = max_concurrent_downloads
#         self._is_paused = False
#         self._should_stop = False
#         self.start_time = None
#         self.last_update_time = None
#         self.downloaded_bytes_history = []
#         self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
#         self.priority_order = sorted(range(len(pieces)), key=lambda i: pieces[i].length, reverse=True)
#
#     async def simulate_piece_download(self, piece: Piece, overall_pbar, piece_pbars):
#         async with self.semaphore:
#             if self._should_stop:
#                 return
#
#             piece_pbar = piece_pbars[piece.index]
#             piece.update_status(Piece.Status.IN_PROGRESS)
#
#             downloaded = 0
#             chunk_size = 16  # bytes per "tick"
#
#             while downloaded < piece.length and not self._should_stop:
#                 while self._is_paused and not self._should_stop:
#                     piece.update_status(Piece.Status.PAUSED)
#                     await asyncio.sleep(0.1)
#
#                 if self._should_stop:
#                     break
#
#                 piece.update_status(Piece.Status.IN_PROGRESS)
#
#                 # Simulate variable download speed
#                 actual_chunk_size = chunk_size * (0.5 + random())
#                 increment = min(actual_chunk_size, piece.length - downloaded)
#
#                 await asyncio.sleep(0.05 * (0.8 + 0.4 * random()))  # simulate variable network latency
#
#                 downloaded += increment
#                 piece.downloaded_bytes = downloaded
#
#                 # Update progress bars
#                 piece_pbar.update(increment)
#                 overall_pbar.update(increment)
#
#                 # Record download history for speed calculation
#                 self.downloaded_bytes_history.append((time.time(), increment))
#
#             # Simulate validation
#             if not self._should_stop:
#                 data = np.random.bytes(piece.length)
#                 if randint(0, 100) < 95:  # 95% chance of success
#                     piece.update_status(Piece.Status.COMPLETED)
#                     piece_pbar.set_description(f"Piece {piece.index} ✅")
#                 else:
#                     piece.update_status(Piece.Status.FAILED)
#                     piece_pbar.set_description(f"Piece {piece.index} ❌")
#                     # For failed pieces, reset progress (simulating needing to redownload)
#                     piece.downloaded_bytes = 0
#                     overall_pbar.update(-downloaded)  # subtract the failed bytes
#                     piece_pbar.reset()
#
#     def get_stats(self) -> DownloadStats:
#         stats = DownloadStats()
#         stats.total_pieces = len(self.pieces)
#         stats.completed_pieces = sum(p.is_completed() for p in self.pieces)
#         stats.failed_pieces = sum(p.is_failed() for p in self.pieces)
#         stats.active_pieces = sum(p.is_active() for p in self.pieces)
#         stats.total_bytes = sum(p.length for p in self.pieces)
#         stats.downloaded_bytes = sum(p.downloaded_bytes for p in self.pieces)
#
#         # Calculate download speed (bytes/sec) based on recent history
#         now = time.time()
#         window = 5  # seconds to look back for speed calculation
#         recent_bytes = sum(b for (t, b) in self.downloaded_bytes_history if t > now - window)
#         stats.download_speed = recent_bytes / window if window > 0 else 0
#
#         # Calculate time remaining
#         remaining_bytes = stats.total_bytes - stats.downloaded_bytes
#         stats.time_remaining = remaining_bytes / stats.download_speed if stats.download_speed > 0 else float('inf')
#
#         return stats
#
#     def toggle_pause(self):
#         self._is_paused = not self._is_paused
#         return self._is_paused
#
#     def stop(self):
#         self._should_stop = True
#
#     async def start_download(self):
#         self.start_time = time.time()
#         self.last_update_time = time.time()
#
#         # Create progress bars
#         overall_pbar = tqdm(
#             total=sum(p.length for p in self.pieces),
#             desc="Overall Progress",
#             unit='B',
#             unit_scale=True,
#             unit_divisor=1024,
#             leave=True,
#             ncols=100,
#             position=0
#         )
#
#         piece_pbars = [
#             tqdm(
#                 total=p.length,
#                 desc=f"Piece {p.index}",
#                 unit='B',
#                 unit_scale=True,
#                 unit_divisor=1024,
#                 leave=False,
#                 ncols=80,
#                 position=i + 1
#             )
#             for i, p in enumerate(self.pieces)
#         ]
#
#         # Create and run download tasks
#         tasks = []
#         for idx in self.priority_order:
#             piece = self.pieces[idx]
#             tasks.append(self.simulate_piece_download(piece, overall_pbar, piece_pbars))
#
#         # Create a task to update stats periodically
#         stats_task = asyncio.create_task(self.display_stats(overall_pbar, piece_pbars))
#
#         try:
#             await asyncio.gather(*tasks)
#         except asyncio.CancelledError:
#             print("\nDownload was cancelled!")
#         finally:
#             stats_task.cancel()
#             overall_pbar.close()
#             for pbar in piece_pbars:
#                 pbar.close()
#
#             self.show_summary()
#
#     async def display_stats(self, overall_pbar, piece_pbars):
#         while not self._should_stop:
#             stats = self.get_stats()
#
#             # Update overall progress bar description
#             overall_pbar.set_description(
#                 f"Overall Progress [Speed: {self.format_speed(stats.download_speed)}, "
#                 f"ETA: {self.format_time(stats.time_remaining)}]"
#             )
#
#             # Update individual piece bars
#             for i, p in enumerate(self.pieces):
#                 if p.is_active():
#                     piece_pbars[i].set_description(
#                         f"Piece {p.index} [{self.format_speed(p.download_speed())}]"
#                     )
#
#             await asyncio.sleep(0.5)
#
#     def show_summary(self):
#         stats = self.get_stats()
#         total_time = time.time() - self.start_time if self.start_time else 0
#         avg_speed = stats.downloaded_bytes / total_time if total_time > 0 else 0
#
#         print("\nDownload Summary:")
#         print(f"✅ Completed: {stats.completed_pieces}/{stats.total_pieces} pieces")
#         print(f"❌ Failed:    {stats.failed_pieces}/{stats.total_pieces} pieces")
#         print(f"📦 Data:      {self.format_bytes(stats.downloaded_bytes)}/{self.format_bytes(stats.total_bytes)} "
#               f"({stats.downloaded_bytes / stats.total_bytes:.1%})")
#         print(f"⏱️  Time:      {self.format_time(total_time)}")
#         print(f"🚀 Avg Speed: {self.format_speed(avg_speed)}")
#
#         # Show piece status summary
#         status_counts = defaultdict(int)
#         for p in self.pieces:
#             status_counts[p._status] += 1
#
#         print("\nPiece Status:")
#         for status, count in status_counts.items():
#             print(f"  {status.name}: {count}")
#
#     @staticmethod
#     def format_bytes(size: float) -> str:
#         if size == 0:
#             return "0B"
#         units = ["B", "KB", "MB", "GB"]
#         i = int(math.floor(math.log(size, 1024)))
#         p = math.pow(1024, i)
#         s = round(size / p, 2)
#         return f"{s} {units[i]}"
#
#     @staticmethod
#     def format_speed(speed: float) -> str:
#         return f"{DownloadManager.format_bytes(speed)}/s"
#
#     @staticmethod
#     def format_time(seconds: float) -> str:
#         if seconds == float('inf'):
#             return "∞"
#         seconds = int(seconds)
#         minutes, seconds = divmod(seconds, 60)
#         hours, minutes = divmod(minutes, 60)
#         if hours > 0:
#             return f"{hours}h {minutes}m {seconds}s"
#         elif minutes > 0:
#             return f"{minutes}m {seconds}s"
#         else:
#             return f"{seconds}s"
#
#
# # Run the simulation
# async def main():
#     # Create pieces with random sizes
#     pieces = [Piece(i, length=randint(100, 500)) for i in range(8)]
#     manager = DownloadManager(pieces, max_concurrent_downloads=3)
#
#     # Start the download in a separate task so we can simulate user interaction
#     download_task = asyncio.create_task(manager.start_download())
#
#     # Simulate user pausing/resuming after 2 seconds
#     await asyncio.sleep(2)
#     print("\nPausing download...")
#     manager.toggle_pause()
#     await asyncio.sleep(2)
#     print("Resuming download...")
#     manager.toggle_pause()
#
#     # Wait for download to complete or be interrupted
#     try:
#         await download_task
#     except asyncio.CancelledError:
        print("Download was cancelled!")


if __name__ == "__main__":
    asyncio.run(main())
