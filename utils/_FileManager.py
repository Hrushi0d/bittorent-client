import logging
from pathlib import Path
import aiofiles

from utils._AsyncQueue import AsyncQueue

class FileManager:
    def __init__(self, async_queue: AsyncQueue, logger: logging.Logger, torrent, download_dir: str):
        self.async_queue = async_queue
        self.logger = logger
        self.logger.info('FileManager - Initialized')
        self.info_dict = torrent[b'info']
        self.piece_length = self.info_dict[b'piece length']
        self.pieces_data = self.info_dict[b'pieces']
        self.download_dir = Path(download_dir)
        self.mappings = self.map(torrent)
        self.total_length = sum(m["length"] for m in self.mappings)
        self._intervals = []
        for i, m in enumerate(self.mappings):
            self._intervals.append((m["start_offset"], m["end_offset"], i))
        self._intervals.sort()
        self._stopped = False

    def map(self, torrent):
        info = torrent[b'info']
        mappings = []
        offset = 0

        if b'files' in info:
            base_dir = info[b'name'].decode()
            for file in info[b'files']:
                length = file[b'length']
                path_components = [component.decode() for component in file[b'path']]
                file_path = self.download_dir / base_dir / Path(*path_components)
                mappings.append({
                    "path": file_path,
                    "length": length,
                    "start_offset": offset,
                    "end_offset": offset + length - 1
                })
                offset += length
        else:
            length = info[b'length']
            name = info[b'name'].decode()
            file_path = self.download_dir / name
            mappings.append({
                "path": file_path,
                "length": length,
                "start_offset": 0,
                "end_offset": length - 1
            })

        return mappings

    def _find_mapping_idx(self, abs_offset: int):
        intervals = self._intervals
        lo, hi = 0, len(intervals)
        while lo < hi:
            mid = (lo + hi) // 2
            start, end, idx = intervals[mid]
            if abs_offset < start:
                hi = mid
            elif abs_offset > end:
                lo = mid + 1
            else:
                return idx
        return None

    async def start_writer(self):
        for m in self.mappings:
            m["path"].parent.mkdir(parents=True, exist_ok=True)
        file_handles = {}
        for m in self.mappings:
            mode = "r+b" if m["path"].exists() else "wb"
            file_handles[m["path"]] = await aiofiles.open(m["path"], mode)

        try:
            while not self._stopped:
                piece = await self.async_queue.pop()
                if piece is None or getattr(piece, "data", None) is None:
                    break

                abs_offset = piece.index * self.piece_length
                remaining = len(piece.data)
                data_ptr = 0

                write_offset = abs_offset
                while remaining > 0:
                    mapping_idx = self._find_mapping_idx(write_offset)
                    if mapping_idx is None:
                        self.logger.error(f"FileManager - Offset {write_offset} not mapped to any file")
                        break
                    file_map = self.mappings[mapping_idx]
                    file_handle = file_handles[file_map["path"]]
                    file_offset = write_offset - file_map["start_offset"]
                    write_len = min(file_map["end_offset"] - write_offset + 1, remaining)

                    await file_handle.seek(file_offset)
                    await file_handle.write(piece.data[data_ptr:data_ptr + write_len])
                    self.logger.info(f"FileManager - Wrote to {file_map['path']} at {file_offset}, length {write_len}")

                    write_offset += write_len
                    data_ptr += write_len
                    remaining -= write_len

        finally:
            for fh in file_handles.values():
                await fh.close()
            self.logger.info("FileManager - Closed all file handles.")

    async def stop(self):
        """Signal the writer to exit by pushing a None to the async_queue."""
        self._stopped = True
        await self.async_queue.push(None, None)