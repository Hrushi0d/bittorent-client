import logging
from pathlib import Path

from utils._AsyncQueue import AsyncQueue
from utils._Bencode import Decoder

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

    def map(self, torrent):
        info = torrent[b'info']
        mappings = []
        offset = 0

        if b'files' in info:
            # Multi-file torrent
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
            # Single-file torrent
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

    async def start_writer(self):
        # Ensure all directories exist
        for m in self.mappings:
            m["path"].parent.mkdir(parents=True, exist_ok=True)

        # Open all files in binary read-write mode, create if not exist
        file_handles = {}
        for m in self.mappings:
            mode = "r+b" if m["path"].exists() else "wb"
            file_handles[m["path"]] = open(m["path"], mode)

        try:
            while True:
                piece = await self.async_queue.pop()
                if piece is None or piece.data is None:
                    break

                abs_offset = piece.index * self.piece_length
                remaining = len(piece.data)
                data_ptr = 0

                # Write the piece data to the correct files
                write_offset = abs_offset
                while remaining > 0:
                    # Find which mapping this offset belongs to
                    file_map = next((m for m in self.mappings if m["start_offset"] <= write_offset <= m["end_offset"]), None)
                    if file_map is None:
                        self.logger.error(f"FileManager - Offset {write_offset} not mapped to any file")
                        break

                    file_handle = file_handles[file_map["path"]]
                    file_offset = write_offset - file_map["start_offset"]
                    write_len = min(file_map["end_offset"] - write_offset + 1, remaining)

                    file_handle.seek(file_offset)
                    file_handle.write(piece.data[data_ptr:data_ptr + write_len])
                    self.logger.info(f"FileManager - Wrote to {file_map['path']} at {file_offset}, length {write_len}")

                    write_offset += write_len
                    data_ptr += write_len
                    remaining -= write_len

        finally:
            # Close all file handles
            for fh in file_handles.values():
                fh.close()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    with open('../Factorio [FitGirl Repack].torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        async_queue = AsyncQueue(logger=logger)
        file_manager = FileManager(torrent=torrent, logger=logger, async_queue=async_queue, download_dir='/')
        print(file_manager.mappings)