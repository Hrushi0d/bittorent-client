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
        self.output_file = self.download_dir / self.info_dict[b'name'].decode()

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
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_file, "r+b" if self.output_file.exists() else "wb") as f:
            while True:
                piece = await self.async_queue.pop()
                if piece is None or piece.data is None:
                    break  # Could also check for a special sentinel value

                offset = piece.index * self.piece_length
                f.seek(offset)
                f.write(piece.data)
                self.logger.info(f"FileManager - Wrote piece {piece.index} to offset {offset}")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,  # Or use INFO if DEBUG is too verbose
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