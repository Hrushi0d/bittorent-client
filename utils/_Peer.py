import asyncio
import logging
import socket


class Peer:
    def __init__(self, ip, port, info_hash, logger=None):
        self.ip = ip
        self.port = port
        self.logger = logger or logging.getLogger()
        self.info_hash = info_hash
        self.reader = None
        self.writer = None
        self.retry_count = 0

    async def connect(self, timeout=5):
        backoff_sequence = [1, 2, 4, 8, 16]  # Define the backoff sequence
        try:
            # Attempt to connect
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=timeout
            )
            self.logger.info(f"Connected to {self.ip}:{self.port}")
            return True
        except asyncio.TimeoutError:
            self.logger.error(f"Connection attempt to {self.ip}:{self.port} timed out.")
        except (ConnectionRefusedError, socket.gaierror) as e:
            self.logger.error(f"Failed to connect to {self.ip}:{self.port} - {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to {self.ip}:{self.port} - {e}")

        # If connection fails, apply exponential backoff
        if self.retry_count < len(backoff_sequence):
            backoff_time = backoff_sequence[self.retry_count]
        else:
            backoff_time = backoff_sequence[-1]  # Cap the backoff time to the last value (16 seconds)

        self.retry_count += 1  # Increment retry count

        # Sleep for the calculated backoff time before retrying
        await asyncio.sleep(backoff_time)

        return False

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info(f"Disconnected from {self.ip}:{self.port}")
        else:
            self.logger.warning(f"Not connected to {self.ip}:{self.port}, nothing to disconnect.")

    async def send_message(self, message):
        if self.writer:
            self.writer.write(message)
            await self.writer.drain()
            self.logger.info(f"Sent message to {self.ip}:{self.port}")
        else:
            self.logger.error(f"Not connected to {self.ip}:{self.port}, cannot send message.")

    async def receive_message(self, buffer_size=1024):
        if self.reader:
            data = await self.reader.read(buffer_size)
            self.logger.info(f"Received message from {self.ip}:{self.port}")
            return data
        else:
            self.logger.error(f"Not connected to {self.ip}:{self.port}, cannot receive message.")
            return None

    async def __aenter__(self):
        """Async context manager entry, attempts to connect."""
        if not await self.connect():
            raise ConnectionError(f"Failed to connect to {self.ip}:{self.port}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit, ensures disconnection."""
        await self.disconnect()
