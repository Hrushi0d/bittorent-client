# **********************************************************************************************************************
#                     _________  ________  ________  ________  _______   ________   _________
#                    |\___   ___\\   __  \|\   __  \|\   __  \|\  ___ \ |\   ___  \|\___   ___\
#                    \|___ \  \_\ \  \|\  \ \  \|\  \ \  \|\  \ \   __/|\ \  \\ \  \|___ \  \_|
#                         \ \  \ \ \  \\\  \ \   _  _\ \   _  _\ \  \_|/_\ \  \\ \  \   \ \  \
#                          \ \  \ \ \  \\\  \ \  \\  \\ \  \\  \\ \  \_|\ \ \  \\ \  \   \ \  \
#                           \ \__\ \ \_______\ \__\\ _\\ \__\\ _\\ \_______\ \__\\ \__\   \ \__\
#                            \|__|  \|_______|\|__|\|__|\|__|\|__|\|_______|\|__| \|__|    \|__|
#
#                                                 INFO ABOUT THIS FILE
#                               `RedisClient` class, which provides an asynchronous
#                               interface to a Redis database for use as a cache
#
# ******************************************************** IMPORTS *****************************************************

import logging

import redis.asyncio as aioredis


# ***************************************************** REDIS CLIENT **************************************************

class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0, ttl=3600):
        """ Initialize Async Redis Client """
        self.client = aioredis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.ttl = ttl  # Time-to-live for cached data (in seconds)

    async def set_cache(self, key, value):
        """ Asynchronously sets a cache entry with an expiration time. """
        try:
            await self.client.setex(key, self.ttl, value)
            logging.info(f"RedisClient - Cache set for key: {key}")
        except Exception as e:
            logging.error(f"RedisClient - Error setting cache for key {key}: {e}")

    async def get_cache(self, key):
        """ Asynchronously gets cache data, returns None if not found or expired. """
        try:
            cached_value = await self.client.get(key)
            if cached_value:
                logging.info(f"RedisClient - Cache hit for key: {key}")
            else:
                logging.info(f"RedisClient - Cache miss for key: {key}")
            return cached_value
        except Exception as e:
            logging.error(f"RedisClient - Error getting cache for key {key}: {e}")
            return None

    async def delete_cache(self, key):
        """ Asynchronously deletes a specific cache key. """
        try:
            await self.client.delete(key)
            logging.info(f"RedisClient - Cache deleted for key: {key}")
        except Exception as e:
            logging.error(f"RedisClient - Error deleting cache for key {key}: {e}")

    async def clear_all(self):
        """ Asynchronously clears all cache. Use cautiously. """
        try:
            await self.client.flushdb()
            logging.info("RedisClient - All caches cleared.")
        except Exception as e:
            logging.error(f"RedisClient - Error clearing all caches: {e}")
# ********************************************************** EOF *******************************************************
