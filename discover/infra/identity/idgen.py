#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/identity/idgen.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 09:53:16 pm                                              #
# Modified   : Monday September 23rd 2024 02:29:27 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

import redis

from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
class IDGen:
    """
    A class for generating sequential IDs using Redis as a key-value store.

    The class interacts with a Redis database to store and retrieve a sequential ID.
    It takes a configuration reader to determine the base directory and initializes
    the ID storage path under that directory. Each time `next_id` is accessed, it retrieves
    the current ID, increments it by 1, and stores the next value for future access.

    Attributes:
    -----------
    _basedir : str
        The base directory where the ID file path is located, obtained from the configuration reader.
    filepath : str
        The full file path where the ID is stored, used as the Redis key.
    redis_client : redis.Redis
        The Redis client instance used to interact with the Redis database.

    Methods:
    --------
    next_id():
        Retrieves the current ID from Redis, increments it, stores the next ID, and returns the current one.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader):
        """
        Initializes the IDGen class by setting up the base directory and Redis connection.

        Parameters:
        -----------
        config_reader_cls : type[ConfigReader], optional
            A class type that reads configuration settings. The default is ConfigReader.

        Raises:
        -------
        Any exceptions raised by os.makedirs or Redis connection issues.
        """
        self._basedir = config_reader_cls().get_config(
            section="workspace", namespace=False
        )
        self.filepath = os.path.join(self._basedir, "idgen")

        # Ensure the directory for storing the ID file exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

        # Set up the Redis client to connect to the local Redis server
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

    @property
    def next_id(self) -> int:
        """
        Retrieves and increments the current ID stored in Redis.

        The method fetches the current ID from the Redis database using the filepath as the key.
        If no ID exists, it initializes the ID to 1. The current ID is returned, and the next
        ID is stored in Redis for the next call.

        Returns:
        --------
        int
            The current ID before it was incremented.
        """
        # Get the current ID from Redis
        current_id = self.redis_client.get(self.filepath)
        if current_id is None:
            # Initialize current ID if it doesn't exist
            current_id = 1
            self.redis_client.set(self.filepath, current_id)
        else:
            current_id = int(current_id)

        # Increment and store the next ID
        next_id = current_id + 1
        self.redis_client.set(self.filepath, next_id)

        return current_id
