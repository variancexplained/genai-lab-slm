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
# Modified   : Wednesday September 25th 2024 04:04:39 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
import os
import shelve

from discover.infra.config.reader import ConfigReader


class IDGen:
    """
    A class for generating sequential IDs using a shelve-based key-value store.

    The class interacts with a shelve database to store and retrieve a sequential ID.
    It takes a configuration reader to determine the base directory and initializes
    the ID storage path under that directory. Each time `next_id` is accessed, it retrieves
    the current ID, increments it by 1, and stores the next value for future access.

    Attributes:
    -----------
    _basedir : str
        The base directory where the ID file path is located, obtained from the configuration reader.
    filepath : str
        The full file path where the ID is stored.

    Methods:
    --------
    next_id() -> int:
        Retrieves the current ID from the shelve, increments it, stores the next ID, and returns the current one.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader):
        """
        Initializes the IDGen class by setting up the base directory and shelve connection.

        Parameters:
        -----------
        config_reader_cls : type[ConfigReader], optional
            A class type that reads configuration settings. The default is ConfigReader.

        Raises:
        -------
        Any exceptions raised by os.makedirs or shelve issues.
        """
        self._basedir = config_reader_cls().get_config(
            section="workspace", namespace=False
        )
        self.filepath = os.path.join(self._basedir, "idgen")

        # Ensure the directory for storing the ID file exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    @property
    def next_id(self) -> int:
        """
        Retrieves and increments the current ID stored in the shelve.

        The method fetches the current ID from the shelve database. If no ID exists, it initializes the ID at 0,
        increments it, and stores the next ID for future access.

        Returns:
        --------
        int:
            The next ID.
        """
        with shelve.open(self.filepath, writeback=True) as db:
            current_id = db.get("current_id", 0)
            next_id = current_id + 1
            db["current_id"] = next_id  # Store the incremented ID
            return current_id  # Return the current ID
