#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/file/filesystem.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 09:55:53 pm                                                   #
# Modified   : Monday July 1st 2024 05:40:38 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File System Module"""
import logging
import os
from typing import Any

from appinsight.infrastructure.persist.base import Persistence
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class FileSystem(Persistence):
    """File system class."""

    def __init__(
        self,
        io_cls: type[IOService] = IOService,
    ) -> None:
        super().__init__()
        self._io = io_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, filepath: str, data: Any) -> None:
        """Creates data on the file system

        Args:
            filepath (str): Path to the file
            data (Any): Data to be saved to file.
        """
        self._io.write(filepath=filepath, data=data)

    def read(self, filepath) -> Any:
        """Reads file from disk

        Args:
            filepath (str): Filepath from which to read the data.

        """

    def delete(self, filepath) -> None:
        """Removes a file from the filesystem

        Args:
            filepath (str): Filepath from which to read the data.

        """
        delete = input(
            f"You don't really want to delete {filepath}. Pop 'Y' if you do."
        )
        if "Y" == delete:
            try:
                os.remove(filepath)
            except FileNotFoundError as e:
                msg = f"File {filepath} was not found.\n{e}"
                self._logger.exception(msg)
                raise
