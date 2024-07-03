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
# Modified   : Tuesday July 2nd 2024 10:19:51 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File System Module"""
import logging
import os
import shutil
from typing import Any

from appinsight.infrastructure.config.env import EnvManager
from appinsight.infrastructure.persist.base import Persistence
from appinsight.infrastructure.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class FileSystem(Persistence):
    """File system class."""

    def __init__(
        self,
        io_cls: type[IOService] = IOService,
        env_mgr_cls: type[EnvManager] = EnvManager,
    ) -> None:
        super().__init__()
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, data: Any, directory: str, filename: str = None, **kwargs) -> None:
        """Creates data on the file system

        Args:
            directory (str): Directory to which the data is to be saved.
            filename (str): Name of file. Optional.
            data (Any): Data to be saved to file.

        Returns:
            str: The filepath to which the data was saved.
        """
        filepath = self._create_path(directory=directory, filename=filename)
        if os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._io.write(filepath=filepath, data=data, **kwargs)

    def read(self, directory: str, filename: str = None) -> Any:
        """Reads file from disk

        Args:
            filepath (str): Filepath from which to read the data.

        """
        filepath = self._create_path(directory=directory, filename=filename)
        return self._io.read(filepath=filepath)

    def delete(self, directory: str, filename: str = None) -> None:
        """Removes a file from the filesystem

        Args:
            filepath (str): Filepath from which to read the data.

        """
        filepath = self._create_path(directory=directory, filename=filename)

        try:
            os.remove(filepath)
        except FileNotFoundError as e:
            msg = f"File {filepath} was not found.\n{e}"
            self._logger.exception(msg)
            raise
        except OSError:
            shutil.rmtree(filepath)

    def _create_path(self, directory: str, filename: str = None) -> str:
        """Creates a path (directory or file) based on current environment."""
        env = self._env_mgr.get_environment()
        if filename is not None:
            return os.path.join("data", env, directory, filename)
        else:
            return os.path.join("data", env, directory)
