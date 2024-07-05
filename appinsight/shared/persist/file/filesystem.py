#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/shared/persist/file/filesystem.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 09:55:53 pm                                                   #
# Modified   : Thursday July 4th 2024 09:14:53 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File System Module"""
import logging
import os
import shutil
from typing import Any

from appinsight.shared.config.env import EnvManager
from appinsight.shared.persist.base import Persistence
from appinsight.shared.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class FileSystem(Persistence):
    """File system class.

    This manages file within a base directory, default is 'data'. Within the base
    directory, files are separated into environments. Files are identified
    by a directory (or directory path) and a filename. The FileSystem
    will read or write from or to the file in the appropriate environment.

    """

    __base_dir = "data"

    def __init__(
        self,
        base_dir: str = None,
        io_cls: type[IOService] = IOService,
        env_mgr_cls: type[EnvManager] = EnvManager,
    ) -> None:
        super().__init__()
        self._base_dir = base_dir or self.__base_dir
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, data: Any, filepath: str, **kwargs) -> None:
        """Creates data on the file system

        Args:
            filepath (str): The path to which the file (or files, i.e. parquet) will be saved.
            data (Any): Data to be saved to file.

        """
        filepath = self._create_path(filepath=filepath)
        if os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._io.write(filepath=filepath, data=data, **kwargs)

    def read(self, filepath: str) -> Any:
        """Reads file from disk

        Args:
            filepath (str): Filepath from which to read the data.

        """
        filepath = self._create_path(filepath=filepath)
        return self._io.read(filepath=filepath)

    def delete(self, filepath: str) -> None:
        """Removes a file from the filesystem

        Args:
            filepath (str): Filepath to the file to be deleted.

        """
        filepath = self._create_path(filepath=filepath)

        try:
            os.remove(filepath)
        except FileNotFoundError as e:
            msg = f"File {filepath} was not found.\n{e}"
            self._logger.exception(msg)
            raise
        except OSError:
            shutil.rmtree(filepath)

    def exists(self, filepath: str) -> bool:
        """Returns the existence of a file in the environment.

        Args:
            filepath (str): Path to file relative to the environment root.

        Returns:
            Boolean: True if file exists, false otherwise.

        """
        return os.path.exists(self.get_filepath(filepath=filepath))

    def get_filepath(self, filepath: str) -> str:
        """Returns a environment specific filepath, given a directory and filename

        Args:
            filepath (str): filepath within the environment.

        Returns:
            Path relative to the project root.

        """
        return self._create_path(filepath=filepath)

    def _create_path(self, filepath: str) -> str:
        """Creates a path (directory or file) based on current environment."""
        env = self._env_mgr.get_environment()
        return os.path.join(self._base_dir, env, filepath)
