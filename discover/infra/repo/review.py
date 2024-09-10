#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/filesystem.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 02:58:50 pm                                               #
# Modified   : Monday September 9th 2024 03:42:31 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File System Module"""
import logging
import os
import shutil
from typing import Optional

import pandas as pd
from discover.domain.service.repo import Repo
from discover.infra.config.config import Config
from discover.infra.repo.io import IOService


# ------------------------------------------------------------------------------------------------ #
class ReviewRepo(Repo):
    """File system class.

    This manages file within a base directory, default is 'data'. Within the base
    directory, files are separated into environments. Files are identified
    by a directory (or directory path) and a filename. The FileSystem
    will read or write from or to the file in the appropriate environment.

    """

    __base_dir = "data"

    def __init__(
        self,
        base_dir: Optional[str] = None,
        io_cls: type[IOService] = IOService,
        env_mgr_cls: type[Config] = Config,
    ) -> None:
        super().__init__()
        self._base_dir = base_dir or self.__base_dir
        self._io = io_cls()
        self._env_mgr = env_mgr_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, stage: str, entity: str, data: pd.DataFrame) -> None:
        """Creates data on the file system

        Args:
            filepath (str): The path to which the file (or files, i.e. parquet) will be saved.
            data (Any): Data to be saved to file.

        """
        filepath = self._create_path(stage=stage, entity=entity)
        if os.path.exists(filepath):
            msg = f"File {filepath} cannot be created. It already exists."
            self._logger.exception(msg)
            raise FileExistsError(msg)

        self._io.write(filepath=filepath, data=data)

    def get(self, stage: str, entity: str) -> pd.DataFrame:
        """Reads file from disk

        Args:
            filepath (str): Filepath from which to read the data.

        """
        filepath = self._create_path(stage=stage, entity=entity)
        return self._io.read(filepath=filepath)

    def remove(self, stage: str, entity: str) -> None:
        """Removes a file from the filesystem

        Args:
            filepath (str): Filepath to the file to be deleted.

        """
        confirm = input(
            f"Removing {entity} from {stage} stage is permanent. Confirm [Y/N]."
        )
        if confirm.lower() == "y":
            filepath = self._create_path(stage=stage, entity=entity)

            try:
                os.remove(filepath)
            except FileNotFoundError as e:
                msg = f"File {filepath} was not found.\n{e}"
                self._logger.exception(msg)
                raise
            except OSError:
                shutil.rmtree(filepath)
        else:
            self._logger.info(f"Removal of {entity} from {stage} stage aborted.")

    def exists(self, stage: str, entity: str) -> bool:
        """Returns the existence of a file in the environment.

        Args:
            filepath (str): Path to file relative to the environment root.

        Returns:
            Boolean: True if file exists, false otherwise.

        """
        filepath = self._create_path(stage=stage, entity=entity)
        return os.path.exists(filepath)

    def _create_path(self, stage: str, entity: str) -> str:
        """Creates a path (directory or file) based on current environment."""
        env = self._env_mgr.get_environment()
        return os.path.join(self._base_dir, env, stage, entity)
