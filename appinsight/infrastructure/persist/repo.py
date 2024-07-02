#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/repo.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 05:08:52 am                                                    #
# Modified   : Monday July 1st 2024 06:03:50 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Implementation Module"""
import logging
import os
from typing import Union

import pandas as pd

from appinsight.domain.dataset import Dataset
from appinsight.domain.enums import Stage
from appinsight.domain.repo import Repo
from appinsight.infrastructure.persist.database.db import SQLiteDB
from appinsight.infrastructure.persist.file.filesystem import FileSystem
from appinsight.infrastructure.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Abstract base class defining the interface for repositories."""

    def __init__(
        self,
        fs_cls: type[FileSystem] = FileSystem,
        db_cls: type[SQLiteDB] = SQLiteDB,
        env_mgr_cls: type[EnvManager] = EnvManager,
    ) -> None:
        super().__init__()
        self._fs = fs_cls()
        self._db = db_cls()
        self._env_mgr = env_mgr_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, dataset: Dataset) -> None:
        """Adds an entity to the repository."""
        dataset.validate()
        # Persist the content
        filepath = self._get_filepath(dataset=dataset)
        self._fs.create(filepath=filepath, data=dataset.content)

        # Persist the metadata
        query = """INSERT INTO dataset (name, description, phase, stage, size, nrows, ncols, creator, created)
        VALUES
        (?,?,?,?,?,?,?,?,?);"""
        params = (
            dataset.name,
            dataset.description,
            dataset.phase,
            dataset.stage,
            dataset.size,
            dataset.nrows,
            dataset.ncols,
            dataset.creator,
            dataset.created,
        )
        dataset.oid = self._db.create(query=query, params=params)
        return dataset

    def get(self, oid: int) -> Dataset:
        """Returns an item from the repository."""

    def find(self, *args, **kwargs) -> Union[None, pd.DataFrame]:
        """Returns a DataFrame of results from the repository"""

    def remove(self, *args, **kwargs) -> None:
        """Removes zero, one or more items from the repository."""

    def _get_filepath(self, dataset: Dataset) -> str:
        """Returns the filepath for a dataset."""
        directory = Stage[dataset.stage].value
        env = self._env_mgr().get_environment()
        return os.path.join("data", env, directory, dataset.name)
