#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/shared/recovery/base.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 10:23:48 pm                                                   #
# Modified   : Thursday July 4th 2024 07:38:31 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from dependency_injector.wiring import Provide, inject

from appinsight.shared.dependency.container import AppInsightContainer
from appinsight.shared.persist.database.base import Database


# ------------------------------------------------------------------------------------------------ #
class Recovery(ABC):

    @inject
    def __init__(
        self, config_cls, db: Database = Provide[AppInsightContainer.db.sqlite]
    ):
        super().__init__()
        self._config = config_cls()
        self._db = db()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        """Count the number of backups."""
        return len(self.show_backups())

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @abstractmethod
    def backup(self, *args, **kwargs) -> None:
        """Executes a backup of an asset."""

    @abstractmethod
    def restore(self, *args, **kwargs) -> None:
        """Executes a restore of an asset."""

    def show_backups(self) -> pd.DataFrame:
        """Return the backups from the database."""
        query = """SELECT * FROM backup WHERE backup_type =:backup_type;"""
        params = {"backup_type": self.__BACKUP_TYPE}
        return self._db.query(query, params)

    def _generate_backup_filename(self, backup_name: str) -> str:
        """Generate a backup filename with the current timestamp.

        Args:
            backup_name (str): Name of the backup file sans datetime.

        Returns:
            str: The generated filename with timestamp.
        """
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return f"{backup_name}_{current_datetime}.tar.gz"

    def _log_backup(
        self,
        backup_source: str,
        backup_filename: str,
        file_count: int,
        dir_count: int,
        size_uncompressed: int,
        size_compressed: int,
    ) -> None:
        """Logs the backup to the database"""
        log = {
            "backup_type": self.__BACKUP_TYPE,
            "backup_source": backup_source,
            "backup_filename": backup_filename,
            "n_files": file_count,
            "n_directories": dir_count,
            "size_compressed": size_compressed,
            "size_uncompressed": size_uncompressed,
        }
        self._logger.debug(f"Logging backup as:\n{log}")
        log = pd.DataFrame(data=log, index=[0])
        _ = self._db.insert(data=log, tablename="backup", if_exists="append")
        self._logger.debug("Logging complete.")
        return
