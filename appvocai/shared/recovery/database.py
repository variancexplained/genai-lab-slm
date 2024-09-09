#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/shared/recovery/database.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 02:03:55 pm                                                 #
# Modified   : Tuesday August 27th 2024 10:54:13 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Database Recovery Module"""
import os
import tarfile

from appvocai.shared.config.recovery import DBRecoveryConfig
from appvocai.shared.dependency.container import AppInsightContainer
from appvocai.shared.persist.database.base import Database
from appvocai.shared.recovery.base import Recovery
from dependency_injector.wiring import Provide, inject


# ------------------------------------------------------------------------------------------------ #
class DBRecovery(Recovery):

    __BACKUP_TYPE = "Database"

    @inject
    def __init__(
        self,
        config_cls: type[DBRecoveryConfig] = DBRecoveryConfig,
        db: Database = Provide[AppInsightContainer.db.sqlite],
    ) -> None:
        super().__init__(config_cls, db)

    def backup(self, name: str = None) -> None:
        """Executes a backup of the database.

        Args:
            name (str, optional): A name for the backup. Defaults to current timestamp.

        Raises:
            FileNotFoundError: If the specified backup_source does not exist.
        """
        # Ascertain and validate backup_source
        backup_source = self._config.get_backup_source()
        self.logger.debug(f"Backing up from {backup_source}")

        if not os.path.exists(backup_source):
            raise FileNotFoundError(
                f"The specified backup_source '{backup_source}' does not exist."
            )
        self.logger.debug(f"Source of backup is {backup_source}")

        # Compute the uncompressed size
        size_uncompressed = os.path.getsize(backup_source)

        # Backup name is the designated name or the basename for the backup_source.
        backup_name = name or os.path.basename(os.path.normpath(backup_source))

        # Backup folder from config, make sure the directory exists
        backup_folder = self._config.get_backup_directory()
        os.makedirs(backup_folder, exist_ok=True)
        self.logger.debug(f"Back up folder is {backup_folder}")

        # Backup filename is a concatenation of backup_name and current datetime.
        backup_filename = self._generate_backup_filename(backup_name)
        backup_filepath = os.path.join(backup_folder, backup_filename)
        self.logger.debug(f"Backing up to {backup_filename}")

        # Create the backup archive
        with tarfile.open(backup_filepath, "w:gz") as tar:
            tar.add(backup_source, arcname=os.path.basename(backup_source))

        # Capture compressed size.
        size_compressed = os.path.getsize(backup_filepath)
        # Log the backup to the backup database
        self.log_backup(
            backup_source=backup_source,
            backup_filename=backup_filename,
            file_count=1,
            dir_count=0,
            size_uncompressed=size_uncompressed,
            size_compressed=size_compressed,
        )

        print(f"Backup completed: {backup_filename}")

    def restore(
        self,
        backup_filename: str,
        force: bool = False,
    ) -> None:
        """Executes a restore of a folder from a tar.gz file.

        The original backup_source of the backup contained in the backup_filename
        is obtained from the database.

        Args:
            backup_filename (str): The name of the backup file in the restore folder.
            force (bool, optional): If True, overwrite existing files. Defaults to False.

        Raises:
            FileNotFoundError: If the specified backup file does not exist.
        """
        # Obtain the backup source from config
        backup_source = self._config.get_backup_source()
        self.logger.debug(f"Recovering to {backup_source} from {backup_filename}")
        # Convert the backup_filename to a path
        backup_filepath = os.path.join(
            self._config.get_backup_directory(), backup_filename
        )
        # Confirm it exists.
        if not os.path.exists(backup_filepath):
            raise FileNotFoundError(
                f"Error: Backup file '{backup_filename}' not found."
            )

        # Check if force is False and backup_source is not empty
        if not force and os.path.exists(backup_source) and os.listdir(backup_source):
            confirm = (
                input(
                    f"Files already exist in '{backup_source}'. Overwrite? (yes/no): "
                )
                .strip()
                .lower()
            )
            if confirm != "yes":
                print("Restore aborted.")
                return

        with tarfile.open(backup_filepath, "r:gz") as tar:
            tar.extractall(path=backup_source)

        print(f"Restore completed: {backup_filename} to {backup_source}")
