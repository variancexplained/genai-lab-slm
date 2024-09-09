#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/shared/recovery/file.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 10:47:30 pm                                                   #
# Modified   : Tuesday August 27th 2024 10:54:13 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Recovery Module"""
import os
import tarfile

from appvocai.shared.config.recovery import FileRecoveryConfig
from appvocai.shared.dependency.container import AppInsightContainer
from appvocai.shared.persist.database.base import Database
from appvocai.shared.recovery.base import Recovery
from dependency_injector.wiring import Provide, inject


# ------------------------------------------------------------------------------------------------ #
class FileRecovery(Recovery):

    __BACKUP_TYPE = "File"

    @inject
    def __init__(
        self,
        config_cls: type[FileRecoveryConfig] = FileRecoveryConfig,
        db: Database = Provide[AppInsightContainer.db.sqlite],
    ) -> None:
        super().__init__(config_cls, db)

    def backup(self, backup_source: str = None, name: str = None) -> None:
        """Executes a backup of a folder to a compressed named file.

        Args:
            backup_source (str, optional): The folder to be backed up. Defaults to the default source in config.
            name (str, optional): A name for the backup. Defaults to the default backup_source
                and a timestamp.

        Raises:
            FileNotFoundError: If the specified backup_source does not exist.
        """
        # Ascertain and validate backup_source
        backup_source = backup_source or self._config.get_default_backup_source()
        if not os.path.exists(backup_source):
            raise FileNotFoundError(
                f"The specified backup_source '{backup_source}' does not exist."
            )
        self.logger.debug(f"Source of backup is {backup_source}")

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

        # Initialize metrics
        file_count = 0
        dir_count = 0
        total_size_uncompressed = 0

        # Create the backup archive
        with tarfile.open(backup_filepath, "w:gz") as tar:
            for root, dirs, files in os.walk(backup_source):
                for file in files:
                    file_path = os.path.join(root, file)
                    tar.add(
                        file_path, arcname=os.path.relpath(file_path, backup_source)
                    )

                    # Increment counts
                    file_count += 1
                    total_size_uncompressed += os.path.getsize(file_path)

                for dir in dirs:
                    dir_count += 1

        # Capture compressed size.
        total_size_compressed = os.path.getsize(backup_filepath)
        # Log the backup to the backup database
        self.log_backup(
            backup_source=backup_source,
            backup_filename=backup_filename,
            file_count=file_count,
            dir_count=dir_count,
            size_uncompressed=total_size_uncompressed,
            size_compressed=total_size_compressed,
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
        # Obtain the original backup_source for the backup from the database.
        backup_source = self._get_backup_source(backup_filename=backup_filename)
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

    def _get_backup_source(self, backup_filename: str) -> str:
        """Obtains the backup_source of the designated backup"""
        query = """SELECT backup_source FROM backup WHERE backup_filename = :backup_filename;"""
        params = {"backup_filename": backup_filename}
        result = self._db.query(query=query, params=params)
        return result.iloc[0]["backup_source"]
