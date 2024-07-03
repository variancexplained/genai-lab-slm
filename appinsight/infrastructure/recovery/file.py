#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/recovery/file.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 10:47:30 pm                                                   #
# Modified   : Wednesday July 3rd 2024 01:20:40 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import tarfile
from datetime import datetime

import pandas as pd

from appinsight.infrastructure.recovery.base import Recovery


# ------------------------------------------------------------------------------------------------ #
class FileRecovery(Recovery):
    def __init__(self) -> None:
        super().__init__()

    def __len__(self) -> int:
        """Count the number of backups in the backup folder for the environment."""
        backup_folder = self.config["file"]["backup"]
        return len(os.listdir(backup_folder))

    def show_backups(self) -> pd.DataFrame:
        """Show a DataFrame with backup filename, size, and date created."""
        backup_folder = self.config["file"]["backup"]
        backups = []
        for file in os.listdir(backup_folder):
            file_path = os.path.join(backup_folder, file)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                created_time = datetime.fromtimestamp(os.path.getctime(file_path))
                backups.append(
                    {"Filename": file, "Size": file_size, "Created": created_time}
                )

        df = pd.DataFrame(backups)
        return df

    def backup(self, location: str = None, name: str = None) -> None:
        """Executes a backup of a folder to a compressed named file.

        Args:
            location (str, optional): The folder to be backed up. Defaults to the default location in config.
            name (str, optional): A name for the backup. Defaults to current timestamp.

        Raises:
            FileNotFoundError: If the specified location does not exist.
        """
        # Ascertain and validate location
        location = location or self.config["file"]["location"]
        if not os.path.exists(location):
            raise FileNotFoundError(
                f"The specified location '{location}' does not exist."
            )

        # Backup name is the designated name or the basename for the location.
        backup_name = name or os.path.basename(os.path.normpath(location))

        # Backup folder from config, make sure the directory exists
        backup_folder = self.config["file"]["backup"]
        os.makedirs(backup_folder, exist_ok=True)

        # Backup filename is a concatenation of backup_name and current datetime.
        backup_filename = self._generate_backup_filename(backup_name)
        backup_path = os.path.join(backup_folder, backup_filename)

        with tarfile.open(backup_path, "w:gz") as tar:
            tar.add(location, arcname=os.path.basename(location))

        print(f"Backup completed: {backup_filename}")

    def restore(
        self,
        filename: str,
        location: str = None,
        force: bool = False,
    ) -> None:
        """Executes a restore of a folder from a tar.gz file.

        Args:
            filename (str): The name of the backup file in the restore folder.
            location (str, optional): The location to which the asset will be restored.
                Defaults to the default restore location in config.
            force (bool, optional): If True, overwrite existing files. Defaults to False.

        Raises:
            FileNotFoundError: If the specified backup file does not exist.
        """
        location = location or self.config["file"]["restore"]

        backup_filepath = os.path.join(self.config["file"]["backup"], filename)
        if not os.path.exists(backup_filepath):
            raise FileNotFoundError(f"Error: Backup file '{filename}' not found.")

        # Check if force is False and location is not empty
        if not force and os.path.exists(location) and os.listdir(location):
            confirm = (
                input(f"Files already exist in '{location}'. Overwrite? (yes/no): ")
                .strip()
                .lower()
            )
            if confirm != "yes":
                print("Restore aborted.")
                return

        with tarfile.open(backup_filepath, "r:gz") as tar:
            tar.extractall(path=location)

        print(f"Restore completed: {filename} to {location}")

    def _generate_backup_filename(self, backup_name: str) -> str:
        """Generate a backup filename with the current timestamp.

        Args:
            backup_name (str): Name of the backup file sans datetime.

        Returns:
            str: The generated filename with timestamp.
        """
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return f"{backup_name}_{current_datetime}.tar.gz"
