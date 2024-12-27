#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/file/stats.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 25th 2024 10:50:08 pm                                            #
# Modified   : Friday December 27th 2024 08:52:08 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File / Directory Stats Module"""
import os
from datetime import datetime
from typing import Union

from discover.infra.utils.data.format import format_size
from discover.infra.utils.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
d84mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
class FileStats:
    """Utility class for retrieving statistics and metadata about files and directories.

    This class provides methods to calculate the size, creation time, last access time,
    last modified time, and file/directory counts. It supports both files and directories,
    handling recursive operations for directories when necessary.
    """

    @classmethod
    def get_count(cls, filepath: str) -> int:
        """Gets the count of items in the specified file or directory.

        Args:
            filepath (str): The path to the file or directory.

        Returns:
            int: 1 if the path is a file, or the total number of files and directories
            in the specified directory (recursively).

        Raises:
            ValueError: If the filepath is neither a file nor a directory.
        """
        if os.path.isfile(filepath):
            return 1
        elif os.path.isdir(filepath):
            return cls._get_directory_count(directory=filepath)
        else:
            raise ValueError(f"The filepath {filepath} is not valid.")

    @classmethod
    def file_created(cls, filepath: str) -> datetime:
        """Gets the creation time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The creation time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        ctime = datetime.fromtimestamp(stat_info.st_ctime)
        return d84mtr.to_HTTP_format(dt=ctime)

    @classmethod
    def file_last_accessed(cls, filepath: str) -> datetime:
        """Gets the last access time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The last access time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        atime = datetime.fromtimestamp(stat_info.st_atime)
        return d84mtr.to_HTTP_format(dt=atime)

    @classmethod
    def file_last_modified(cls, filepath: str) -> datetime:
        """Gets the last modified time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The last modified time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        mtime = datetime.fromtimestamp(stat_info.st_mtime)
        return d84mtr.to_HTTP_format(dt=mtime)

    @classmethod
    def get_size(cls, path: str, in_bytes: bool = True) -> Union[int, str]:
        """Gets the size of the specified file or directory in a human-readable format.

        Args:
            path (str): The path to the file or directory.
            in_bytes (bool): Whether to return size in bytes as integer.

        Returns:
            Union[int,str]: The size of the file or directory in a formatted string (e.g., '1.23 MB')
                if in_bytes is False, otherwise an integer is returned.

        Raises:
            ValueError: If the path is neither a file nor a directory.
        """
        if os.path.isfile(path):
            size = cls._get_file_size(filepath=path)
        elif os.path.isdir(path):
            size = cls._get_directory_size(directory=path)
        else:
            raise ValueError(f"The path {path} is not valid.")
        if in_bytes:
            return size
        else:
            return format_size(size_in_bytes=size)

    @classmethod
    def _get_file_size(cls, filepath: str) -> int:
        """Gets the size of a file in bytes.

        Args:
            filepath (str): The path to the file.

        Returns:
            int: The size of the file in bytes.
        """
        return os.path.getsize(filepath)

    @classmethod
    def _get_directory_size(cls, directory: str) -> int:
        """Recursively calculates the total size of a directory in bytes.

        Args:
            directory (str): The path to the directory.

        Returns:
            int: The total size of the directory in bytes.
        """
        total = 0
        with os.scandir(directory) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += cls._get_directory_size(directory=entry.path)
        return total

    @classmethod
    def _get_directory_count(cls, directory: str) -> int:
        """Recursively counts the total number of files and directories in a directory.

        Args:
            directory (str): The path to the directory.

        Returns:
            int: The total count of files and directories in the specified directory.
        """
        total = 0
        with os.scandir(directory) as it:
            for entry in it:
                if entry.is_file():
                    total += 1
                elif entry.is_dir():
                    total += cls._get_directory_count(directory=entry.path)
        return total
