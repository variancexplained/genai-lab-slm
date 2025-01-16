#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/file/info.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 25th 2024 10:50:08 pm                                            #
# Modified   : Thursday January 16th 2025 05:21:52 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File / Directory Stats Module"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional, Union

from pydantic.dataclasses import dataclass

from discover.core.dstruct import DataClass
from discover.core.file import FileFormat
from discover.infra.utils.data.format import format_size
from discover.infra.utils.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
d84mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
#                                  FILE INFO HELPER FUNCTIONS                                      #
# ------------------------------------------------------------------------------------------------ #
def get_file_size(self, filepath: str) -> int:
    """Gets the size of a file in bytes.

    Args:
        filepath (str): The path to the file.

    Returns:
        int: The size of the file in bytes.
    """
    return os.path.getsize(filepath)


# ------------------------------------------------------------------------------------------------ #
def get_directory_size(directory: str) -> int:
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
                total += get_directory_size(directory=entry.path)
    return total


# ------------------------------------------------------------------------------------------------ #
def get_directory_count(directory: str) -> int:
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
                total += get_directory_count(directory=entry.path)
    return total


# ------------------------------------------------------------------------------------------------ #
def get_size(path: str, in_bytes: bool = True) -> Union[int, str]:
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
        size = get_file_size(filepath=path)
    elif os.path.isdir(path):
        size = get_directory_size(directory=path)
    else:
        raise ValueError(f"The path {path} is not valid.")
    if in_bytes:
        return size
    else:
        return format_size(size_in_bytes=size)


# ------------------------------------------------------------------------------------------------ #
def isdir(filepath: str) -> bool:
    """Returns True if the filepath is a directory

    Args:
        filepath (str): The path to the file or directory.

    Returns:
        True if the filepath is a directory. False otherwise.
    """
    return os.path.isdir(filepath)


# ------------------------------------------------------------------------------------------------ #
def get_count(filepath: str) -> int:
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
        return get_directory_count(directory=filepath)
    else:
        raise ValueError(f"The filepath {filepath} is not valid.")


# ------------------------------------------------------------------------------------------------ #
def file_created(filepath: str) -> datetime:
    """Gets the creation time of the specified file.

    Args:
        filepath (str): The path to the file.

    Returns:
        datetime: The creation time of the file in HTTP date format.
    """
    stat_info = os.stat(filepath)
    return datetime.fromtimestamp(stat_info.st_ctime)


# ------------------------------------------------------------------------------------------------ #
def file_last_accessed(filepath: str) -> datetime:
    """Gets the last access time of the specified file.

    Args:
        filepath (str): The path to the file.

    Returns:
        datetime: The last access time of the file in HTTP date format.
    """
    stat_info = os.stat(filepath)
    return datetime.fromtimestamp(stat_info.st_atime)


# ------------------------------------------------------------------------------------------------ #
def file_last_modified(filepath: str) -> datetime:
    """Gets the last modified time of the specified file.

    Args:
        filepath (str): The path to the file.

    Returns:
        datetime: The last modified time of the file in HTTP date format.
    """
    stat_info = os.stat(filepath)
    return datetime.fromtimestamp(stat_info.st_mtime)


# ------------------------------------------------------------------------------------------------ #
#                                      FILE METADATA                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class FileMeta(DataClass):
    """Encapsulates File level metadata."""

    path: str
    name: str
    format: FileFormat
    isdir: Optional[bool] = None
    file_count: Optional[int] = None
    created: Optional[datetime] = None
    accessed: Optional[datetime] = None
    modified: Optional[datetime] = None
    size: Optional[int] = None

    @classmethod
    def create(cls, path: str, file_format: FileFormat) -> FileMeta:
        if os.path.exists(path):
            return cls(
                path=path,
                name=os.path.basename(path),
                format=file_format,
                isdir=isdir(filepath=path),
                file_count=get_count(filepath=path),
                created=file_created(filepath=path),
                accessed=file_last_accessed(filepath=path),
                modified=file_last_modified(filepath=path),
                size=get_size(path=path, in_bytes=True),
            )
        else:
            return cls(path=path, name=os.path.basename(path), format=file_format)


# ------------------------------------------------------------------------------------------------ #
#                            PARQUET FILE DETECTOR (IN-PROGRESS)                                   #
# ------------------------------------------------------------------------------------------------ #
class ParquetFileDetector:
    """
    Detects whether a given path is a Parquet file or a directory containing Parquet files.

    Methods:
        is_parquet(path: str) -> bool:
            Determines if the given path is a Parquet file or a directory containing Parquet files.

        is_parquet_file(filepath: str) -> bool:
            Checks if a file is a Parquet file using its magic number.

        is_parquet_directory(directory: str) -> bool:
            Recursively checks a directory and its subdirectories for Parquet files,
            quitting early once a valid file is found.

    Args:
        None
    """

    def is_parquet(self, path: str) -> bool:
        """
        Determines if the given path is a Parquet file or a directory containing Parquet files.

        Args:
            path (str): The path to a file or directory to check.

        Returns:
            bool: True if the path is a Parquet file or a directory containing Parquet files, False otherwise.
        """
        if os.path.isfile(path):
            return self.is_parquet_file(filepath=path)
        else:
            return self.is_parquet_directory(directory=path)

    def is_parquet_file(self, filepath: str) -> bool:
        """
        Checks if a file is a Parquet file using its magic number.

        Args:
            filepath (str): The path to the file to check.

        Returns:
            bool: True if the file is a valid Parquet file, False otherwise.
        """
        try:
            with open(filepath, "rb") as f:
                # Check the first 4 bytes (header)
                magic_start = f.read(4)
                # Check the last 4 bytes (footer)
                f.seek(-4, os.SEEK_END)
                magic_end = f.read(4)
                return magic_start == b"PAR1" and magic_end == b"PAR1"
        except Exception:
            return False

    def is_parquet_directory(self, directory: str) -> bool:
        """
        Recursively checks a directory and its subdirectories for Parquet files,
        quitting early once a valid file is found.

        Args:
            directory (str): The path to the directory to check.

        Returns:
            bool: True if the directory contains at least one valid Parquet file, False otherwise.
        """
        try:
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    if self.is_parquet_file(filepath=file_path):
                        return True  # Early quit: found a valid Parquet file
            return False  # No valid Parquet files found
        except Exception as e:
            print(f"Error while checking directory {directory}: {e}")
            return False


# ------------------------------------------------------------------------------------------------ #
#                              FILE TYPE DETECTOR    (IN-PROGRESS)                                 #
# ------------------------------------------------------------------------------------------------ #
class FileTypeDetector:
    """A class to detect the file type based on magic numbers, including handling directories."""

    MAGIC_NUMBERS = {
        "Parquet": (b"PAR1", None),  # Footer-based detection
        "pickle": (b"\x80\x04", None),  # Pickle protocol magic bytes
        "Feather": (b"FEA1", None),  # Feather magic number
        "HDF5": (b"\x89HDF", None),  # HDF5 magic number
        # CSV doesn't have a magic number; handle based on content characteristics
        "csv": None,
    }

    def __init__(self) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_file_type(self, path: str) -> str:
        """Detect the file type using magic numbers, including handling directories.

        Args:
            path (str): Path to the file or directory.

        Returns:
            str: The detected file type or 'Unknown' if no match is found.

        Raises:
            FileNotFoundError: If the path does not exist.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"The path {path} does not exist.")

        if os.path.isdir(path):
            return self._detect_directory_type(path)

        return self._detect_file_type(path)

    def _detect_file_type(self, filepath: str) -> str:
        """Detect file type for a single file."""
        try:
            with open(filepath, "rb") as file:
                header = file.read(8)  # Read enough bytes to check multiple types

                # Check footer for formats like Parquet
                file.seek(-4, os.SEEK_END)
                footer = file.read(4)

                for file_type, magic in self.MAGIC_NUMBERS.items():
                    if not magic:  # Special handling for CSV
                        continue
                    header_magic, footer_magic = magic
                    if (header_magic and header.startswith(header_magic)) or (
                        footer_magic and footer == footer_magic
                    ):
                        return file_type

                # CSV fallback: check for text-like content
                file.seek(0)
                if self._is_csv(file):
                    return "csv"

        except Exception:
            return self._fallback(filepath=filepath)

        return self._fallback(filepath=filepath)

    def _detect_directory_type(self, directory_path: str) -> str:
        """Detect file type for a directory based on its contents."""
        try:
            # List all files in the directory
            files = [
                os.path.join(directory_path, f)
                for f in os.listdir(directory_path)
                if os.path.isfile(os.path.join(directory_path, f))
            ]

            # Check the type of the first valid file
            for file in files:
                file_type = self._detect_file_type(file)
                if file_type != "Unknown":
                    return file_type

            return self._fallback(filepath=directory_path)

        except Exception:
            return self._fallback(filepath=directory_path)

    def _is_csv(self, file) -> bool:
        """Fallback for detecting CSV files based on plain text."""
        try:
            sample = file.read(1024).decode("utf-8", errors="ignore")
            return "," in sample or "\n" in sample  # Basic CSV characteristics
        except Exception:
            return False

    def _fallback(self, filepath: str) -> str:
        """Returns the fallback if unable to determine filetype using magic number method"""
        if "parquet" in filepath:
            msg = "Based on the filename, we are inferring a `parquet` file."
            self._logger.debug(msg)
            return "parquet"
        elif "csv" in filepath:
            msg = "Based on the filename, we are inferring a `csv` file."
            self._logger.debug(msg)
            return "csv"
        else:
            msg = "Unable to determine file type. We're assuming parquet."
            self._logger.debug(msg)
            return "parquet"
