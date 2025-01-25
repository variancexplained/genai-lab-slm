#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/utils/file/fileset.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 25th 2024 10:50:08 pm                                            #
# Modified   : Saturday January 25th 2025 04:40:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Fileset Module"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from enum import Enum
from typing import Optional, Union

from genailabslm.core.dstruct import DataClass
from genailabslm.infra.utils.data.format import format_size
from genailabslm.infra.utils.date_time.format import ThirdDateFormatter
from pydantic.dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
d84mtr = ThirdDateFormatter()
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(f"{__name__}")


# ------------------------------------------------------------------------------------------------ #
#                                     FILE FORMATS                                                 #
# ------------------------------------------------------------------------------------------------ #
class FileFormat(Enum):
    CSV = ("csv", ".csv")
    PARQUET = ("parquet", ".parquet")
    PICKLE = ("pickle", ".pkl")

    @classmethod
    def from_value(cls, value) -> FileFormat:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, ext: str):
        """Factory method that creates an Enum member.

        Args:
            name (str): The id for the enum member, i.e  PICKLE
            ext (str): The file extension for the file format, i.e. `.pkl

        """
        obj = object.__new__(cls)
        obj._value_ = name
        obj.ext = ext
        return obj


# ------------------------------------------------------------------------------------------------ #
#                                        FILESET                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class FileSet(DataClass):
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


# ------------------------------------------------------------------------------------------------ #
#                                        FILEATTR                                                  #
# ------------------------------------------------------------------------------------------------ #
class FileAttr:

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def get_fileset(filepath: str, file_format: FileFormat) -> FileSet:
        """Returns a FileSet object for a designated filepath

        Args:
            filepath (str): The path to the file.

        Returns:
            FileSet: Object encapsulating file attributes and metadata.

        """
        try:
            return FileSet(
                path=filepath,
                name=os.path.basename(filepath),
                format=file_format,
                isdir=FileAttr.isdir(filepath=filepath),
                file_count=FileAttr.get_count(filepath=filepath),
                created=FileAttr.file_created(filepath=filepath),
                accessed=FileAttr.file_last_accessed(filepath=filepath),
                modified=FileAttr.file_last_modified(filepath=filepath),
                size=FileAttr.get_size(path=filepath, in_bytes=True),
            )
        except FileNotFoundError as e:
            logger.warning(f"File not found at {filepath}.\n{e}")
            return FileSet(
                path=filepath,
                name=os.path.basename(filepath),
                format=file_format,
            )
        except Exception as e:
            msg = f"Unknown exception raised in the FileAttr class for file path {filepath}.\n{e}"
            logger.warning(msg)
            return FileSet(
                path=filepath,
                name=os.path.basename(filepath),
                format=file_format,
            )

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def get_file_size(filepath: str) -> int:
        """Gets the size of a file in bytes.

        Args:
            filepath (str): The path to the file.

        Returns:
            int: The size of the file in bytes.
        """
        return os.path.getsize(filepath)

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
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
                    total += FileAttr.get_directory_size(directory=entry.path)
        return total

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
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
                    total += FileAttr.get_directory_count(directory=entry.path)
        return total

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
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
            size = FileAttr.get_file_size(filepath=path)
        elif os.path.isdir(path):
            size = FileAttr.get_directory_size(directory=path)
        else:
            raise ValueError(f"The path {path} is not valid.")
        if in_bytes:
            return size
        else:
            return format_size(size_in_bytes=size)

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def isdir(filepath: str) -> bool:
        """Returns True if the filepath is a directory

        Args:
            filepath (str): The path to the file or directory.

        Returns:
            True if the filepath is a directory. False otherwise.
        """
        return os.path.isdir(filepath)

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
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
            return FileAttr.get_directory_count(directory=filepath)
        else:
            raise ValueError(f"The filepath {filepath} is not valid.")

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def file_created(filepath: str) -> datetime:
        """Gets the creation time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The creation time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        return datetime.fromtimestamp(stat_info.st_ctime)

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def file_last_accessed(filepath: str) -> datetime:
        """Gets the last access time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The last access time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        return datetime.fromtimestamp(stat_info.st_atime)

    # -------------------------------------------------------------------------------------------- #
    @staticmethod
    def file_last_modified(filepath: str) -> datetime:
        """Gets the last modified time of the specified file.

        Args:
            filepath (str): The path to the file.

        Returns:
            datetime: The last modified time of the file in HTTP date format.
        """
        stat_info = os.stat(filepath)
        return datetime.fromtimestamp(stat_info.st_mtime)
