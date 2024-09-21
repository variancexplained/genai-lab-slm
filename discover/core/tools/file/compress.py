#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/utils/file_utils/compress.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday April 28th 2024 12:15:31 am                                                  #
# Modified   : Wednesday September 11th 2024 02:43:29 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Compression Module"""
import logging
import os
import tarfile


# ------------------------------------------------------------------------------------------------ #
#                                  TAR GZ HANDLER                                                  #
# ------------------------------------------------------------------------------------------------ #
class TarGzHandler:
    """
    A class to handle .tar.gz file operations such as extracting and compressing directories or single files.

    Example:
        # To extract a .tar.gz file:
        handler = TarGzHandler()
        handler.extract('/path/to/file.tar.gz', '/path/to/extract/directory')

        # To compress a directory into a .tar.gz file:
        handler.compress_directory('/path/to/directory', '/path/to/file.tar.gz')

        # To compress a single file into a .tar.gz file:
        handler.compress_file('/path/to/file', '/path/to/file.tar.gz')
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def extract(self, tar_gz_path, extract_dir):
        """
        Extracts the contents of a .tar.gz file to a specified directory.

        Args:
            tar_gz_path (str): The path to the .tar.gz file.
            extract_dir (str): The directory where the contents should be extracted.

        Raises:
            tarfile.TarError: If there is an error during extraction.
        """
        os.makedirs(extract_dir, exist_ok=True)
        try:
            with tarfile.open(tar_gz_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)
                print(f"Extracted {tar_gz_path} to {extract_dir}")
        except FileNotFoundError as e:
            self._logger.exception(f"Error extracting {tar_gz_path}: {e}")
            raise
        except tarfile.TarError as e:
            self._logger.exception(f"Error extracting {tar_gz_path}: {e}")
            raise

    def compress_directory(self, directory_path, tar_gz_path):
        """
        Compresses a directory into a .tar.gz file.

        Args:
            directory_path (str): The directory to be compressed.
            tar_gz_path (str): The path where the .tar.gz file will be created.

        Raises:
            tarfile.TarError: If there is an error during compression.
        """
        try:
            with tarfile.open(tar_gz_path, "w:gz") as tar:
                tar.add(directory_path, arcname=os.path.basename(directory_path))
                print(f"Compressed {directory_path} into {tar_gz_path}")
        except FileNotFoundError as e:
            self._logger.exception(
                f"Error compressing {directory_path} into {tar_gz_path}: {e}"
            )
            raise
        except tarfile.TarError as e:
            self._logger.exception(
                f"Error compressing {directory_path} into {tar_gz_path}: {e}"
            )
            raise

    def compress_file(self, file_path, tar_gz_path):
        """
        Compresses a single file into a .tar.gz file.

        Args:
            file_path (str): The file to be compressed.
            tar_gz_path (str): The path where the .tar.gz file will be created.

        Raises:
            tarfile.TarError: If there is an error during compression.
        """
        os.makedirs(os.path.dirname(tar_gz_path))
        try:
            with tarfile.open(tar_gz_path, "w:gz") as tar:
                tar.add(file_path, arcname=os.path.basename(file_path))
                print(f"Compressed {file_path} into {tar_gz_path}")
        except FileNotFoundError as e:
            self._logger.exception(
                f"Error compressing {file_path} into {tar_gz_path}: {e}"
            )
            raise
        except tarfile.TarError as e:
            self._logger.exception(
                f"Error compressing {file_path} into {tar_gz_path}: {e}"
            )
            raise
