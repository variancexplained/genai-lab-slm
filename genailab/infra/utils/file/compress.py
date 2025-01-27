#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /genailab/infra/utils/file/compress.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday April 28th 2024 12:15:31 am                                                  #
# Modified   : Sunday January 26th 2025 10:41:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Compression Module"""
import logging
import os
import tarfile
import zipfile


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

    def compress_file(self, filepath, tar_gz_path):
        """
        Compresses a single file into a .tar.gz file.

        Args:
            filepath (str): The file to be compressed.
            tar_gz_path (str): The path where the .tar.gz file will be created.

        Raises:
            tarfile.TarError: If there is an error during compression.
        """
        os.makedirs(os.path.dirname(tar_gz_path), exist_ok=True)
        try:
            with tarfile.open(tar_gz_path, "w:gz") as tar:
                tar.add(filepath, arcname=os.path.basename(filepath))
                print(f"Compressed {filepath} into {tar_gz_path}")
        except FileNotFoundError as e:
            self._logger.exception(
                f"Error compressing {filepath} into {tar_gz_path}: {e}"
            )
            raise
        except tarfile.TarError as e:
            self._logger.exception(
                f"Error compressing {filepath} into {tar_gz_path}: {e}"
            )
            raise


# ------------------------------------------------------------------------------------------------ #
#                                  ZIP FILE HANDLER                                                #
# ------------------------------------------------------------------------------------------------ #
class ZipFileHandler:
    def __init__(self):
        """Initialize the ZipFileHandler with no arguments."""
        pass

    def extract(self, zippath, extract_to):
        """
        Extracts the contents of the zip file to the specified directory.

        Args:
            zippath (str): The path to the zip file to extract.
            extract_to (str): The directory to extract the contents to.
        """
        with zipfile.ZipFile(zippath, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {zippath} to {extract_to}")

    def compress_file(self, filepath, zippath):
        """
        Compresses a single file into the zip file.

        Args:
            filepath (str): The path to the file to compress.
            zippath (str): The path to the zip file to create.
        """
        with zipfile.ZipFile(zippath, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.write(filepath, os.path.basename(filepath))
        print(f"Compressed {filepath} into {zippath}")

    def compress_directory(self, directory, zippath):
        """
        Compresses the contents of an entire directory into the zip file.

        Args:
            directory (str): The path to the directory to compress.
            zippath (str): The path to the zip file to create.
        """
        with zipfile.ZipFile(zippath, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            for root, _, files in os.walk(directory):
                for file in files:
                    filepath = os.path.join(root, file)
                    arcname = os.path.relpath(filepath, directory)
                    zip_ref.write(filepath, arcname)
        print(f"Compressed {directory} into {zippath}")
