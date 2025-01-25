#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/utils/file/copy.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 04:54:28 pm                                            #
# Modified   : Saturday January 25th 2025 04:44:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import shutil


# ------------------------------------------------------------------------------------------------ #
class Copy:
    """
    A utility class for copying Parquet and CSV files or directories, with validation and optional overwrite functionality.
    """

    def __call__(self, source: str, target: str, overwrite: bool = False):
        if os.path.isdir(source):
            self.directory(source=source, target=target, overwrite=overwrite)
        elif os.path.isfile(source):
            self.file(source=source, target=target, overwrite=overwrite)
        else:
            raise ValueError(f"Path {source} is not a directory or file.")

    def directory(self, source: str, target: str, overwrite: bool = False) -> None:
        """
        Copies a directory from the source to the target location.

        Args:
            source (str): The path to the source directory.
            target (str): The path to the target directory.
            overwrite (bool, optional): Whether to overwrite the target if it already exists. Defaults to False.

        Raises:
            FileExistsError: If the target exists and `overwrite` is False.
            ValueError: If the source and target are incompatible (e.g., one is a file and the other is a directory).
            Exception: If an unexpected error occurs during the copy operation.
        """
        self._validate(source=source, target=target, overwrite=overwrite)
        try:
            if os.path.exists(target):
                shutil.rmtree(target)
            shutil.copytree(source, target)
        except Exception as e:
            msg = f"Unexpected error in Copy.directory.\n{e}"
            raise Exception(msg)

    def file(self, source: str, target: str, overwrite: bool = False) -> None:
        """
        Copies a file from the source to the target location.

        Args:
            source (str): The path to the source file.
            target (str): The path to the target file.
            overwrite (bool, optional): Whether to overwrite the target if it already exists. Defaults to False.

        Raises:
            FileExistsError: If the target exists and `overwrite` is False.
            ValueError: If the source and target are incompatible (e.g., one is a file and the other is a directory).
            Exception: If an unexpected error occurs during the copy operation.
        """
        self._validate(source=source, target=target, overwrite=overwrite)
        try:
            if os.path.exists(target):
                os.remove(target)

            shutil.copyfile(source, target)
        except Exception as e:
            msg = f"Unexpected error in Copy.file.\n{e}"
            raise Exception(msg)

    def _validate(self, source: str, target: str, overwrite: bool = False) -> None:
        """
        Validates the parameters for the copy operation.

        Args:
            source (str): The path to the source file or directory.
            target (str): The path to the target file or directory.
            overwrite (bool, optional): Whether to overwrite the target if it already exists. Defaults to False.

        Raises:
            FileExistsError: If the target exists and `overwrite` is False.
            ValueError: If the source and target are incompatible (e.g., one is a file and the other is a directory).
        """
        if not overwrite and os.path.exists(target):
            msg = "Target file / directory already exists."
            raise FileExistsError(msg)
        elif (
            os.path.isfile(source)
            and os.path.isdir(target)
            or os.path.isdir(source)
            and os.path.isfile(target)
        ):
            msg = "Incompatible source and target. Both must be files or both must be directories"
            raise ValueError(msg)
