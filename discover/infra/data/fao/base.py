#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fao/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Thursday December 19th 2024 03:49:48 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Layer Base Module"""

import os
import shutil
from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import pyspark

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                               FILE ACCESS OBJECT                                                 #
# ------------------------------------------------------------------------------------------------ #
class FAO(ABC):
    """Abstract base class for File Access Object"""

    @abstractmethod
    def create(
        self, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """Creates and saves the data to the designated filepath

        Args:
            data (DataFrame): Data to write
            filepath (str): Path to file.
            overwrite (bool): Whether to ovewrite existing data. Default = False.
            **kwargs: Arbitrary keyword arguments.
        """
        if self.exists(filepath=filepath) and not overwrite:
            raise FileExistsError(f"File exists error at {filepath}.")

    @abstractmethod
    def read(self, filepath, **kwargs) -> DataFrame:
        """Reads data from the designated filepath
        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.
        """

    def exists(self, filepath: str) -> bool:
        """Returns True if the file exists, False otherwise.

        Args:
            filepath (str): Path to file.
        """
        return os.path.exists(filepath)

    def delete(self, filepath: str, ignore_errors: bool = False) -> None:
        """Deletes the file or directory at the designated filepath.

        If the file was not found, a FileNotFoundError will be raised, unless
        ignore_errors is True.

        Args:
            filepath (str): Path to file.
            ignore_errors (bool): Whether to ignore errors.
        """
        try:
            os.remove(filepath)
        except FileNotFoundError as e:
            if not ignore_errors:
                msg = f"File not found at {filepath}.\n{e}"
                raise (msg)
        except OSError:  # File is a directory
            shutil.rmtree(path=filepath, ignore_errors=ignore_errors)
        except Exception as e:  # Unknown error occurred
            if not ignore_errors:
                msg = f"Unknown error occurred while deleting file at {filepath}.\n{e}"
                raise (msg)
