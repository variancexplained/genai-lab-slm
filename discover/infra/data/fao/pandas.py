#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fao/pandas.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Thursday December 19th 2024 03:54:23 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Pandas File Access Object Module"""
import pandas as pd

from discover.infra.data.dal.base import FAO
from discover.infra.data.fao.exception import FileIOException


# ------------------------------------------------------------------------------------------------ #
#                                      PARQUET                                                     #
# ------------------------------------------------------------------------------------------------ #
class PandasParquetFAO(FAO):
    """
    Pandas File Access Object (FAO) for parquet files."""

    def __init__(self, config: dict) -> None:
        self._config = config

    # -------------------------------------------------------------------------------------------- #
    def create(
        self, data: pd.DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """Creates and saves the data to the designated filepath

        Args:
            data (DataFrame): Data to write
            filepath (str): Path to file.
            overwrite (bool): Whether to ovewrite existing data. Default = False.
            **kwargs: Arbitrary keyword arguments.
        """
        super().create(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        try:
            data.to_parquet(filepath, **self._config.get("write_kwargs"))
        except Exception as e:
            msg = (
                f"Exception occurred while creating a Parquet file at {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def read(self, filepath, **kwargs) -> pd.DataFrame:
        """Reads data from the designated filepath

        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            pd.DataFrame Object
        """
        try:
            return pd.read_parquet(filepath, **self._config.get("read_kwargs"))
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}.File does not exist.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = (
                f"Exception occurred while reading a Parquet file from {filepath}.\n{e}"
            )
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                         CSV                                                      #
# ------------------------------------------------------------------------------------------------ #
class PandasCSVFAO(FAO):
    """
    Pandas File Access Object (FAO) for CSV files."""

    def __init__(self, config: dict) -> None:
        self._config = config

    # -------------------------------------------------------------------------------------------- #
    def create(
        self, data: pd.DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """Creates and saves the data to the designated filepath

        Args:
            data (DataFrame): Data to write
            filepath (str): Path to file.
            overwrite (bool): Whether to ovewrite existing data. Default = False.
            **kwargs: Arbitrary keyword arguments.
        """
        super().create(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        try:
            data.to_csv(filepath, **self._config.get("write_kwargs"))
        except Exception as e:
            msg = (
                f"Exception occurred while creating a Parquet file at {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def read(self, filepath, **kwargs) -> pd.DataFrame:
        """Reads data from the designated filepath

        Args:
            filepath (str): Path to file.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            pd.DataFrame Object
        """
        try:
            return pd.read_csv(filepath, **self._config.get("read_kwargs"))
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a csv file from {filepath}.File does not exist.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading a csv file from {filepath}.\n{e}"
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
