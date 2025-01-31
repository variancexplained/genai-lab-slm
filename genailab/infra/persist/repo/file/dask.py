#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/persist/repo/file/dask.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Friday January 31st 2025 03:49:49 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dask File Access Object Module"""
from __future__ import annotations

import logging

import dask.dataframe as dd

from genailab.infra.exception.file import FileIOException
from genailab.infra.persist.repo.file.base import DataFrameReader as BaseDataFrameReader
from genailab.infra.persist.repo.file.base import DataFrameWriter as BaseDataFrameWriter


# ------------------------------------------------------------------------------------------------ #
#                                    DATAFRAME READERS                                             #
# ------------------------------------------------------------------------------------------------ #
class DaskDataFrameParquetReader(BaseDataFrameReader):
    """A reader class for loading dataframe into Dask DataFrames from parquet files."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def read(self, filepath: str, **kwargs) -> dd.DataFrame:
        """
        Reads a Parquet file into a Dask DataFrame.

        Args:
            filepath (str): The path to the Parquet file.
            **kwargs: Additional keyword arguments passed to `dask.read_parquet`.

        Returns:
            pd.DataFrame: A Dask DataFrame containing the dataframe from the Parquet file.

        Raises:
            FileNotFoundError: If the specified Parquet file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        try:
            df = dd.read_parquet(path=filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} read from {filepath}"
            self._logger.debug(msg)
            return df
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while reading a Parquet file from {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                  DATAFRAME WRITERS                                               #
# ------------------------------------------------------------------------------------------------ #
class DaskDataFrameParquetWriter(BaseDataFrameWriter):
    """Writes a dask DataFrame to a parquet file."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def write(
        self,
        dataframe: dd.DataFrame,
        filepath: str,
    ) -> None:
        """
        Writes the dataframe to a Parquet file at the designated filepath.

        Args:
            dataframe (pd.DataFrame): The Dask DataFrame to write to the Parquet file.
            filepath (str): The path where the Parquet file will be saved.
            **kwargs: Additional keyword arguments passed to `dask.DataFrame.to_parquet`.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        self.validate_write(filepath, **self._kwargs)
        try:
            dataframe.to_parquet(path=filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} wrote to {filepath}"
            self._logger.debug(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while creating a Parquet file at {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e
