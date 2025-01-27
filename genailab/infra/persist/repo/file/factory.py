#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/persist/repo/file/factory.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 26th 2024 02:21:28 pm                                             #
# Modified   : Sunday January 26th 2025 10:38:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame IO Factory Module"""
import logging

from genailab.core.dtypes import DFType
from genailab.infra.persist.repo.file.base import (
    DataFrameReader,
    DataFrameWriter,
    IOFactory,
)
from genailab.infra.persist.repo.file.pandas import (
    PandasDataFrameCSVReader,
    PandasDataFrameCSVWriter,
    PandasDataFrameParquetReader,
    PandasDataFrameParquetWriter,
)
from genailab.infra.persist.repo.file.spark import (
    SparkDataFrameCSVReader,
    SparkDataFrameCSVWriter,
    SparkDataFrameParquetReader,
    SparkDataFrameParquetWriter,
)
from genailab.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
class DataFrameIOFactory(IOFactory):
    """Factory that produces DataFrame IO objects for different dftypes and file formats.

    Args:
        config (dict): A nested dictionary containing `read_kwargs` and `write_kwargs`
                       for each `dftype` and `file_format` combination.
    """

    __reader_map = {
        "pandas_csv": PandasDataFrameCSVReader,
        "pandas_parquet": PandasDataFrameParquetReader,
        "spark_csv": SparkDataFrameCSVReader,
        "spark_parquet": SparkDataFrameParquetReader,
        "sparknlp_parquet": SparkDataFrameParquetReader,
    }
    __writer_map = {
        "pandas_csv": PandasDataFrameCSVWriter,
        "pandas_parquet": PandasDataFrameParquetWriter,
        "spark_csv": SparkDataFrameCSVWriter,
        "spark_parquet": SparkDataFrameParquetWriter,
        "sparknlp_parquet": SparkDataFrameParquetWriter,
    }

    def __init__(self, config: dict) -> None:
        self._config = config

    def get_reader(
        self, dftype: DFType, file_format: FileFormat = FileFormat.PARQUET
    ) -> DataFrameReader:
        """
        Returns a dataframe reader for the specified dataframe structure and file format.

        Args:
            dftype (DFType): The data structure type (e.g., 'pandas', 'spark').
            file_format (FileFormat): The file format (e.g., 'csv', 'parquet').

        Returns:
            DataFrameReader: An instance of the appropriate reader class.

        Raises:
            ValueError: If the combination of `dftype` and `file_format` is unsupported.
        """
        key = self._format_key(dftype=dftype, file_format=file_format)
        try:
            logging.debug(f"Requesting a {key} reader from the DataFrameIOFactory")
            reader = self.__reader_map[key]
            kwargs = self._config[dftype.value][file_format.value]["read_kwargs"]
            return reader(kwargs)
        except KeyError as e:
            raise ValueError(
                f"Failed to create reader for dftype={dftype} and file_format={file_format}: {e}. "
                f"Supported dftypes are {list(self._config.keys())} with valid file formats "
                f"{list(self.__reader_map.keys())}."
            )

    def get_writer(
        self, dftype: DFType, file_format: FileFormat = FileFormat.PARQUET
    ) -> DataFrameWriter:
        """
        Returns a dataframe writer for the specified dataframe structure and file format.

        Args:
            dftype (DFType): The data structure type (e.g., 'pandas', 'spark').
            file_format (FileFormat): The file format (e.g., 'csv', 'parquet').

        Returns:
            DataFrameWriter: An instance of the appropriate writer class.

        Raises:
            ValueError: If the combination of `dftype` and `file_format` is unsupported.
        """
        key = self._format_key(dftype=dftype, file_format=file_format)
        try:
            logging.debug(f"Requesting a {key} writer from the DataFrameIOFactory")
            writer = self.__writer_map[key]
            kwargs = self._config[dftype.value][file_format.value]["write_kwargs"]
            return writer(kwargs)
        except KeyError as e:
            raise ValueError(
                f"Failed to create writer for dftype={dftype} and file_format={file_format}: {e}. "
                f"Supported dftypes are {list(self._config.keys())} with valid file formats "
                f"{list(self.__writer_map.keys())}."
            )

    def _format_key(
        self,
        dftype: DFType,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> str:
        """Generates a key for the reader or writer map."""
        return f"{dftype.value}_{file_format.value}"
