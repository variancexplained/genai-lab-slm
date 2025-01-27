#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/service/data/convert.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 27th 2025 01:55:39 pm                                                #
# Modified   : Monday January 27th 2025 03:25:49 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Conversion Module"""
import os
from typing import Union
import tempfile
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pandas as pd
from dependency_injector.wiring import Provide, inject
from genailab.container import GenAILabContainer

from genailab.core.dtypes import DFType
from genailab.infra.persist.repo.file.fao import FAO
from genailab.infra.utils.file.fileset import FileFormat

# ------------------------------------------------------------------------------------------------ #
class Converter:
    """Utility class for data conversions, particularly to pandas DataFrame."""

    @classmethod
    @inject
    def to_pandas(cls, df: DataFrame, fao: FAO = Provide[GenAILabContainer.io.fao], direct: bool = False) -> Union[pd.core.frame.DataFrame, pd.DataFrame]:
        """
        Converts a given DataFrame into a pandas DataFrame using a temporary Parquet file.

        This method writes the provided DataFrame to a temporary Parquet file and reads it back
        as a pandas DataFrame. The conversion leverages the provided FAO (File Access Object)
        for file operations.

        Args:
            df (DataFrame): The input DataFrame to be converted.
            fao (FAO, optional): An injected File Access Object for handling file I/O. Defaults
                to `GenAILabContainer.io.fao`.
            direct (bool): Whether to convert direct to pandas using pyspark with pyarrow optimization.

        Returns:
            Union[pd.core.frame.DataFrame, pd.DataFrame]: The resulting pandas DataFrame.

        Raises:
            Exception: If file creation, writing, or reading fails.

        Example:
            >>> converted_df = Converter.to_pandas(df=my_custom_dataframe)
            >>> print(type(converted_df))
            <class 'pandas.core.frame.DataFrame'>
        """
        if direct:
            # Doesn't work. PySpark, Pandas DateTime incompatibilities.
            df = df.withColumn("date", (col("date").cast("long") / 1000).cast("timestamp"))
            return df.toPandas()
        else:
            with tempfile.TemporaryDirectory() as td:
                filepath = os.path.join(td, "temp.parquet")
                fao.create(filepath=filepath, file_format=FileFormat.PARQUET, dataframe=df)
                return fao.read(filepath=filepath, dftype=DFType.PANDAS, file_format=FileFormat.PARQUET)
