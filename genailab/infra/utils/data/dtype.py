#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /genailab/infra/utils/data/dtype.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 04:23:05 am                                                 #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Cast Data Types Module"""
import logging
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType


# ------------------------------------------------------------------------------------------------ #
#                                      CAST PANDAS                                                 #
# ------------------------------------------------------------------------------------------------ #
class CastPandas:
    """Casts data types in a Pandas DataFrame."""

    def __init__(self) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def apply(self, data: pd.DataFrame, datatypes: Dict[str, type]) -> pd.DataFrame:
        """Applies the data types to the DataFrame.

        Args:
            data (pd.DataFrame): DataFrame to cast
            datatypes (Dict[str,type]): Mapping between columns and data types.

        Returns:
            pd.DataFrame: The DataFrame with columns cast to specified data types.
        """
        for column, dtype in datatypes.items():
            if column in data.columns:
                data[column] = data[column].astype(dtype)
            else:
                msg = f"Column {column} not found in DataFrame"
                self._logger.exception(msg)
                raise ValueError(msg)

        return data


# ------------------------------------------------------------------------------------------------ #
#                                      CAST PANDAS                                                 #
# ------------------------------------------------------------------------------------------------ #
class CastPySpark:
    """Casts data types in a PySpark DataFrame."""

    def __init__(self):
        """Initialize the CastPySpark instance."""
        self._logger = logging.getLogger(__name__)

    def apply(self, data: DataFrame, datatypes: Dict[str, type]) -> DataFrame:
        """Applies the specified data types to the DataFrame columns.

        Args:
            data (DataFrame): The DataFrame to cast.
            datatypes (Dict[str, type]): A mapping between column names and data types.

        Returns:
            DataFrame: The DataFrame with columns cast to the specified data types.
        """
        # Create the schema
        fields = [
            StructField(name, data_type(), True)
            for name, data_type in datatypes.items()
        ]
        structtype = StructType(fields)

        # Create a list of columns with the desired data types
        casted_columns = []
        for column in structtype:
            # Otherwise, cast to the target data type
            casted_col = col(column.name).cast(column.dataType).alias(column.name)
            casted_columns.append(casted_col)

        # Select the columns with the new data types
        casted_df = data.select(*casted_columns)

        return casted_df
