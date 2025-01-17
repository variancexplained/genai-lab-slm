#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/data/info.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 16th 2025 02:49:20 pm                                              #
# Modified   : Thursday January 16th 2025 08:07:29 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFrame Info Module"""
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampNTZType,
)


# ------------------------------------------------------------------------------------------------ #
class DataFrameInfo:

    def pandas_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generates a DataFrame with detailed information about each column.

        This method returns a DataFrame containing detailed information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and memory usage in bytes.

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        info = pd.DataFrame()
        info["Column"] = df.columns
        info["DataType"] = df.dtypes.values
        info["Complete"] = df.count().values
        info["Null"] = df.isna().sum().values
        info["Completeness"] = info["Complete"] / len(df)
        info["Unique"] = df.nunique().values
        info["Duplicate"] = len(df) - info["Unique"]
        info["Uniqueness"] = info["Unique"] / len(df)
        info["Size (Bytes)"] = df.memory_usage(deep=True, index=False).values
        return info

    def spark_info(self, df: DataFrame) -> pd.DataFrame:
        """Generates a DataFrame with detailed information about each column for a Spark DataFrame.

        This method returns a DataFrame containing detailed information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and estimated memory usage in bytes.

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        total_rows = df.count()

        info = []

        for column in df.columns:
            data_type = df.schema[column].dataType.simpleString()

            complete_count = df.filter(col(column).isNotNull()).count()
            null_count = total_rows - complete_count
            unique_count = df.select(column).distinct().count()
            duplicate_count = total_rows - unique_count

            completeness = complete_count / total_rows
            uniqueness = unique_count / total_rows

            size_estimate = self._estimate_column_size(df=df, column=column)

            info.append(
                {
                    "Column": column,
                    "DataType": data_type,
                    "Complete": complete_count,
                    "Null": null_count,
                    "Completeness": completeness,
                    "Unique": unique_count,
                    "Duplicate": duplicate_count,
                    "Uniqueness": uniqueness,
                    "Size (Bytes)": size_estimate,
                }
            )

        return pd.DataFrame(info)

    def _estimate_column_size(self, df: DataFrame, column: str) -> int:
        """Estimates the in-memory size of a Spark DataFrame column in bytes.

        Args:
            df (DataFrame): A spark DataFrame object.
            column (str): The name of the column to estimate size for.

        Returns:
            int: The estimated size of the column in bytes.
        """

        data_type = df.schema[column].dataType

        if isinstance(data_type, StringType):
            # Estimate size of StringType by calculating the average string length
            avg_length = df.select(_sum(length(col(column)))).first()[0] or 0
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * avg_length
        elif isinstance(data_type, BinaryType):
            # Estimate size of BinaryType by calculating the average binary length
            avg_length = df.select(_sum(length(col(column)))).first()[0] or 0
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * avg_length
        elif isinstance(data_type, ShortType):
            # Short type (smallint) types have fixed sizes 2.
            size_per_value = 2
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, TimestampNTZType):
            # TimestampNTZType is eight bytes
            size_per_value = 8
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, (IntegerType, LongType)):
            # Integer and Long types have fixed sizes (4 and 8 bytes respectively)
            size_per_value = 4 if isinstance(data_type, IntegerType) else 8
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, DoubleType):
            # Double type is 8 bytes per value
            size_per_value = 8
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, BooleanType):
            # Boolean type is 1 byte per value
            size_per_value = 1
            num_non_nulls = df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        else:
            # For other types, further implementation may be needed
            size_estimate = 0

        return size_estimate
