#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/analytics/profile.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday February 8th 2025 01:31:52 pm                                              #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFrame Profiler Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, isnan

from genailab.core.dtypes import DFType


# ------------------------------------------------------------------------------------------------ #
class Profiler(ABC):
    @abstractmethod
    def profile(self, dataframe: Union[pd.DataFrame, DataFrame]) -> pd.DataFrame:
        """Abstract method that defines the interface for dataset profiles.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The input DataFrame, either Pandas or PySpark.

        Returns:
            pd.DataFrame: A DataFrame containing profile information.
        """
        pass
# ------------------------------------------------------------------------------------------------ #
class PandasProfiler(Profiler):

    def profile(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Profiles a Pandas DataFrame.

        Calculates and returns a profile of the input Pandas DataFrame,
        including statistics for each column.

        Args:
            dataframe (pd.DataFrame): The input Pandas DataFrame to profile.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the profile information.
                           The profile includes the following columns for each
                           column in the input DataFrame:
                               - "Column": Name of the column.
                               - "DataType": Data type of the column.
                               - "Complete": Number of non-missing values.
                               - "Null": Number of missing values.
                               - "Completeness": Proportion of non-missing values.
                               - "Unique": Number of unique values.
                               - "Duplicate": Number of duplicate values.
                               - "Uniqueness": Proportion of unique values.
                               - "Size (Bytes)": Memory usage of the column (deep=True).
        """
        profile = pd.DataFrame()
        profile["Column"] = dataframe.columns
        profile["DataType"] = dataframe.dtypes.values
        profile["Complete"] = len(dataframe) - dataframe.isna().sum().values  # More efficient way to calculate complete count
        profile["Null"] = dataframe.isna().sum().values
        profile["Completeness"] = profile["Complete"] / len(dataframe)
        profile["Unique"] = dataframe.nunique().values
        profile["Duplicate"] = len(dataframe) - profile["Unique"]
        profile["Uniqueness"] = profile["Unique"] / len(dataframe)
        profile["Size (Bytes)"] = dataframe.memory_usage(deep=True, index=False).values
        return profile


# ------------------------------------------------------------------------------------------------ #
class PySparkProfiler(Profiler):

    def profile(self, dataframe: DataFrame) -> pd.DataFrame:
        """Profiles a PySpark DataFrame and returns the profile as a Pandas DataFrame.

        Calculates and returns a profile of the input PySpark DataFrame,
        including statistics for each column. The profile is returned as a
        Pandas DataFrame.

        Args:
            dataframe (DataFrame): The input PySpark DataFrame to profile.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the profile information.
                           The profile includes the following columns for each
                           column in the input DataFrame:
                               - "Column": Name of the column.
                               - "DataType": Data type of the column.
                               - "Complete": Number of non-missing values.
                               - "Null": Number of missing values.
                               - "Completeness": Proportion of non-missing values.
                               - "Unique": Number of unique values.
                               - "Duplicate": Number of duplicate values.
                               - "Uniqueness": Proportion of unique values.
                               - "Size (Bytes)": Memory usage of the column (approximated).
        """

        # Calculate counts and other statistics
        total_rows = dataframe.count()

        profile_data = []
        for column in dataframe.columns:
            # Data Type
            data_type = str(dataframe.schema[column].dataType)

            # Complete, Null, Completeness
            data_type = str(dataframe.schema[column].dataType)

            if "StringType" in data_type or "DateType" in data_type or "TimestampType" in data_type:  # strings or dates
                complete_count = dataframe.filter(col(column).isNotNull()).count()  # Check for NOT NULL
            elif "IntegerType" in data_type or "DoubleType" in data_type or "FloatType" in data_type or "LongType" in data_type: #numeric
                complete_count = dataframe.filter(~isnan(col(column))).count()  # Use isnan for numeric types
            elif "BooleanType" in data_type: #boolean
                complete_count = dataframe.filter(col(column).isNotNull()).count()
            else:  # Handle other types as needed (e.g., ArrayType, MapType)
                complete_count = dataframe.filter(col(column).isNotNull()).count()
            null_count = total_rows - complete_count
            completeness = complete_count / total_rows if total_rows > 0 else 0.0

            # Unique, Duplicate, Uniqueness
            unique_count = dataframe.select(countDistinct(col(column))).collect()[0][0]
            duplicate_count = total_rows - unique_count
            uniqueness = unique_count / total_rows if total_rows > 0 else 0.0

            # Size (Bytes) - Approximated for PySpark
            # More accurate method, requires Spark 3.0+ for storageLevel:
            #size_bytes = dataframe.select(length(column)).agg({"length(column)": "sum"}).collect()[0][0] if dataframe.select(length(column)).count() > 0 else 0
            # A simpler, but less accurate method (available in Spark 2.x):
            # size_bytes = dataframe.select(col(column)).rdd.flatMap(lambda x: x).map(len).sum() if dataframe.select(col(column)).count() > 0 else 0
            size_bytes = dataframe.rdd.map(lambda row: len(str(row))).sum()

            profile_data.append({
                "Column": column,
                "DataType": data_type,
                "Complete": complete_count,
                "Null": null_count,
                "Completeness": completeness,
                "Unique": unique_count,
                "Duplicate": duplicate_count,
                "Uniqueness": uniqueness,
                "Size (Bytes)": size_bytes
            })

        profile = pd.DataFrame(profile_data)
        return profile

# ------------------------------------------------------------------------------------------------ #
class ProfilerFactory:
    """A factory class for creating Profiler instances based on DataFrame type.

    This factory allows you to obtain the appropriate Profiler implementation
    (either PandasProfiler or PySparkProfiler) based on whether you're working
    with a Pandas DataFrame or a PySpark DataFrame.
    """
    __strategies = {
        DFType.PANDAS: PandasProfiler(),
        DFType.SPARK: PySparkProfiler(),
    }

    def get_profiler(self, dftype: DFType) -> Profiler:
        """Returns a Profiler instance based on the specified DataFrame type.

        Args:
            dftype (DFType): An enum representing the type of DataFrame (PANDAS or SPARK).

        Returns:
            Profiler: An instance of either PandasProfiler or PySparkProfiler, depending on the dftype.

        Raises:
            KeyError: If an invalid DFType is provided.  (Though the current implementation won't raise it, it's good practice to document).
        """
        return self.__strategies[dftype]