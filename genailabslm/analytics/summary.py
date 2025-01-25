#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/analytics/summary.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 16th 2025 02:49:20 pm                                              #
# Modified   : Saturday January 25th 2025 04:41:12 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFramer Module"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Union

import numpy as np
import pandas as pd
from genailabslm.infra.utils.data.dataframe import (
    PySparkDataFrameMemoryFootprintEstimator,
)
from genailabslm.infra.utils.visual.print import Printer
from pandarallel import pandarallel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, length, max, mean, min
from pyspark.sql.functions import round as spark_round
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
from pyspark.testing.utils import assertSchemaEqual

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
#                                      DATAFRAMER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DatasetSummarizer(ABC):
    """Abstract base class for subclasses that provide dataset overview and summary information"""

    def __init__(self) -> None:
        # Cache
        self._df = None
        self._info = None
        self._summary = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def is_cache_valid(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]
    ):
        """Checks if the cached DataFrame is still valid for reuse in info and summary methods.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]): The DataFrame to compare with the cached DataFrame.

        Returns:
            bool: True if the cache is valid, False otherwise.
        """
        pass

    @abstractmethod
    def info(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]
    ) -> pd.DataFrame:
        """Returns a concise summary of the DataFrame, including column types, null values, and cardinality."""

    @abstractmethod
    def summarize(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]
    ) -> Dict[str, Union[str, int, float]]:
        """Prints a qualitative summary of variable counts, averages, frequencies and basic descriptive statistics."""


# ------------------------------------------------------------------------------------------------ #
#                                  PANDAS OVERVIEW                                                 #
# ------------------------------------------------------------------------------------------------ #
class PandasDatasetSummarizer(DatasetSummarizer):
    """Provides overview and summary information for Pandas DataFrames."""

    def __init__(self) -> None:
        super().__init__()

    def is_cache_valid(self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]):
        """Checks if the cached DataFrame is still valid for reuse in info and summary methods.

        Args:
            df (DataFrame): The DataFrame to compare with the cached DataFrame.

        Returns:
            bool: True if the cache is valid, False otherwise.
        """
        if self._df is None:
            # No cached DataFrame exists
            return False

        # Structural checks
        if df.shape != self._df.shape:
            return False
        if not df.columns.equals(self._df.columns):
            return False

        # Content validation: Check the sum of a key column
        key_column = "rating"
        if key_column not in df.columns or key_column not in self._df.columns:
            # If the key column doesn't exist, consider the cache invalid
            return False

        # Compare the sum of the key column
        self._logger.info(
            "Checking equality of the cached dataframe. This may take some time... "
        )
        current_sum = df[key_column].sum()
        cached_sum = self._df[key_column].sum()
        self._logger.info("Equality check complete")
        return current_sum == cached_sum

    def info(self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]) -> pd.DataFrame:
        """Method that provides structural information regarding the dataset and its columns.

        This method returns a DataFrame containing quantitative information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and memory usage in bytes

        First, it determines whether the dataframe has been cached. If so, it returns the cached result,
        otherwise, it returns a computed result.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame]): Dataframe to analyze

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        if self.is_cache_valid(df=df) and isinstance(
            self._info, (pd.DataFrame, pd.core.frame.DataFrame)
        ):
            return self._info
        else:
            self._df = df
            self._info = self._profile(df=df)
            return self._info

    def _profile(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]
    ) -> pd.DataFrame:
        """Computes the structural information regarding the dataset and its columns."""
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

    def summarize(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]
    ) -> Dict[str, Union[str, float, int]]:
        """Prints a summary of the app review dataset and returns a dictionary

        This method prints various counts, frequencies and summary statistics for  the dataframe,
        such as the number of reviews, apps, and reviewrs, as well rating and review length descriptive
        statistics.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame]): Dataframe to analyze

        """
        if self.is_cache_valid(df=df) and isinstance(self._summary, dict):
            pass
        else:
            self._df = df
            self._summary = self._summarize(df=df)

        title = "AppVoCAI Dataset Summary"
        printer.print_dict(title=title, data=self._summary)
        return self._summary

    def _summarize(
        self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]
    ) -> Dict[str, Union[str, float, int]]:
        """Prints a summary of the app review dataset and returns a dictionary

        This method prints various counts, frequencies and summary statistics for  the dataframe,
        such as the number of reviews, apps, and reviewrs, as well rating and review length descriptive
        statistics.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame]): Dataframe to analyze

        """
        n = df.shape[0]
        p = df.shape[1]
        n_auth = df["author"].nunique()
        n_auth_inf = df.loc[df["vote_count"] > 0]["author"].nunique()
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2)
        n_repeat_auth = int((df["author"].value_counts() > 1).sum())
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2)
        n_apps = df["app_id"].nunique()
        n_categories = df["category"].nunique()
        ave_reviews_per_app = round(n / n_apps, 2)

        review_lengths = df["content"].parallel_apply(lambda n: len(n.split()))
        min_review_length = np.min(review_lengths)
        max_review_length = np.max(review_lengths)
        avg_review_length = np.mean(review_lengths)

        mem = df.memory_usage(deep=True).sum().sum()
        dt_first = df["date"].min()
        dt_last = df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Number of Categories": n_categories,
            "Features": p,
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": round(avg_review_length, 2),
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        return d


# ------------------------------------------------------------------------------------------------ #
#                                  PYSPARK DATAFRAMER                                              #
# ------------------------------------------------------------------------------------------------ #
class PySparkDatasetSummarizer(DatasetSummarizer):
    """Provides overview and summary information for PySpark DataFrames."""

    def __init__(self) -> None:
        super().__init__()

    def is_cache_valid(self, df: DataFrame):
        """Checks if the cached DataFrame is still valid for reuse in info and summary methods.

        Args:
            df (DataFrame): The DataFrame to compare with the cached DataFrame.

        Returns:
            bool: True if the cache is valid, False otherwise.
        """
        if self._df is None:
            # No cached DataFrame exists
            return False

        # Structural checks
        if df.count() != self._df.count():
            return False
        if len(df.columns) != len(self._df.columns):
            return False
        if not assertSchemaEqual(df, self._df):
            return False

        # Content validation: Check the sum of a key column
        key_column = "rating"
        if key_column not in df.columns or key_column not in self._df.columns:
            # If the key column doesn't exist, consider the cache invalid
            return False

        # Compare the sum of the key column
        current_sum = df.agg({key_column: "sum"}).collect()[0][0]
        cached_sum = self._df.agg({key_column: "sum"}).collect()[0][0]
        return current_sum == cached_sum

    def info(self, df: DataFrame) -> pd.DataFrame:
        """Method that provides structural information regarding the dataset and its columns.

        This method returns a DataFrame containing quantitative information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and memory usage in bytes

        First, it determines whether the dataframe has been cached. If so, it returns the cached result,
        otherwise, it returns a computed result.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame]): Dataframe to analyze

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        if self.is_cache_valid(df=df) and isinstance(
            self._info, (pd.DataFrame, pd.core.frame.DataFrame)
        ):
            return self._info
        else:
            self._df = df
            self._info = self._profile(df=df)
            return self._info

    def _profile(self, df: DataFrame) -> pd.DataFrame:
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

    def summarize(self, df: DataFrame) -> Dict[str, Union[str, float, int]]:
        """Prints a summary of the app review dataset and returns a dictionary

        This method prints various counts, frequencies and summary statistics for  the dataframe,
        such as the number of reviews, apps, and reviewrs, as well rating and review length descriptive
        statistics.

        Args:
            df (DataFrame): Dataframe to analyze

        """
        if self.is_cache_valid(df=df) and isinstance(self._summary, dict):
            pass
        else:
            self._df = df
            self._summary = self._summarize(df=df)

        title = "AppVoCAI Dataset Summary"
        printer.print_dict(title=title, data=self._summary)
        return self._summary

    def _summarize(self, df: DataFrame) -> Dict[str, Union[str, float, int]]:
        """Efficiently generates a summary of a PySpark DataFrame.

        Args:
            df (DataFrame): The PySpark DataFrame to summarize.

        Returns:
            Dict[str, Union[str, float, int]]: Summary statistics of the dataset.
        """
        # Compute all necessary metrics in a single pass where possible
        agg_result = df.agg(
            count("*").alias("n_reviews"),  # Total number of reviews
            countDistinct("author").alias("n_authors"),  # Unique authors
            countDistinct("app_id").alias("n_apps"),  # Unique apps
            countDistinct("category").alias("n_categories"),  # Unique categories
            spark_round(mean(length("content")), 2).alias(
                "avg_review_length"
            ),  # Avg review length
            min(length("content")).alias("min_review_length"),  # Min review length
            max(length("content")).alias("max_review_length"),  # Max review length
            min("date").alias("dt_first"),  # First review date
            max("date").alias("dt_last"),  # Last review date
        ).collect()[0]

        # Extract metrics from the aggregated result
        n_reviews = agg_result["n_reviews"]
        n_authors = agg_result["n_authors"]
        n_apps = agg_result["n_apps"]
        n_categories = agg_result["n_categories"]
        avg_review_length = float(agg_result["avg_review_length"])
        min_review_length = agg_result["min_review_length"]
        max_review_length = agg_result["max_review_length"]
        dt_first = agg_result["dt_first"]
        dt_last = agg_result["dt_last"]

        # Compute metrics that require filtering or grouping
        n_influential = (
            df.filter(col("vote_count") > 0)
            .select(countDistinct("author"))
            .collect()[0][0]
        )
        repeat_authors = df.groupBy("author").count().filter(col("count") > 1).count()

        # Derived metrics
        p_influential = (
            round((n_influential / n_authors * 100), 2) if n_authors > 0 else 0
        )
        p_repeat_authors = (
            round((repeat_authors / n_authors * 100), 2) if n_authors > 0 else 0
        )
        avg_reviews_per_app = round(n_reviews / n_apps, 2) if n_apps > 0 else 0

        # Estimate memory size
        msize = PySparkDataFrameMemoryFootprintEstimator().estimate_memory_size(df=df)
        memory_mb = round(msize / (1024 * 1024), 2)

        # Create the summary dictionary
        summary = {
            "Number of Reviews": f"{n_reviews:,}",
            "Number of Reviewers": f"{n_authors:,}",
            "Number of Repeat Reviewers": f"{repeat_authors:,} ({p_repeat_authors:.1f}%)",
            "Number of Influential Reviewers": f"{n_influential:,} ({p_influential:.1f}%)",
            "Number of Apps": f"{n_apps:,}",
            "Average Reviews per App": f"{avg_reviews_per_app:.1f}",
            "Number of Categories": f"{n_categories:,}",
            "Features": len(df.columns),
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": avg_review_length,
            "Memory Size (Mb)": memory_mb,
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }

        return summary

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


# ------------------------------------------------------------------------------------------------ #
#                                 DATAFRAMER FACTORY                                               #
# ------------------------------------------------------------------------------------------------ #
class DatasetSummarizerFactory:
    """Factory that returns a DatasetSummarizer class based on DataFrame type.

    This class provides a static method to return an appropriate DatasetSummarizer
    instance for a given dataframe. It supports both Pandas and PySpark DataFrames.

    Methods:
        get_overview: Returns a DataFramer instance for the specified dataframe.
    """

    @staticmethod
    def get_summarizer(
        df: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]
    ) -> DatasetSummarizer:
        """Returns a DatasetSummarizer instance for the given dataframe type.

        Args:
            df (Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]):
                The input dataframe, which can be a Pandas or PySpark DataFrame.

        Returns:
            DataSummarizer: An instance of a DataSummarizer object..

        Raises:
            TypeError: If the input dataframe is not a valid Pandas or PySpark DataFrame.
        """
        if isinstance(df, DataFrame):
            return PySparkDatasetSummarizer()
        elif isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame)):
            return PandasDatasetSummarizer()
        else:
            msg = f"Invalid dataframe type. Expected a Pandas or PySpark DataFrame. Received a {type(df)} object."
            raise TypeError(msg)
