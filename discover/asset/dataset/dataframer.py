#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/dataframer.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 16th 2025 02:49:20 pm                                              #
# Modified   : Wednesday January 22nd 2025 01:10:32 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFramer Module"""
from abc import ABC, abstractmethod
from decimal import ROUND_HALF_UP, Decimal
from typing import Union

import numpy as np
import pandas as pd
from pandarallel import pandarallel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, length, max, mean, min
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

from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
#                                      DATAFRAMER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataFramer(ABC):
    """A Base class for pandas and pyspark DataFrame wrapper subclasses.

    Args:
        df (Union[pd.DataFrame, DataFrame]): A Pandas or PySpark DataFrame.
    """

    def __init__(self, df: Union[pd.DataFrame, DataFrame]) -> None:
        self._df = df

    @property
    def dataframe(self) -> Union[pd.DataFrame, DataFrame]:
        return self._df

    @dataframe.setter
    def dataframe(self, df: Union[pd.DataFrame, DataFrame, None]) -> None:
        self._df = df

    @abstractmethod
    def info(self) -> pd.DataFrame:
        """Returns a concise summary of the DataFrame, including column types, null values, and cardinality."""

    @abstractmethod
    def summary(self) -> None:
        """Prints a qualitative summary of variable counts, averages, frequencies and basic descriptive statistics."""

    def __getstate__(self) -> dict:
        """Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        return self.__dict__.copy()

    def __setstate__(self, state) -> None:
        """Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)


# ------------------------------------------------------------------------------------------------ #
#                                  PANDAS DATAFRAMER                                               #
# ------------------------------------------------------------------------------------------------ #
class PandasDataFramer(DataFramer):
    """A Wrapper for pandas DataFrame objects.

    Encapsulates the DataFrame contents and relevant metadata

    Args:
        df (pd.DataFrame): A Pandas DataFrame.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df=df)

    def info(self) -> pd.DataFrame:
        """Generates a DataFrame with quantitative summaries of the dataset and its columns.

        This method returns a DataFrame containing quantitative information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and memory usage in bytes.

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        info = pd.DataFrame()
        info["Column"] = self._df.columns
        info["DataType"] = self._df.dtypes.values
        info["Complete"] = self._df.count().values
        info["Null"] = self._df.isna().sum().values
        info["Completeness"] = info["Complete"] / len(self._df)
        info["Unique"] = self._df.nunique().values
        info["Duplicate"] = len(self._df) - info["Unique"]
        info["Uniqueness"] = info["Unique"] / len(self._df)
        info["Size (Bytes)"] = self._df.memory_usage(deep=True, index=False).values
        return info

    def summary(self) -> None:
        """Prints a summary of the app review dataset.

        The summary includes:
        - Number of reviews, authors, apps, and categories.
        - Proportion of influential and repeat reviewers.
        - Average review length and reviews per app.
        - Memory usage and date range of the reviews.
        """
        n = self._df.shape[0]
        p = self._df.shape[1]
        n_auth = self._df["author"].nunique()
        n_auth_inf = self._df.loc[self._df["vote_count"] > 0]["author"].nunique()
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2)
        n_repeat_auth = int((self._df["author"].value_counts() > 1).sum())
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2)
        n_apps = self._df["app_id"].nunique()
        n_categories = self._df["category"].nunique()
        ave_reviews_per_app = round(n / n_apps, 2)

        review_lengths = self._df["content"].parallel_apply(lambda n: len(n.split()))
        min_review_length = min(review_lengths)
        max_review_length = max(review_lengths)
        avg_review_length = np.mean(review_lengths)

        mem = self._df.memory_usage(deep=True).sum().sum()
        dt_first = self._df["date"].min()
        dt_last = self._df["date"].max()
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
        title = "AppVoCAI Dataset Summary"
        printer.print_dict(title=title, data=d)
        return d


# ------------------------------------------------------------------------------------------------ #
#                                  PYSPARK DATAFRAMER                                              #
# ------------------------------------------------------------------------------------------------ #
class PySparkDataFramer(DataFramer):
    """A Wrapper for PySpark DataFrame objects.

    Encapsulates the DataFrame contents and relevant metadata

    Args:
        df (DataFrame): A PySpark DataFrame.
    """

    def __init__(self, df: DataFrame) -> None:
        super().__init__(df=df)

    def info(self) -> DataFrame:
        """Generates a DataFrame with detailed information about each column for a Spark DataFrame.

        This method returns a DataFrame containing detailed information about each column,
        including data type, number of complete cases, number of missing values, completeness,
        number of unique values, number of duplicate values, uniqueness, and estimated memory usage in bytes.

        Returns:
            pd.DataFrame: A DataFrame with detailed information about each column.
        """
        total_rows = self._df.count()

        info = []

        for column in self._df.columns:
            data_type = self._df.schema[column].dataType.simpleString()

            complete_count = self._df.filter(col(column).isNotNull()).count()
            null_count = total_rows - complete_count
            unique_count = self._df.select(column).distinct().count()
            duplicate_count = total_rows - unique_count

            completeness = complete_count / total_rows
            uniqueness = unique_count / total_rows

            size_estimate = self._estimate_column_size(df=self._df, column=column)

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

        return DataFrame(info)

    def summary(self) -> None:
        """Prints a summary of a PySpark DataFrame.

        The summary includes:
        - Number of reviews, authors, apps, and categories.
        - Proportion of influential and repeat reviewers.
        - Average review length and reviews per app.
        - Date range of the reviews.
        """
        # Count total number of reviews
        n = self._df.count()

        # Number of distinct authors
        n_auth = self._df.select(countDistinct("author")).collect()[0][0]

        # Number of influential reviewers (with vote_count > 0)
        n_auth_inf = (
            self._df.filter(col("vote_count") > 0)
            .select(countDistinct("author"))
            .collect()[0][0]
        )

        # Proportion of influential reviewers
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2) if n_auth > 0 else 0

        # Number of repeat reviewers (authors with more than one review)
        repeat_auth_df = self._df.groupBy("author").count().filter(col("count") > 1)
        n_repeat_auth = repeat_auth_df.count()
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2) if n_auth > 0 else 0

        # Number of distinct apps and categories
        n_apps = self._df.select(countDistinct("app_id")).collect()[0][0]
        n_categories = self._df.select(countDistinct("category")).collect()[0][0]

        # Average reviews per app
        ave_reviews_per_app = round(n / n_apps, 2) if n_apps > 0 else 0

        # Review Length
        df = self._df.withColumn("review_length", length(col("content")))
        # Extract min, max and average review lengths
        min_review_length = df.select(min("review_length")).collect()[0][0]
        max_review_length = df.select(max("review_length")).collect()[0][0]
        avg_review_length = df.select(mean("review_length")).collect()[0][0]

        # Round average review length
        if avg_review_length is not None:
            avg_review_length = Decimal(avg_review_length).quantize(
                Decimal("0.00"), rounding=ROUND_HALF_UP
            )

        # Date range of reviews
        dt_first = self._df.select(min("date")).collect()[0][0]
        dt_last = self._df.select(max("date")).collect()[0][0]

        # Summary data dictionary
        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Number of Categories": n_categories,
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": (
                float(avg_review_length) if avg_review_length is not None else None
            ),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }

        # Print summary
        title = "AppVoCAI Dataset Summary"
        self._printer.print_dict(title=title, data=d)
        return d

    def _estimate_column_size(self, df: DataFrame, column: str) -> int:
        """Estimates the in-memory size of a Spark DataFrame column in bytes.

        Args:
            self._df (DataFrame): A spark DataFrame object.
            column (str): The name of the column to estimate size for.

        Returns:
            int: The estimated size of the column in bytes.
        """

        data_type = self._df.schema[column].dataType

        if isinstance(data_type, StringType):
            # Estimate size of StringType by calculating the average string length
            avg_length = self._df.select(_sum(length(col(column)))).first()[0] or 0
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * avg_length
        elif isinstance(data_type, BinaryType):
            # Estimate size of BinaryType by calculating the average binary length
            avg_length = self._df.select(_sum(length(col(column)))).first()[0] or 0
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * avg_length
        elif isinstance(data_type, ShortType):
            # Short type (smallint) types have fixed sizes 2.
            size_per_value = 2
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, TimestampNTZType):
            # TimestampNTZType is eight bytes
            size_per_value = 8
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, (IntegerType, LongType)):
            # Integer and Long types have fixed sizes (4 and 8 bytes respectively)
            size_per_value = 4 if isinstance(data_type, IntegerType) else 8
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, DoubleType):
            # Double type is 8 bytes per value
            size_per_value = 8
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        elif isinstance(data_type, BooleanType):
            # Boolean type is 1 byte per value
            size_per_value = 1
            num_non_nulls = self._df.filter(col(column).isNotNull()).count()
            size_estimate = num_non_nulls * size_per_value
        else:
            # For other types, further implementation may be needed
            size_estimate = 0

        return size_estimate
