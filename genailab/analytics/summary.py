#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/analytics/summary.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday February 8th 2025 01:16:58 pm                                              #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFrame Summarizer Module"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, countDistinct, length, max, min, round

from genailab.core.dtypes import DFType
from genailab.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()

# ------------------------------------------------------------------------------------------------ #
class Summarizer(ABC):
    @abstractmethod
    def summarize(self, dataframe: Union[pd.DataFrame, DataFrame], print: bool = True) -> Dict[str, Any]:
        """Abstract method that defines the interface for dataset summaries.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The input DataFrame, either Pandas or PySpark.
            print (bool): Whether to print the summary.

        Returns:
            Dict[str, Any]: A dictionary containing the summary statistics.
        """
        pass

# ------------------------------------------------------------------------------------------------ #
class PySparkSummarizer(Summarizer):
    def summarize(self, dataframe: DataFrame, print: bool = True) -> Dict[str, Any]:
        """Returns and optionally prints a summary of the app review dataset (PySpark version).

        Args:
            dataframe (DataFrame): The input PySpark DataFrame.
            print (bool): Whether to print the summary.

        Returns:
            Dict[str, Any]: A dictionary containing the summary statistics.
        """

        n = dataframe.count()
        p = len(dataframe.columns)  # Number of columns (features)
        n_auth = dataframe.select(countDistinct("author")).collect()[0][0]
        n_auth_inf = dataframe.filter("vote_count > 0").select(countDistinct("author")).collect()[0][0]
        p_auth_inf = np.round((n_auth_inf / n_auth) * 100, 2) if n_auth > 0 else 0.0  # avoid division by zero
        n_repeat_auth = dataframe.groupBy("author").agg(count("*").alias("count")).filter("count > 1").count()
        p_repeat_auth = np.round((n_repeat_auth / n_auth) * 100, 2) if n_auth > 0 else 0.0  # avoid division by zero
        n_apps = dataframe.select(countDistinct("app_id")).collect()[0][0]
        n_categories = dataframe.select(countDistinct("category")).collect()[0][0]
        ave_reviews_per_app = np.round((n / n_apps), 2) if n_apps > 0 else 0.0  # avoid division by zero

        review_lengths = dataframe.select(length("content"))
        min_review_length = review_lengths.agg(min("length(content)")).collect()[0][0]
        max_review_length = review_lengths.agg(max("length(content)")).collect()[0][0]
        avg_review_length = review_lengths.agg(avg("length(content)")).collect()[0][0]

        # Memory usage (more accurate in Spark 3.0+)
        # mem = dataframe.storageLevel.memorySize / (1024 * 1024) if hasattr(dataframe.storageLevel, "memorySize") else 0.0  # Handle cases where memorySize is unavailable
        mem = dataframe.rdd.map(lambda row: len(str(row))).sum() / 1024*1024

        dt_first = dataframe.agg(min("date")).collect()[0][0]
        dt_last = dataframe.agg(max("date")).collect()[0][0]

        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Number of Categories": n_categories,
            "Features": len(dataframe.columns),  # More concise way to get the number of features
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": np.round(avg_review_length, 2),
            "Memory Size (Mb)": f"{np.round(mem, 2):,}",
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }

        if print:
            title = "AppVoCAI Dataset Summary"
            printer.print_dict(title=title, data=d)  # Corrected key to 'data'
        return d
# ------------------------------------------------------------------------------------------------ #
class PandasSummarizer(Summarizer):
    def summarize(self, dataframe: pd.DataFrame, print: bool = False) -> Dict[str, Any]:
        """Prints a summary of the app review dataset (Pandas version).

        Args:
            dataframe (pd.DataFrame): The input Pandas DataFrame.
            print (bool): Whether to print the summary.

        Returns:
            Dict[str, Any]: A dictionary containing the summary statistics.
        """
        n = dataframe.shape[0]
        p = dataframe.shape[1]
        n_auth = dataframe["author"].nunique()
        n_auth_inf = dataframe.loc[dataframe["vote_count"] > 0]["author"].nunique()
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2) if n_auth > 0 else 0.0
        n_repeat_auth = int((dataframe["author"].value_counts() > 1).sum())
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2) if n_auth > 0 else 0.0
        n_apps = dataframe["app_id"].nunique()
        n_categories = dataframe["category"].nunique()
        ave_reviews_per_app = round(n / n_apps, 2) if n_apps > 0 else 0.0

        review_lengths = dataframe["content"].apply(lambda n: len(n.split()))
        min_review_length = np.min(review_lengths)
        max_review_length = np.max(review_lengths)
        avg_review_length = np.mean(review_lengths)

        mem = dataframe.memory_usage(deep=True).sum()
        dt_first = dataframe["date"].min()
        dt_last = dataframe["date"].max()
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
        if print:
            title = "AppVoCAI Dataset Summary"
            printer.print_dict(title=title, data=d)  # Corrected key to 'data'
        return d

# ------------------------------------------------------------------------------------------------ #
class SummarizerFactory:
    """A factory class for creating Summarizer instances based on DataFrame type.

    This factory allows you to obtain the appropriate Summarizer implementation
    (either PandasSummarizer or PySparkSummarizer) depending on whether you're
    working with a Pandas DataFrame or a PySpark DataFrame.  It encapsulates
    the logic for selecting the correct summarization strategy.
    """
    __strategies = {
        DFType.PANDAS: PandasSummarizer(),
        DFType.SPARK: PySparkSummarizer(),
    }

    def get_summarizer(self, dftype: DFType) -> Summarizer:
        """Returns a Summarizer instance based on the specified DataFrame type.

        Args:
            dftype (DFType): An enum representing the type of DataFrame (PANDAS or SPARK).

        Returns:
            Summarizer: An instance of either PandasSummarizer or PySparkSummarizer,
                        depending on the provided `dftype`.

        Raises:
            KeyError: If an invalid `dftype` is provided.  (While the current
                      implementation won't raise it, it's good practice to
                      document potential exceptions for future maintainability).
        """
        return self.__strategies[dftype]