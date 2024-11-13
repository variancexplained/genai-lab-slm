#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/eda.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Tuesday November 12th 2024 11:14:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Class"""
from __future__ import annotations

from typing import Callable

import pandas as pd
from explorify.eda.overview import Overview

from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
class EDA:
    """
    A class for analyzing and visualizing descriptive statistics and summaries of app review data.

    The `EDA` class encapsulates various methods and tools for conducting
    Exploratory Data Analysis (EDA) on app reviews, providing functionality for
    generating overviews, retrieving subsets of data, sampling, and plotting
    distributions, frequencies, and associations.

    Attributes:
        df (pd.DataFrame): The input DataFrame containing app review data.
        distribution (Distribution): An object for analyzing distributions of the data.
        describe (Describe): An object for generating descriptive statistics.
        freqdist (FreqDist): An object for computing frequency distributions.
        freqplot (FrequencyPlot): An object for plotting frequency distributions.
        freqdistplot (FreqDistPlot): An object for visualizing frequency distributions.
        distplot (DistributionPlot): An object for plotting data distributions.
        association (Association): An object for analyzing associations between features.
        assocplot (AssociationPlot): An object for plotting associations.
        multicount (MultivariateCount): An object for counting multivariate relationships.
        multimean (MultivariateStatisticsMean): An object for calculating means in multivariate statistics.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._df = self._df.convert_dtypes(convert_string=True)

    def info(self) -> None:
        """Displays basic information about the DataFrame, such as data types and non-null counts."""
        self._overview = Overview(data=self._df)
        return self._overview.info()

    def summary(self) -> None:
        """Prints a summary of the app review data, including the number of reviews, authors, apps, categories, and memory usage."""
        n = self._df.shape[0]
        p = self._df.shape[1]
        n_auth = self._df["author"].nunique()
        n_auth_inf = self._df.loc[self._df["vote_count"] > 0]["author"].nunique()
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2)
        n_repeat_auth = int((self._df["author"].value_counts() > 1).sum())
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2)
        n_apps = self._df["app_id"].nunique()
        n_categories = self._df["category"].nunique()
        ave_review_len = round(self._df["review_length"].mean(), 2)
        ave_reviews_per_app = round(n / n_apps, 2)
        mem = self._df.memory_usage(deep=True).sum().sum()
        dt_first = self._df["date"].min()
        dt_last = self._df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Number of Categories": n_categories,
            "Average Review Length": f"{ave_review_len:.1f}",
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Features": p,
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        title = "AppVoCAI Dataset Summary"
        printer.print_dict(title=title, data=d)

    def get(
        self,
        condition: Callable = None,
        column_subset: list = None,
        sort_by: str = None,
        ascending: bool = True,
        index: int = None,
    ) -> pd.DataFrame:
        """Retrieves a subset of the DataFrame based on specified conditions, column subsets, and sorting.

        Args:
            condition (Callable, optional): A condition to filter the DataFrame. Defaults to None.
            column_subset (list, optional): A list of columns to include in the subset. Defaults to None.
            sort_by (str, optional): The column to sort by. Defaults to None.
            ascending (bool, optional): Sort order. Defaults to True.
            index (int, optional): The index of the specific row to retrieve. Defaults to None.

        Returns:
            pd.DataFrame: The filtered and optionally sorted subset of the DataFrame.
        """
        df = self._df
        if isinstance(column_subset, str):
            column_subset = [column_subset]
        if condition:
            df = df.loc[condition]
        if sort_by:
            df = df.sort_values(by=sort_by, ascending=ascending)
        if index is not None:
            df = df.iloc[index]
        # if column_subset:
        #     cols = self._get_columns(column_subset)
        #     df = df[cols]
        if isinstance(df, pd.Series):
            df = df.to_frame().T
        if df.shape[0] == 1:
            title = df["app_name"].tolist()[0]
            printer.print_dataframe_as_dict(df=df, title=title, text_col="content")
        else:
            return df

    def sample(
        self,
        n: int = 5,
        frac: float = None,
        condition: Callable = None,
        column_subset: list = None,
        random_state: int = None,
    ) -> pd.DataFrame:
        """Samples rows from the DataFrame based on specified parameters.

        Args:
            n (int, optional): The number of samples to draw. Defaults to 5.
            frac (float, optional): The fraction of the DataFrame to sample. Defaults to None.
            condition (Callable, optional): A condition to filter the DataFrame before sampling. Defaults to None.
            column_subset (list, optional): A list of columns to include in the sample. Defaults to None.
            random_state (int, optional): The random seed for reproducibility. Defaults to None.

        Returns:
            pd.DataFrame: A sampled subset of the DataFrame.
        """
        if column_subset:
            if isinstance(column_subset, str):
                column_subset = [column_subset]
        df = self._df.copy()
        if condition:
            df = df.loc[condition]
        df = df.sample(n=n, frac=frac, random_state=random_state)
        # if column_subset:
        # cols = self._get_columns(column_subset)
        # df = df[cols]
        if n == 1:
            printer.print_dataframe_as_dict(
                df=df, title="AppVoCAI Sample", text_col="content"
            )
        else:
            return df

    # def _get_columns(self, column_subset: list = None) -> list:
    #     """Retrieves the appropriate columns for a given subset.

    #     Args:
    #         column_subset (list, optional): A list of column group names to retrieve. Defaults to None.

    #     Returns:
    #         list: A list of column names based on the specified subsets.
    #     """
    #     cols = OrderedSet(["id", "app_id", "app_name", "category", "author"])
    #     df = pd.DataFrame.from_dict(
    #         data=columns, orient="index", columns=["group"]
    #     ).reset_index()
    #     if "core" in column_subset:
    #         cols.update(df.loc[df["group"] == "core"]["index"].tolist())
    #     if "deviation" in column_subset:
    #         cols.update(df.loc[df["group"] == "deviation"]["index"].tolist())
    #     if "lexical" in column_subset:
    #         cols.update(df.loc[df["group"] == "lexical"]["index"].tolist())
    #     if "syntactic" in column_subset:
    #         cols.update(df.loc[df["group"] == "syntactic"]["index"].tolist())
    #     if "sentiment" in column_subset:
    #         cols.update(df.loc[df["group"] == "sentiment"]["index"].tolist())
    #     if "temporal" in column_subset:
    #         cols.update(df.loc[df["group"] == "temporal"]["index"].tolist())
    #     if "quality" in column_subset:
    #         cols.update(df.loc[df["group"] == "quality"]["index"].tolist())
    #     return list(cols)
