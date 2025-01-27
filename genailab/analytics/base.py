#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/analytics/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Sunday January 26th 2025 11:06:54 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Module"""
from __future__ import annotations

from abc import ABC
from typing import Callable

import numpy as np
import pandas as pd
import seaborn as sns
from explorify.eda.visualize.visualizer import Visualizer
from genailab.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class Analysis(ABC):
    """Abstract base class for analyzing app review datasets.

    This class provides methods to analyze and manipulate a DataFrame containing app review data. It supports basic information
    display, visualization, and data querying operations.

    Args:
        df (pd.DataFrame): The input DataFrame containing app review data.

    Attributes:
        info (None): Displays basic DataFrame information.
        visualize (Visualizer): A visualizer instance for creating plots and charts.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._df = self._df.convert_dtypes(convert_string=True)
        self._visualizer = Visualizer()

    @property
    def info(self) -> None:
        """Displays basic information about the DataFrame, such as data types and non-null counts."""
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
    @property
    def visualize(self) -> Visualizer:
        """Returns the Visualizer instance for data visualization.

        Returns:
            Visualizer: An instance of the Visualizer class for creating plots and charts.
        """
        return self._visualizer

    def summarize(self) -> None:
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
        min_review_length = np.min(review_lengths)
        max_review_length = np.max(review_lengths)
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
        if n == 1:
            printer.print_dataframe_as_dict(
                df=df, title="AppVoCAI Sample", text_col="content"
            )
        else:
            return df

    def select(
        self,
        n: int = 10,
        x: str = None,
        exclude: str = None,
        condition: Callable = None,
        sort_by: str = None,
        cols: list = None,
        ascending: bool = False,
    ) -> pd.DataFrame:
        """Selects a subset of the DataFrame based on conditions and sorting.

        Args:
            n (int, optional): The maximum number of rows to select. Defaults to 10.
            x (str, optional): Column name to apply a condition. Defaults to None.
            exclude (str, optional): Column name to exclude certain rows. Defaults to None.
            condition (Callable, optional): A condition to apply to column `x`. Defaults to None.
            sort_by (str, optional): The column to sort by. Defaults to None.
            cols (list, optional): Columns to include in the result. Defaults to None.
            ascending (bool, optional): Sort order. Defaults to False.

        Returns:
            pd.DataFrame: A subset of the DataFrame meeting the specified criteria.
        """
        df = self._df
        if exclude:
            df = df.loc[~df[exclude]]
        if x and condition:
            df = df.loc[df[x].apply(condition)]
        if sort_by:
            df = df.sort_values(sort_by, ascending=ascending)
        if cols:
            df = df[cols]
        if n:
            df = df.head(n=n)
        return df.reset_index(drop=True)

    def show_review(self, review_id: str) -> None:
        """Displays a detailed view of a single review by its ID.

        Args:
            review_id (str): The ID of the review to display.

        Returns:
            None
        """
        df = self._df.loc[self._df["id"] == review_id]
        self._printer.print_dataframe_as_dict(
            df=df, title=df["app_name"].values[0], text_col="content"
        )
