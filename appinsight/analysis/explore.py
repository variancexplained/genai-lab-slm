#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/analysis/explore.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 8th 2024 04:22:19 am                                                  #
# Modified   : Friday June 7th 2024 02:22:36 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
import shelve
from typing import Union

import pandas as pd
from studioai.analysis import Explorer as EDA

from appinsight.utils.cache import cachenow
from appinsight.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
class ReviewsExplorer(EDA):
    """Explorer object for Apple IOS App Reviews backed by PySpark

    Args:
        df (pd.DataFrame)
    """

    def ___init__(self, df: pd.DataFrame) -> ReviewsExplorer:
        super().__init__(df=df)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # ------------------------------------------------------------------------------------------- #
    @property
    def summary(self) -> pd.DataFrame:
        summary = self._summary()
        return summary.style.format(thousands=",", precision=2)

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _describe(
        self,
        x: list[str] = None,
        include: list[str] = None,
        exclude: list[str] = None,
        groupby: Union[str, list[str]] = None,
    ) -> EDA.SummaryStats:
        """Provides descriptive statistics for the dataset.

        Args:
            x (list[str]): List of variables to incude. If non-Null, include and exclude will be ignored.
            include (list[str]): List of data types to include in the analysis.
            exclude (list[str]): List of data types to exclude from the analysis.
            groupby (str): Column used as a factor variable for descriptive statistics.
        """
        return super().describe(x=x, include=include, exclude=exclude, groupby=groupby)

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _unique(self, columns: list = None) -> pd.DataFrame:
        """Returns a DataFrame containing the unique values for all or the designated columns.

        Args:
            columns (list): List of columns for which unique values are to be returned.
        """
        return super().unique(columns=columns)

    # ------------------------------------------------------------------------------------------- #
    def top_n_frequency_analysis(self, x: str, n: int = 10, data: pd.DataFrame = None):
        """Returns a dataframe with proportional and cumulative counts of one or more categorical variables.

        Args:
            x (Union[str,List[str]]): A string or list of strings indicating the variables included in the count.
            n (int): Number of rows to include in top n.
            data (pd.DataFrame). Data to analyze. Optional.

        """
        return self._top_n_frequency_analysis(x=x, n=n, data=data).style.format(
            thousands=",", precision=2
        )

    @cachenow()
    def _top_n_frequency_analysis(self, x: str, n: int, data: pd.DataFrame = None):
        """Returns a dataframe with proportional and cumulative counts of one or more categorical variables."""
        return super().top_n_frequency_analysis(x=x, n=n, data=data)

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _countstats(self, x: str, df: pd.DataFrame = None) -> pd.DataFrame:
        """Computes descriptive statistics of counts for a variable.

        The value counts of the x variable are obtained and descriptive statistics are
        computed showing min, max, average, std, and quantiles of counts.

        Args:
            x (str): Name of a variable in the dataset
            df (pd.DataFrame): Optional dataframe from which counts will be taken.

        Returns:
            Value Counts: pd.DataFrame
            Count Statistics: pd.DataFrame

        """
        return super().countstats(x=x, df=df)

    # ------------------------------------------------------------------------------------------- #
    def _reset_cache(self):
        # Get the current environment
        env = EnvManager().get_environment()
        # Set the shelve file
        shelf_file = f"cache/{env}/cache"
        # Check if the shelve file exists
        try:
            # Open the shelve file
            with shelve.open(shelf_file) as cache:
                # Get the class name
                class_name = type(self).__name__
                # Iterate over cache keys
                keys_to_remove = [key for key in cache.keys() if class_name in key]
                # Remove keys from the cache
                for key in keys_to_remove:
                    del cache[key]
        except Exception as e:
            msg = f"Exception occurred in reset_cache.\n{e}"
            self.logger.exception(msg)
            raise RuntimeError(msg)

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _summary(self) -> pd.DataFrame:
        """Summarizes the app data by category"""
        df2 = self.df.groupby(["category"])["id"].nunique().to_frame()
        df3 = self.df.groupby(["category"])["app_id"].nunique().to_frame()
        df4 = self.df.groupby(["category"])["author"].nunique().to_frame()
        df5 = df2.join(df3, on="category").reset_index()
        df5["Reviews/App"] = df5["id"] / df5["app_id"]
        df5 = df5.join(df4, on="category")
        df5.columns = ["Category", "Reviews", "Apps", "Reviews/App", "Authors"]
        return df5
