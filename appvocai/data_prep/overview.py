#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/analysis/overview.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 30th 2024 09:31:08 pm                                                  #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""AppVoC Dataset Overview Module"""
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from appvocai.analysis.base import Analysis
from appvocai.shared.persist.file.io import IOService
from appvocai.shared.persist.object.cache import cachenow
from appvocai.shared.utils.print import Printer
from matplotlib.lines import Line2D

# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
# ------------------------------------------------------------------------------------------------ #
DTYPES = {
    "id": "string",
    "app_id": "string",
    "app_name": "string",
    "category_id": "category",
    "category": "category",
    "author": "string",
    "rating": "float64",
    "title": "string",
    "content": "string",
    "vote_count": "Int64",
    "vote_sum": "Int64",
}


# ------------------------------------------------------------------------------------------------ #
class FilesetOverview(Analysis):

    def __init__(self, folder: str) -> None:
        super().__init__()
        self._folder = folder
        self._nfiles = 0

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def overview(self) -> pd.DataFrame:
        overview = self._overview(folder=self._folder)
        return overview.style.format(thousands=",", precision=2)

    @cachenow()
    def _overview(self, folder: str) -> pd.DataFrame:
        """Summarizes the dataset"""
        categories = []
        filenames = []
        nrows = []
        ncols = []
        disk_sizes = []
        mem_sizes = []
        filepaths = self._get_filepaths(folder=folder)
        for filepath in filepaths:
            category = os.path.splitext(os.path.basename(filepath))[0]
            category = category.replace("-", " ")
            filename = os.path.basename(filepath)
            df = pd.read_csv(
                filepath,
                sep="\t",
                dtype=DTYPES,
                parse_dates=["date"],
                lineterminator="\n",
            )
            rows = df.shape[0]
            cols = df.shape[1]
            disk_size = round(os.path.getsize(filepath) / (1024 * 1024), 2)
            mem_size = round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)

            categories.append(category)
            filenames.append(filename)
            nrows.append(rows)
            ncols.append(cols)
            disk_sizes.append(disk_size)
            mem_sizes.append(mem_size)
        d = {
            "Category": categories,
            "Filename": filenames,
            "Rows": nrows,
            "Columns": ncols,
            "Size Disk (Mb)": disk_sizes,
            "Size Memory (Mb)": mem_sizes,
        }
        df = pd.DataFrame(d)
        df = df.set_index("Category").sort_index(ascending=True)
        df.loc["Total"] = df.sum()
        df.loc[df.index[-1], "Filename"] = ""
        df.loc[df.index[-1], "Columns"] = ncols[0]
        return df

    def _get_filepaths(self, folder: str) -> list:
        """Returns filepaths in the dataset folder."""
        try:
            return [
                os.path.join(folder, f)
                for f in os.listdir(folder)
                if f.endswith(".tsv")
            ]
        except OSError as e:
            print(f"Unable to read files from {self._folder}.\n{e}")
            raise


# ------------------------------------------------------------------------------------------------ #
class DatasetOverview(Analysis):
    """Provides an overview of a dataset."""

    def __init__(
        self,
        data: pd.DataFrame,
        filepath_reference_data: str = "data/ref/apps_by_category.csv",
    ) -> None:
        super().__init__()
        self._data = data
        self._reference_data = IOService.read(filepath=filepath_reference_data)
        self._printer = Printer()

    # ------------------------------------------------------------------------------------------- #
    @property
    def data(self) -> pd.DataFrame:
        return self._data

    # ------------------------------------------------------------------------------------------- #
    @property
    def summary(self) -> list:
        """Returns a list containing the names of the columns in the dataset."""
        summary = self._summarize()
        return summary.style.format(thousands=",", precision=2)

    # ------------------------------------------------------------------------------------------- #
    @property
    def overview(self) -> None:
        title = "AppVoC Overview"
        self._printer.print_dict(title=title, data=self._overview())

    # ------------------------------------------------------------------------------------------- #
    @property
    def info(self) -> pd.DataFrame:
        info = self._info()
        return info.style.format(thousands=",", precision=2)

    # ------------------------------------------------------------------------------------------- #
    @property
    def by_category(self) -> pd.DataFrame:
        data = self._by_category()
        return data.style.format(thousands=",", precision=2)

    # ------------------------------------------------------------------------------------------- #
    @property
    def dtypes(self) -> pd.DataFrame:
        return self._dtypes()

    # ------------------------------------------------------------------------------------------- #
    @property
    def size(self) -> int:
        return self._size()

    # ------------------------------------------------------------------------------------------- #
    def describe(self, x: str) -> pd.DataFrame:
        """Returns descriptive statistics for a designated overview variable 'x'.

        Args:
            x (str): The variable for which descriptive statistics
                will be computed.

        Returns: DataFrame containing descriptive statistics.

        """
        df = self._by_category()
        return df[x].describe().T

    # ------------------------------------------------------------------------------------------- #
    def apps_by_category(self) -> pd.DataFrame:
        """Returns apps by category for both AppStore and AppVoC Dataset"""
        abc_appstore = self._reference_data
        abc_appstore["% AppStore"] = round(
            abc_appstore["AppStore"] / abc_appstore["AppStore"].sum(), 2
        )
        abc_appvoc = (
            self._data.groupby(by="category")["app_name"].nunique().reset_index()
        )
        abc_appvoc.columns = ["Category", "AppVoC"]
        abc_appvoc["% AppVoC"] = round(
            abc_appvoc["AppVoC"] / abc_appvoc["AppVoC"].sum(), 2
        )
        df = pd.merge(abc_appstore, abc_appvoc, on="Category")
        df = df.sort_values(by="AppVoC", ascending=False)
        return df

    # ------------------------------------------------------------------------------------------- #
    def get_counts_by_category(
        self,
        x: str,
        sort_by_value: bool = True,
        ascending: bool = False,
        appvoc: bool = True,
    ) -> pd.DataFrame:
        """Provides review counts by category

        Args:
            x (str): The variable to count.
            sort_by_value (bool): Whether to sort by value rather than
                by category.
            ascending (bool): How to sort if sorting by value.
            appvoc (bool): Whether to return counts for the AppVoC Dataset
                or for the App Store. Only relevant for app counts
                by category. This variable is ignored for
                other measures.
        """
        if appvoc:
            # Obtain the data
            df = self._by_category()
        else:
            # Only relevant when x = "Apps"
            df = self._reference_data()
        # Set the columns of interest
        columns = ["Category", x]
        # Subset the dataset
        df = df[columns]
        # Sort the dataset
        if sort_by_value:
            df = df.sort_values(by=x, ascending=ascending)
        else:
            df = df.sort_values(by="Category", ascending=True)
        # Add Cumulative counts
        df["Cumulative"] = df[x].cumsum()
        # Add percentages
        total = df[x].sum()
        df["Percent"] = round(df[x] / total, 2)
        # Add cumulative percent
        df["Cumulative Percent"] = df["Percent"].cumsum()

        return df

    # ------------------------------------------------------------------------------------------- #
    def plot_reviews_by_category(
        self,
        ax: plt.Axes = None,
        title: str = "AppVoC\nReviews by Category",
    ) -> plt.Axes:
        """Plots the number of reviews by category.

        Args:
            ax (plt.Axes): Matplotlib Axes object. Optional
            title (str): Title for the plot.

        Returns: plt.Axes object.

        """
        # Set or create the Axes object
        if ax is None:
            _, ax = plt.subplots(nrows=1, ncols=1, figsize=(12, 4))
        # Obtain the data
        df = self._by_category()
        # Sort the data by review counts
        df = df.sort_values(by="Reviews", ascending=False)
        # Render the barplot
        ax = sns.barplot(data=df, x="Category", y="Reviews", ax=ax, palette="Blues_r")
        # Set the title and tick labels.
        ax.set_title(title)
        ax.tick_params(axis="x", labelrotation=90)

        return ax

    # ------------------------------------------------------------------------------------------- #
    def plot_authors_by_category(
        self,
        ax: plt.Axes = None,
        title: str = "AppVoC\nAuthors by Category",
    ) -> plt.Axes:
        """Plots the number of authors by category.

        Args:
            ax (plt.Axes): Matplotlib Axes object. Optional
            title (str): Title for the plot.

        Returns: plt.Axes object.

        """
        # Set or create the Axes object
        if ax is None:
            _, ax = plt.subplots(nrows=1, ncols=1, figsize=(12, 4))
        # Obtain the data
        df = self._by_category()
        # Sort the data by author counts
        df = df.sort_values(by="Authors", ascending=False)
        # Render the barplot
        ax = sns.barplot(data=df, x="Category", y="Authors", ax=ax, palette="Blues_r")
        # Set the title and tick labels.
        ax.set_title(title)
        ax.tick_params(axis="x", labelrotation=90)

        return ax

    # ------------------------------------------------------------------------------------------- #
    def plot_reviews_per_app_by_category(
        self,
        ax: plt.Axes = None,
        title: str = "AppVoC\nReviews per App by Category",
    ) -> plt.Axes:
        """Plots the number of reviews per app by category.

        Args:
            ax (plt.Axes): Matplotlib Axes object. Optional
            title (str): Title for the plot.
            return_data (bool): Whether to return the data as well.

        Returns: plt.Axes object.

        """
        # Set or create the Axes object
        if ax is None:
            _, ax = plt.subplots(nrows=1, ncols=1, figsize=(12, 4))
        # Get the data
        df = self._by_category()
        # Sort the data by reviews per app
        df = df.sort_values(by="Reviews per App", ascending=False)
        # Render the barplot
        ax = sns.barplot(
            data=df, x="Category", y="Reviews per App", ax=ax, palette="Blues_r"
        )
        # Set the title and axis ticks.
        ax.set_title(title)
        ax.tick_params(axis="x", labelrotation=90)

        return ax

    # ------------------------------------------------------------------------------------------- #
    def plot_apps_by_category(
        self,
        ax: plt.Axes = None,
        title: str = "AppVoC\nApps by Category",
    ) -> plt.Axes:
        """Plots the number of apps by category.

        Args:
            ax (plt.Axes): Matplotlib Axes object. Optional
            title (str): Title for the plot.

        Returns: plt.Axes object.

        """
        # Get or create the matplotlib Axes object
        if ax is None:
            _, ax = plt.subplots(nrows=1, ncols=1, figsize=(12, 4))
        # Obtain the statistics by category
        df = self._by_category()
        # Sort descending by app count
        df = df.sort_values(by="Apps", ascending=False)
        # Render the barplot of app counts by category
        ax = sns.barplot(data=df, x="Category", y="Apps", ax=ax, palette="Blues_r")
        ax.set_title(title)
        ax.tick_params(axis="x", labelrotation=90)

        # Set up a 2nd axis.
        ax2 = ax.twinx()

        # Render a lineplot on the 2nd axis
        ax2 = sns.pointplot(
            data=self._reference_data,
            x="Category",
            y="Apps",
            color="orange",
            ax=ax2,
            linewidth=1,
            scale=0.5,
        )
        # Custom legend
        bar_legend = Line2D([0], [0], color="steelblue", lw=4, label="AppVoC Apps")
        line_legend = Line2D([0], [0], color="orange", lw=1, label="App Store Apps")

        ax.legend(handles=[bar_legend, line_legend], loc="upper left")

        return ax

    # ------------------------------------------------------------------------------------------- #
    def boxplot(
        self,
        x: str,
        ax: plt.Axes = None,
        title: str = None,
    ) -> plt.Axes:
        """Renders a univariate boxplot for the variable x

        The variable x reflects counts by category. This method renders a univariate boxplot
        of the designated count variable by category.

        Args:
            x (str): The column containing the data to be plotted.
            ax (plt.Axes): Matplotlib Axes object. Optional
            title (str): Title for the plot.

        Returns: plt.Axes object.
        """

        # Get or create the matplotlib Axes object
        if ax is None:
            _, ax = plt.subplots(nrows=1, ncols=1, figsize=(12, 4))

        # Obtain the data
        df = self._by_category()
        # Render the plot
        ax = sns.boxplot(data=df, x=x, ax=ax)
        # Add the title
        if title is not None:
            ax.set_title(title)

        return ax

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _summarize(self) -> pd.DataFrame:
        """Summarizes the number of reviews by category."""
        summary = self._data.groupby("category").size().reset_index(name="Count")
        summary["Percent"] = round(summary["Count"] / len(self._data) * 100, 2)
        return summary

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _overview(self) -> None:
        """Returns an overview of the dataset in terms of its shape and size."""
        n_reviews = self._data.shape[0]
        n_users = self._data["author"].nunique()
        n_apps = self._data["app_name"].nunique()
        n_categories = self._data["category"].nunique()
        n_first = self._data["date"].min().date()
        n_last = self._data["date"].max().date()
        size = self.size / (1024 * 1024)

        return {
            "Number of Reviews": n_reviews,
            "Number of Users": n_users,
            "Number of Apps": n_apps,
            "Number of Categories": n_categories,
            "Features": self._data.shape[1],
            "Memory Size (Mb)": round(size, 2),
            "Date of First Review": n_first,
            "Date of Last Review": n_last,
        }

    # ------------------------------------------------------------------------------------------- #

    @cachenow()
    def _dtypes(self) -> pd.DataFrame:
        """Returns the count of data types in the dataset."""
        dtypes = self._data.dtypes.apply(lambda x: str(x)).value_counts().reset_index()
        dtypes.columns = ["Data Type", "Count"]
        return dtypes

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _size(self) -> int:
        """Returns the size of the Dataset in memory in bytes."""
        return self._data.memory_usage(deep=True).sum()

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _info(self) -> pd.DataFrame:
        """Returns basic dataset quality statistics."""

        info = pd.DataFrame()
        info["Column"] = self._data.columns
        info["DataType"] = self._data.dtypes.values
        info["Complete"] = self._data.count().values
        info["Null"] = self._data.isna().sum().values
        info["Completeness"] = info["Complete"] / len(self._data)
        info["Unique"] = self._data.nunique().values
        info["Duplicate"] = len(self._data) - info["Unique"]
        info["Uniqueness"] = info["Unique"] / len(self._data)
        info["Size (Bytes)"] = self._data.memory_usage(deep=True, index=False).values
        return info

    # ------------------------------------------------------------------------------------------- #
    @cachenow()
    def _by_category(self) -> pd.DataFrame:
        """Summarizes the app data by category"""
        df1 = self._data.groupby(["category"])["id"].nunique().to_frame()
        df2 = self._data.groupby(["category"])["app_id"].nunique().to_frame()
        df3 = self._data.groupby(["category"])["author"].nunique().to_frame()
        df4 = df1.join(df2, on="category").reset_index()
        df5 = df4.join(df3, on="category")
        df5.columns = ["Category", "Reviews", "Apps", "Authors"]
        df5["Reviews per App"] = round(df5["Reviews"] / df5["Apps"], 2)
        return df5
