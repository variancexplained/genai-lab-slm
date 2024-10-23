#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/review.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday October 20th 2024 05:43:16 pm                                                #
# Modified   : Wednesday October 23rd 2024 04:06:02 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Callable, Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from explorify.eda.overview import Overview

from discover.assets.dataset import Dataset
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class Review:
    def __init__(self, dataset: Dataset) -> None:
        self._df = dataset.content[
            [
                "id",
                "app_id",
                "app_name",
                "category_id",
                "category",
                "author",
                "rating",
                "vote_count",
                "vote_sum",
                "review_length",
                "date",
                "content",
            ]
        ]
        self._overview = Overview(
            data=self._df,
        )

    def overview(self) -> None:
        n = self._df.shape[0]
        p = self._df.shape[1]
        n_auth = self._df["author"].nunique()
        n_auth_inf = self._df.loc[self._df["vote_count"] > 0].nunique()
        n_apps = self._df["app_id"].nunique()
        n_categories = self._df["category"].nunique()
        mem = self._df.memory_usage(deep=True)
        dt_first = self._df["date"].min()
        dt_last = self._df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Authors": n_auth,
            "Number of Authors with Influence": n_auth_inf,
            "Number of Apps": n_apps,
            "Number of Categories": n_categories,
            "Features": p,
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        title = "AppVoCAI Dataset Overview"
        printer.print_dict(title=title, data=d)

    def info(self) -> None:
        return self._overview.info()

    def describe(self) -> pd.DataFrame:
        return (
            self._df[["rating", "review_length", "vote_count", "vote_sum"]].describe().T
        )

    def get(self, condition: Callable = None) -> pd.DataFrame:
        if condition:
            df = self._df.loc[condition]
            if df.shape[0] == 1:
                printer.print_dataframe_as_dict(
                    title="AppVoCAI Review Sample", df=df, text_col="content"
                )
            else:
                return df
        return self._df

    def frequency_distribution(
        self,
        x: str,
        bins: int = None,
        sort: bool = True,
        topn: int = None,
        ascending: bool = False,
    ) -> pd.DataFrame:
        df = (
            self._df[x]
            .value_counts(bins=bins, sort=sort, ascending=ascending)
            .to_frame()
        )
        df["%"] = round(df["count"] / df["count"].sum() * 100, 2)
        if topn:
            df = df.iloc[0:topn]
        return df

    def category_summary(self) -> pd.DataFrame:
        df = self._df[
            [
                "category",
                "id",
                "author",
                "app_id",
            ]
        ]
        s = df.groupby("category").nunique()
        s.columns = [
            "Reviews",
            "Author",
            "Apps",
        ]
        return s

    def category_engagement(self) -> pd.DataFrame:
        return (
            self._df[["category", "rating", "review_length", "vote_count", "vote_sum"]]
            .groupby("category")
            .mean()
        )

    def sample(self, n: int = 5, random_state: int = None) -> Union[pd.DataFrame, None]:
        sample = self._df.sample(n=n, random_state=random_state)
        if n > 1:
            return sample
        else:
            printer.print_dataframe_as_dict(
                title="AppVoCAI Review Sample", df=sample, text_col="content"
            )

    def frequency_plot(
        self,
        x: str = None,
        y: str = None,
        stat: str = "count",
        topn: int = None,
        dodge: str = "auto",
        title: str = None,
        threshold: int = 20,
        ax: plt.Axes = None,
    ) -> plt.Axes:
        df = self._df
        var = x if x else y
        orient = "v" if x else "h"
        figsize = (12, 4) if orient == "v" else (4, 6)
        fig, ax = plt.subplots(figsize=figsize)

        counts = df[var].value_counts(ascending=False)
        stats = counts.describe().to_frame().T

        if len(counts) > threshold or topn:
            df = self._get_most_frequent(df=df, var=var, counts=counts, topn=topn)

        total = len(df)  # Used to compute % of counts

        sns.countplot(
            data=df,
            x=x,
            y=y,
            stat=stat,
            dodge=dodge,
            ax=ax,
            order=df[var].value_counts().index,
        )

        if title:
            ax.set_title(title)

        if orient == "v":
            for p in ax.patches:
                x = p.get_bbox().get_points()[:, 0]
                y = p.get_bbox().get_points()[1, 1]
                ax.annotate(
                    text=f"{round(y,0)}\n({round(y/total*100,1)}%)",
                    xy=(x.mean(), y),
                    ha="center",
                    va="bottom",
                )
        else:
            for p in ax.patches:
                x = p.get_x() + p.get_width()
                y = p.get_y() + p.get_height() / 2
                ax.annotate(
                    text=f"{round(p.get_width(),0)} ({round(p.get_width()/total*100,1)}%)",
                    xy=(x, y),
                    va="center",
                )
        return stats

    def frequency_distribution_plot(
        self,
        x: str = None,
        y: str = None,
        df: pd.DataFrame = None,
        hue: str = None,
        fill: bool = True,
        title: str = None,
        cumulative: bool = False,
        outlier_filter: float = None,
    ) -> pd.DataFrame:

        if df is not None:
            df = df
        else:
            df = self._df
        var = x if x else y
        counts = df[var].value_counts(ascending=False).to_frame().reset_index()
        counts = counts["count"]
        counts = counts.rename(var).to_frame().reset_index()
        return self.distribution_plot(
            x=var,
            df=counts,
            hue=hue,
            fill=fill,
            title=title,
            cumulative=cumulative,
            outlier_filter=outlier_filter,
        )

    def distribution_plot(
        self,
        x: str = None,
        y: str = None,
        df: pd.DataFrame = None,
        hue: str = None,
        fill: bool = True,
        title: str = None,
        cumulative: bool = False,
        outlier_filter: float = None,
    ) -> pd.DataFrame:

        if df is not None:
            df = df
        else:
            df = self._df

        fig, axs = plt.subplots(ncols=2, figsize=(12, 4))
        sns.kdeplot(
            data=df, x=x, y=y, hue=hue, fill=fill, cumulative=cumulative, ax=axs[0]
        )
        sns.violinplot(data=df, x=x, y=y, hue=hue, fill=fill, ax=axs[1])

        kde_title = (
            "Kernel Density Plot" if not cumulative else "Cumulative Distribution Plot"
        )

        axs[0].set_title(kde_title)
        axs[1].set_title("Violin Plot")

        if title:
            fig.suptitle(title)
        plt.tight_layout()

        var = x if x else y
        return df[var].describe().to_frame().T

    def association_plot(
        self,
        x: str = None,
        y: str = None,
        orient: str = "h",
        df: pd.DataFrame = None,
        hue: str = None,
        fill: bool = True,
        dodge: str = "auto",
        title: str = None,
    ) -> pd.DataFrame:

        df = self._df

        fig, ax = plt.subplots(figsize=(6, 6))
        sns.barplot(
            data=df,
            x=x,
            y=y,
            orient=orient,
            hue=hue,
            fill=fill,
            ax=ax,
        )

        ax.set_title(title)

        plt.tight_layout()

    def _get_most_frequent(
        self, df: pd.DataFrame, var: str, counts: pd.Series, topn: int = 10
    ) -> plt.Axes:

        counts = counts.to_frame()
        counts = counts.iloc[0:topn].reset_index()
        return df.loc[df[var].isin(counts[var])]
