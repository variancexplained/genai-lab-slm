#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/univariate.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday October 20th 2024 05:43:16 pm                                                #
# Modified   : Tuesday October 29th 2024 08:00:40 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from discover.app.base import Analysis
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class Distribution(Analysis):
    def __call__(
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
            df = df

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


# ------------------------------------------------------------------------------------------------ #
class Describe(Analysis):
    def __call__(self, data: Union[pd.DataFrame, pd.Series]) -> pd.DataFrame:
        return data.describe().T


# ------------------------------------------------------------------------------------------------ #
class FreqDist(Analysis):
    def __call__(
        self,
        x: str,
        df: pd.DataFrame,
        bins: int = None,
        sort: bool = True,
        topn: int = None,
        ascending: bool = False,
    ) -> pd.DataFrame:
        df = df[x].value_counts(bins=bins, sort=sort, ascending=ascending).to_frame()
        df["%"] = round(df["count"] / df["count"].sum() * 100, 2)
        if topn:
            df = df.iloc[0:topn]
        return df

    # ------------------------------------------------------------------------------------------------ #


class FrequencyPlot(Analysis):
    def __call__(
        self,
        df: pd.DataFrame,
        x: str = None,
        y: str = None,
        stat: str = "count",
        topn: int = None,
        dodge: str = "auto",
        title: str = None,
        threshold: int = 20,
        ax: plt.Axes = None,
    ) -> plt.Axes:
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

    def _get_most_frequent(
        self, df: pd.DataFrame, var: str, counts: pd.Series, topn: int = 10
    ) -> plt.Axes:

        counts = counts.to_frame()
        counts = counts.iloc[0:topn].reset_index()
        return df.loc[df[var].isin(counts[var])]


class FreqDistPlot(Analysis):
    def __call__(
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

        var = x if x else y
        counts = df[var].value_counts(ascending=False).to_frame().reset_index()
        counts = counts["count"]
        counts = counts.rename(var).to_frame().reset_index()
        distplot = DistributionPlot()
        return distplot(
            x=var,
            df=counts,
            hue=hue,
            fill=fill,
            title=title,
            cumulative=cumulative,
            outlier_filter=outlier_filter,
        )


class DistributionPlot(Analysis):
    def __call__(
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
